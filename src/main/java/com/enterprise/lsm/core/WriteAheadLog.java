package com.enterprise.lsm.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

public final class WriteAheadLog implements AutoCloseable {

    public static final byte OP_PUT = (byte) 1;
    public static final byte OP_DELETE = (byte) 2;

    private static final int HEADER_SIZE = 4 + 4 + 8 + 1 + 4 + 4; // 25 bytes

    private final FileChannel channel;
    private final File walFile;
    private long lastSequenceNumber;

    public WriteAheadLog(File walFile) throws IOException {
        this.walFile = walFile;
        if (!walFile.exists()) {
            File parent = walFile.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
            walFile.createNewFile();
        }
        this.channel = FileChannel.open(
            walFile.toPath(),
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE
        );
        this.lastSequenceNumber = 0L;
    }

    public synchronized void append(long sequenceNumber, byte operationType, byte[] key, byte[] value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (value == null) {
            value = new byte[0];
        }

        int keyLen = key.length;
        int valLen = value.length;
        int payloadLength = 1 + 4 + keyLen + 4 + valLen; // OpType(1) + KeyLen(4) + Key + ValLen(4) + Val
        int totalFrameSize = 4 + 4 + 8 + payloadLength; // CRC32(4) + Len(4) + Seq(8) + Payload

        ByteBuffer buffer = ByteBuffer.allocate(totalFrameSize);
        // Skip CRC32 for now (first 4 bytes), position at Payload Length
        buffer.position(4);
        buffer.putInt(payloadLength);
        buffer.putLong(sequenceNumber);
        buffer.put(operationType);
        buffer.putInt(keyLen);
        buffer.put(key);
        buffer.putInt(valLen);
        buffer.put(value);

        // Compute CRC32 over payload length, sequence number, operation type, key length, key, value length, value
        ByteBuffer crcBuf = buffer.duplicate();
        crcBuf.position(4);
        crcBuf.limit(totalFrameSize);
        
        CRC32 crc32 = new CRC32();
        crc32.update(crcBuf);
        int checksum = (int) crc32.getValue();

        buffer.putInt(0, checksum);
        buffer.rewind();

        channel.position(channel.size());
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        channel.force(false);
        this.lastSequenceNumber = Math.max(this.lastSequenceNumber, sequenceNumber);
    }

    public synchronized List<LogEntry> recover() throws IOException {
        List<LogEntry> entries = new ArrayList<>();
        channel.position(0);
        long fileSize = channel.size();
        ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);

        long validUpTo = 0L;

        while (channel.position() + HEADER_SIZE <= fileSize) {
            headerBuf.clear();
            long currentPos = channel.position();
            int readBytes = channel.read(headerBuf);
            if (readBytes < HEADER_SIZE) {
                break;
            }
            headerBuf.flip();

            int expectedCrc = headerBuf.getInt();
            int payloadLength = headerBuf.getInt();
            long sequenceNumber = headerBuf.getLong();
            byte operationType = headerBuf.get();
            int keyLen = headerBuf.getInt();
            int valLen = headerBuf.getInt();

            long totalRecordSize = HEADER_SIZE - 8 + payloadLength; // HEADER_SIZE has CRC(4)+Len(4)+Seq(8)+Op(1)+KLen(4)+VLen(4) = 25. Payload length starts from opType.
            // Wait, let's trace layout:
            // Frame: [CRC32(4) | Payload Length(4) | Sequence Number(8) | Operation Type(1) | Key Length(4) | Key | Value Length(4) | Value]
            // Payload = OpType(1) + KeyLen(4) + Key + ValLen(4) + Value.
            // Header read above read: CRC32(4), Payload Length(4), Seq(8), Op(1), KeyLen(4), ValLen(4) = 25 bytes.
            // Total frame size = 4 (CRC) + 4 (PayloadLen) + PayloadLength.
            
            if (currentPos + 4 + 4 + payloadLength > fileSize) {
                // Partial tail write detected
                break;
            }

            ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLength);
            channel.position(currentPos + 8); // Skip CRC and Length
            channel.read(payloadBuf);
            payloadBuf.flip();

            CRC32 crc32 = new CRC32();
            crc32.update(payloadBuf);
            int computedCrc = (int) crc32.getValue();

            if (computedCrc != expectedCrc) {
                // Checksum mismatch - Corrupted tail or disk error
                break;
            }

            // Extract key and value from payloadBuf
            payloadBuf.position(8); // skip seq(8)
            byte op = payloadBuf.get();
            int kLen = payloadBuf.getInt();
            byte[] key = new byte[kLen];
            payloadBuf.get(key);
            int vLen = payloadBuf.getInt();
            byte[] value = new byte[vLen];
            payloadBuf.get(value);

            entries.add(new LogEntry(sequenceNumber, op, key, value));
            this.lastSequenceNumber = Math.max(this.lastSequenceNumber, sequenceNumber);
            validUpTo = channel.position();
        }

        if (validUpTo < fileSize) {
            channel.truncate(validUpTo);
            channel.force(false);
        }

        return entries;
    }

    public synchronized void truncate() throws IOException {
        channel.truncate(0);
        channel.force(false);
        this.lastSequenceNumber = 0L;
    }

    public synchronized long getLastSequenceNumber() {
        return lastSequenceNumber;
    }

    @Override
    public synchronized void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    public static final class LogEntry {
        private final long sequenceNumber;
        private final byte operationType;
        private final byte[] key;
        private final byte[] value;

        public LogEntry(long sequenceNumber, byte operationType, byte[] key, byte[] value) {
            this.sequenceNumber = sequenceNumber;
            this.operationType = operationType;
            this.key = key;
            this.value = value;
        }

        public long getSequenceNumber() {
            return sequenceNumber;
        }

        public byte getOperationType() {
            return operationType;
        }

        public byte[] getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }
    }
}