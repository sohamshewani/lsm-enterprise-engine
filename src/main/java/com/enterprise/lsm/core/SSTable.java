package com.enterprise.lsm.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class SSTable implements AutoCloseable {

    private static final int BLOCK_SIZE = 4096;
    private static final byte OP_PUT = 0x01;
    private static final byte OP_DELETE = 0x02;

    private final Path filePath;
    private final FileChannel fileChannel;
    private final long fileLength;
    
    private final List<IndexEntry> primaryIndex;
    private final BloomFilter bloomFilter;
    private final byte[] minKey;
    private final byte[] maxKey;

    public static class Entry {
        private final long sequenceNumber;
        private final byte operationType;
        private final byte[] key;
        private final byte[] value;

        public Entry(long sequenceNumber, byte operationType, byte[] key, byte[] value) {
            this.sequenceNumber = sequenceNumber;
            this.operationType = operationType;
            this.key = key;
            this.value = value;
        }

        public long getSequenceNumber() { return sequenceNumber; }
        public byte getOperationType() { return operationType; }
        public byte[] getKey() { return key; }
        public byte[] getValue() { return value; }
    }

    public static class IndexEntry implements Comparable<IndexEntry> {
        private final byte[] maxKey;
        private final long blockOffset;
        private final int blockSize;

        public IndexEntry(byte[] maxKey, long blockOffset, int blockSize) {
            this.maxKey = maxKey;
            this.blockOffset = blockOffset;
            this.blockSize = blockSize;
        }

        public byte[] getMaxKey() { return maxKey; }
        public long getBlockOffset() { return blockOffset; }
        public int getBlockSize() { return blockSize; }

        @Override
        public int compareTo(IndexEntry other) {
            return compareBytes(this.maxKey, other.maxKey);
        }
    }

    public static class BloomFilter {
        private final long[] bitSet;
        private final int numBits;
        private final int numHashFunctions;

        public BloomFilter(int expectedElements, double falsePositiveRate) {
            this.numBits = (int) Math.ceil(-expectedElements * Math.log(falsePositiveRate) / (Math.log(2) * Math.log(2)));
            this.numHashFunctions = Math.max(1, (int) Math.round((double) numBits / expectedElements * Math.log(2)));
            int numLongs = (numBits + 63) / 64;
            this.bitSet = new long[numLongs];
        }

        public BloomFilter(ByteBuffer buffer) {
            this.numBits = buffer.getInt();
            this.numHashFunctions = buffer.getInt();
            int numLongs = buffer.getInt();
            this.bitSet = new long[numLongs];
            for (int i = 0; i < numLongs; i++) {
                this.bitSet[i] = buffer.getLong();
            }
        }

        public void serialize(ByteBuffer buffer) {
            buffer.putInt(numBits);
            buffer.putInt(numHashFunctions);
            buffer.putInt(bitSet.length);
            for (long l : bitSet) {
                buffer.putLong(l);
            }
        }

        public int serializedSize() {
            return 4 + 4 + 4 + (bitSet.length * 8);
        }

        public void add(byte[] key) {
            long[] hashes = hash(key);
            for (int i = 0; i < numHashFunctions; i++) {
                long combinedHash = hashes[0] + i * hashes[1];
                int bitIndex = (int) ((Math.abs(combinedHash)) % numBits);
                int longIndex = bitIndex >>> 6;
                int bitPosition = bitIndex & 63;
                bitSet[longIndex] |= (1L << bitPosition);
            }
        }

        public boolean mightContain(byte[] key) {
            long[] hashes = hash(key);
            for (int i = 0; i < numHashFunctions; i++) {
                long combinedHash = hashes[0] + i * hashes[1];
                int bitIndex = (int) ((Math.abs(combinedHash)) % numBits);
                int longIndex = bitIndex >>> 6;
                int bitPosition = bitIndex & 63;
                if ((bitSet[longIndex] & (1L << bitPosition)) == 0) {
                    return false;
                }
            }
            return true;
        }

        private static long[] hash(byte[] key) {
            long h1 = MurmurHash3.hash64(key, 0, key.length, 104729);
            long h2 = MurmurHash3.hash64(key, 0, key.length, (int) h1);
            return new long[]{h1, h2};
        }
    }

    public static final class MurmurHash3 {
        public static long hash64(byte[] data, int offset, int length, int seed) {
            long h = seed & 0xFFFFFFFFL;
            int nblocks = length / 8;
            for (int i = 0; i < nblocks; i++) {
                int i8 = offset + i * 8;
                long k = (((long) data[i8 + 7] & 0xff) << 56) |
                         (((long) data[i8 + 6] & 0xff) << 48) |
                         (((long) data[i8 + 5] & 0xff) << 40) |
                         (((long) data[i8 + 4] & 0xff) << 32) |
                         (((long) data[i8 + 3] & 0xff) << 24) |
                         (((long) data[i8 + 2] & 0xff) << 16) |
                         (((long) data[i8 + 1] & 0xff) << 8) |
                         ((long) data[i8] & 0xff);
                k *= 0xc4ceb9fe1a85ec53L;
                k = Long.rotateLeft(k, 31);
                k *= 0xff51afd7ed558ccdL;
                h ^= k;
                h = Long.rotateLeft(h, 27) * 5 + 0x52dce729;
            }
            long k1 = 0;
            int tailStart = offset + nblocks * 8;
            int leftover = length & 7;
            switch (leftover) {
                case 7: k1 ^= ((long) data[tailStart + 6] & 0xff) << 48;
                case 6: k1 ^= ((long) data[tailStart + 5] & 0xff) << 40;
                case 5: k1 ^= ((long) data[tailStart + 4] & 0xff) << 32;
                case 4: k1 ^= ((long) data[tailStart + 3] & 0xff) << 24;
                case 3: k1 ^= ((long) data[tailStart + 2] & 0xff) << 16;
                case 2: k1 ^= ((long) data[tailStart + 1] & 0xff) << 8;
                case 1: k1 ^= ((long) data[tailStart] & 0xff);
                    k1 *= 0xc4ceb9fe1a85ec53L;
                    k1 = Long.rotateLeft(k1, 31);
                    k1 *= 0xff51afd7ed558ccdL;
                    h ^= k1;
            }
            h ^= length;
            h ^= (h >>> 33);
            h *= 0xff51afd7ed558ccdL;
            h ^= (h >>> 33);
            h *= 0xc4ceb9fe1a85ec53L;
            h ^= (h >>> 33);
            return h;
        }
    }

    private SSTable(Path filePath, FileChannel fileChannel, long fileLength, 
                    List<IndexEntry> primaryIndex, BloomFilter bloomFilter, 
                    byte[] minKey, byte[] maxKey) {
        this.filePath = filePath;
        this.fileChannel = fileChannel;
        this.fileLength = fileLength;
        this.primaryIndex = primaryIndex;
        this.bloomFilter = bloomFilter;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    public static SSTable open(Path filePath) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r");
        FileChannel channel = raf.getChannel();
        long length = channel.size();

        if (length < 24) {
            channel.close();
            raf.close();
            throw new IOException("Invalid SSTable file: too small.");
        }

        ByteBuffer footerBuffer = ByteBuffer.allocate(24);
        channel.position(length - 24);
        channel.read(footerBuffer);
        footerBuffer.flip();

        long indexOffset = footerBuffer.getLong();
        long bloomOffset = footerBuffer.getLong();
        long magicNumber = footerBuffer.getLong();

        if (magicNumber != 0x4C534D53535424CLL) { // "LSMSSPBL"
            channel.close();
            raf.close();
            throw new IOException("Invalid SSTable magic number: " + magicNumber);
        }

        ByteBuffer bloomMetaBuffer = ByteBuffer.allocate(12);
        channel.position(bloomOffset);
        channel.read(bloomMetaBuffer);
        bloomMetaBuffer.flip();
        int bloomSize = bloomMetaBuffer.getInt();
        
        ByteBuffer bloomBuffer = ByteBuffer.allocate(bloomSize);
        channel.position(bloomOffset);
        channel.read(bloomBuffer);
        bloomBuffer.flip();
        BloomFilter bloom = new BloomFilter(bloomBuffer);

        ByteBuffer indexMetaBuffer = ByteBuffer.allocate(4);
        channel.position(indexOffset);
        channel.read(indexMetaBuffer);
        indexMetaBuffer.flip();
        int indexCount = indexMetaBuffer.getInt();

        ByteBuffer indexDataBuffer = ByteBuffer.allocate((int) (bloomOffset - indexOffset - 4));
        channel.position(indexOffset + 4);
        channel.read(indexDataBuffer);
        indexDataBuffer.flip();

        List<IndexEntry> index = new ArrayList<>(indexCount);
        byte[] firstKey = null;
        byte[] lastKey = null;

        for (int i = 0; i < indexCount; i++) {
            int keyLen = indexDataBuffer.getInt();
            byte[] mKey = new byte[keyLen];
            indexDataBuffer.get(mKey);
            long blockOffset = indexDataBuffer.getLong();
            int blockSize = indexDataBuffer.getInt();

            if (i == 0) {
                firstKey = mKey;
            }
            lastKey = mKey;
            index.add(new IndexEntry(mKey, blockOffset, blockSize));
        }

        return new SSTable(filePath, channel, length, index, bloom, firstKey, lastKey);
    }

    public static void build(Path filePath, Iterable<Entry> entries) throws IOException {
        Path tempPath = filePath.resolveSibling(filePath.getFileName().toString() + ".tmp");
        try (RandomAccessFile raf = new RandomAccessFile(tempPath.toFile(), "rw");
             FileChannel channel = raf.getChannel()) {

            List<IndexEntry> indexEntries = new ArrayList<>();
            List<Entry> entryList = new ArrayList<>();
            for (Entry e : entries) {
                entryList.add(e);
            }

            int expectedElements = entryList.size();
            BloomFilter bloom = new BloomFilter(Math.max(1, expectedElements), 0.01);
            for (Entry e : entryList) {
                bloom.add(e.getKey());
            }

            ByteBuffer currentBlock = ByteBuffer.allocate(BLOCK_SIZE);
            long currentBlockOffset = 0;
            byte[] lastKeyInBlock = null;

            for (Entry entry : entryList) {
                int estimatedSize = 8 + 1 + 4 + entry.getKey().length + 4 + (entry.getValue() != null ? entry.getValue().length : 0);
                
                if (currentBlock.position() + estimatedSize > BLOCK_SIZE) {
                    flushBlock(channel, currentBlock, currentBlockOffset, indexEntries, lastKeyInBlock);
                    currentBlockOffset = channel.position();
                    currentBlock.clear();
                }

                currentBlock.putLong(entry.getSequenceNumber());
                currentBlock.put(entry.getOperationType());
                currentBlock.putInt(entry.getKey().length);
                currentBlock.put(entry.getKey());
                byte[] val = entry.getValue() != null ? entry.getValue() : new byte[0];
                currentBlock.putInt(val.length);
                currentBlock.put(val);
                lastKeyInBlock = entry.getKey();
            }

            if (currentBlock.position() > 0) {
                flushBlock(channel, currentBlock, currentBlockOffset, indexEntries, lastKeyInBlock);
            }

            long indexOffset = channel.position();
            ByteBuffer indexMeta = ByteBuffer.allocate(4);
            indexMeta.putInt(indexEntries.size());
            indexMeta.flip();
            channel.write(indexMeta);

            for (IndexEntry ie : indexEntries) {
                ByteBuffer ieBuf = ByteBuffer.allocate(4 + ie.getMaxKey().length + 8 + 4);
                ieBuf.putInt(ie.getMaxKey().length);
                ieBuf.put(ie.getMaxKey());
                ieBuf.putLong(ie.getBlockOffset());
                ieBuf.putInt(ie.getBlockSize());
                ieBuf.flip();
                channel.write(ieBuf);
            }

            long bloomOffset = channel.position();
            ByteBuffer bloomBuf = ByteBuffer.allocate(bloom.serializedSize());
            bloom.serialize(bloomBuf);
            bloomBuf.flip();
            channel.write(bloomBuf);

            long footerOffset = channel.position();
            ByteBuffer footer = ByteBuffer.allocate(24);
            footer.putLong(indexOffset);
            footer.putLong(bloomOffset);
            footer.putLong(0x4C534D53535424CLL);
            footer.flip();
            channel.write(footer);

            channel.force(true);
        }

        Files.move(tempPath, filePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private static void flushBlock(FileChannel channel, ByteBuffer block, long offset, 
                                   List<IndexEntry> indexEntries, byte[] lastKey) throws IOException {
        int limit = block.position();
        block.limit(limit);
        block.position(0);
        channel.write(block);
        indexEntries.add(new IndexEntry(lastKey, offset, limit));
    }

    public Entry get(byte[] key) throws IOException {
        if (!bloomFilter.mightContain(key)) {
            return null;
        }

        if (compareBytes(key, minKey) < 0 || compareBytes(key, maxKey) > 0) {
            return null;
        }

        int indexPos = Collections.binarySearch(primaryIndex, new IndexEntry(key, 0, 0));
        int targetIndex;
        if (indexPos >= 0) {
            targetIndex = indexPos;
        } else {
            targetIndex = -indexPos - 1;
            if (targetIndex >= primaryIndex.size()) {
                return null;
            }
        }

        IndexEntry entry = primaryIndex.get(targetIndex);
        ByteBuffer blockBuffer = ByteBuffer.allocate(entry.getBlockSize());
        synchronized (fileChannel) {
            fileChannel.position(entry.getBlockOffset());
            fileChannel.read(blockBuffer);
        }
        blockBuffer.flip();

        while (blockBuffer.hasRemaining()) {
            long seq = blockBuffer.getLong();
            byte op = blockBuffer.get();
            int keyLen = blockBuffer