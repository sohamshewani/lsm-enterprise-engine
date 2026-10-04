import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class LsmEnterpriseEngineTest {

    // =========================================================================
    // PRODUCTION ENGINE IMPLEMENTATION (Embedded in file for self-containment)
    // =========================================================================

    public static final class LsmEnterpriseEngine implements AutoCloseable {
        private static final byte OP_PUT = 1;
        private static final byte OP_DEL = 2;
        private static final long MEMTABLE_FLUSH_THRESHOLD = 256 * 1024; // 256 KB

        private final Path dbDir;
        private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        
        // MemTable: sorted map for fast in-memory writes and scans
        private final ConcurrentSkipListMap<String, byte[]> memTable = new ConcurrentSkipListMap<>();
        private long currentMemTableSize = 0;

        // Immutable SSTables on disk
        private final List<SstMetadata> sstables = new CopyOnWriteArrayList<>();
        private final AtomicInteger fileIdSequence = new AtomicInteger(0);

        private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "LSM-Flush-Thread");
            t.setDaemon(true);
            return t;
        });

        private final AtomicBoolean isClosed = new AtomicBoolean(false);

        public static class SstMetadata {
            public final int id;
            public final Path dataPath;
            public final Path indexathaPath;
            public final List<IndexEntry> index;

            public SstMetadata(int id, Path dataPath, Path indexathaPath, List<IndexEntry> index) {
                this.id = id;
                this.dataPath = dataPath;
                this.indexathaPath = indexathaPath;
                this.index = index;
            }
        }

        public static class IndexEntry {
            public final String key;
            public final long offset;
            public final int length;

            public IndexEntry(String key, long offset, int length) {
                this.key = key;
                this.offset = offset;
                this.length = length;
            }
        }

        public LsmEnterpriseEngine(Path dbDir) throws IOException {
            this.dbDir = dbDir;
            Files.createDirectories(dbDir);
            recover();
        }

        private void recover() throws IOException {
            // Scan directory for existing SSTables and WAL/Manifest if any
            try (var stream = Files.list(dbDir)) {
                List<Path> sstFiles = stream
                        .filter(p -> p.getFileName().toString().endsWith(".sst"))
                        .sorted(Comparator.comparing(p -> extractId(p)))
                        .toList();

                for (Path sst : sstFiles) {
                    int id = extractId(sst);
                    Path idx = dbDir.resolve("data_" + id + ".idx");
                    if (Files.exists(idx)) {
                        List<IndexEntry> index = readIndex(idx);
                        sstables.add(new SstMetadata(id, sst, idx, index));
                        fileIdSequence.set(Math.max(fileIdSequence.get(), id + 1));
                    }
                }
            }
        }

        private int extractId(Path p) {
            String name = p.getFileName().toString();
            String num = name.replaceAll("[^0-9]", "");
            return Integer.parseInt(num);
        }

        public void put(String key, byte[] value) throws IOException {
            ensureOpen();
            validateKeyVal(key, value);

            rwLock.writeLock().lock();
            try {
                byte[] oldVal = memTable.put(key, value);
                long addedSize = key.getBytes(StandardCharsets.UTF_8).length + value.length;
                if (oldVal != null) {
                    addedSize -= oldVal.length;
                }
                currentMemTableSize += addedSize;

                if (currentMemTableSize >= MEMTABLE_FLUSH_THRESHOLD) {
                    triggerFlush();
                }
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        public byte[] get(String key) throws IOException {
            ensureOpen();
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("Key cannot be null or empty");
            }

            // 1. Check MemTable
            byte[] val = memTable.get(key);
            if (val != null) {
                return val.length == 0 ? null : val; // Tombstone representation: empty array
            }

            // 2. Check SSTables (Newest to Oldest)
            rwLock.readLock().lock();
            try {
                for (int i = sstables.size() - 1; i >= 0; i--) {
                    SstMetadata sst = sstables.get(i);
                    byte[] found = searchSst(sst, key);
                    if (found != null) {
                        return found.length == 0 ? null : found;
                    }
                }
            } finally {
                rwLock.readLock().unlock();
            }

            return null;
        }

        public void delete(String key) throws IOException {
            // Tombstone write
            put(key, new byte[0]);
        }

        private byte[] searchSst(SstMetadata sst, String key) throws IOException {
            // Binary search in index
            List<IndexEntry> idx = sst.index;
            int low = 0;
            int high = idx.size() - 1;
            int targetIdx = -1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                int cmp = idx.get(mid).key.compareTo(key);
                if (cmp <= 0) {
                    targetIdx = mid;
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }

            if (targetIdx == -1) return null;
            IndexEntry entry = idx.get(targetIdx);
            // If exact match or block match, read data file
            try (FileChannel channel = FileChannel.open(sst.dataPath, StandardOpenOption.READ)) {
                ByteBuffer buf = ByteBuffer.allocate(entry.length);
                channel.read(buf, entry.offset);
                buf.flip();
                
                // Format: [keyLen][key][valLen][val]
                int kLen = buf.getInt();
                byte[] kBytes = new byte[kLen];
                buf.get(kBytes);
                String readKey = new String(kBytes, StandardCharsets.UTF_8);

                if (readKey.equals(key)) {
                    int vLen = buf.getInt();
                    byte[] vBytes = new byte[vLen];
                    buf.get(vBytes);
                    return vBytes;
                }
            }
            return null;
        }

        private void triggerFlush() {
            if (memTable.isEmpty()) return;
            ConcurrentSkipListMap<String, byte[]> snapshot = new ConcurrentSkipListMap<>(memTable);
            memTable.clear();
            currentMemTableSize = 0;

            flushExecutor.submit(() -> {
                try {
                    flushToDisk(snapshot);
                } catch (IOException e) {
                    throw new RuntimeException("Flush failed", e);
                }
            });
        }

        public void forceFlush() throws IOException, ExecutionException, InterruptedException {
            rwLock.writeLock().lock();
            ConcurrentSkipListMap<String, byte[]> snapshot;
            try {
                if (memTable.isEmpty()) return;
                snapshot = new ConcurrentSkipListMap<>(memTable);
                memTable.clear();
                currentMemTableSize = 0;
            } finally {
                rwLock.writeLock().unlock();
            }

            Future<?> f = flushExecutor.submit(() -> {
                try {
                    flushToDisk(snapshot);
                } catch (IOException e) {
                    throw new RuntimeException("Force flush failed", e);
                }
            });
            f.get();
        }

        private void flushToDisk(ConcurrentSkipListMap<String, byte[]> map) throws IOException {
            int fileId = fileIdSequence.incrementAndGet();
            Path dataPath = dbDir.resolve("data_" + fileId + ".sst");
            Path idxPath = dbDir.resolve("data_" + fileId + ".idx");

            List<IndexEntry> index = new ArrayList<>();
            try (FileChannel dataChan = FileChannel.open(dataPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                 BufferedWriter idxWriter = Files.newBufferedWriter(idxPath, StandardCharsets.UTF_8)) {

                long currentOffset = 0;
                for (var entry : map.entrySet()) {
                    String k = entry.getKey();
                    byte[] v = entry.getValue();

                    byte[] kBytes = k.getBytes(StandardCharsets.UTF_8);
                    int recordLen = 4 + kBytes.length + 4 + v.length;
                    ByteBuffer recordBuf = ByteBuffer.allocate(recordLen);
                    recordBuf.putInt(kBytes.length);
                    recordBuf.put(kBytes);
                    recordBuf.putInt(v.length);
                    recordBuf.put(v);
                    recordBuf.flip();

                    dataChan.write(recordBuf);
                    index.add(new IndexEntry(k, currentOffset, recordLen));
                    idxWriter.write(k + "," + currentOffset + "," + recordLen + "\n");

                    currentOffset += recordLen;
                }
                dataChan.force(true);
            }

            rwLock.writeLock().lock();
            try {
                sstables.add(new SstMetadata(fileId, dataPath, idxPath, index));
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        private List<IndexEntry> readIndex(Path idxPath) throws IOException {
            List<IndexEntry> index = new ArrayList<>();
            try (BufferedReader reader = Files.newBufferedReader(idxPath, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length == 3) {
                        index.add(new IndexEntry(parts[0], Long.parseLong(parts[1]), Integer.parseInt(parts[2])));
                    }
                }
            }
            return index;
        }

        public void compact() throws IOException, ExecutionException, InterruptedException {
            forceFlush();
            rwLock.writeLock().lock();
            try {
                if (sstables.size() <= 1) return;

                // Merge all SSTables into a single new SSTable, resolving latest values and discarding tombstones
                Map<String, byte[]> compactedMap = new TreeMap<>();
                for (SstMetadata sst : sstables) {
                    try (FileChannel channel = FileChannel.open(sst.dataPath, StandardOpenOption.READ)) {
                        ByteBuffer buf = ByteBuffer.allocate((int) channel.size());
                        channel.read(buf);
                        buf.flip();
                        while (buf.hasRemaining()) {
                            int kLen = buf.getInt();
                            byte[] kBytes = new byte[kLen];
                            buf.get(kBytes);
                            String k = new String(kBytes, StandardCharsets.UTF_8);

                            int vLen = buf.getInt();
                            byte[] vBytes = new byte[vLen];
                            buf.get(vBytes);

                            if (vBytes.length == 0) {
                                compactedMap.remove(k); // Tombstone cleanup
                            } else {
                                compactedMap.put(k, vBytes);
                            }
                        }
                    }
                }

                // Write compacted SSTable
                int fileId = fileIdSequence.incrementAndGet();
                Path dataPath = dbDir.resolve("data_" + fileId + ".sst");
                Path idxPath = dbDir.resolve("data_" + fileId + ".idx");

                List<IndexEntry> index = new ArrayList<>();
                try (FileChannel dataChan = FileChannel.open(dataPath, StandardOpenOption.CREATE, StandardOpenOption