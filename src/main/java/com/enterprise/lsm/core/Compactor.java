package com.enterprise.lsm.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Enterprise LSM Compactor Engine.
 * Implements multi-way leveled compaction ($L_0 \to L_k$) using read-copy-update (RCU) 
 * version sets to guarantee zero-downtime reads during background file merges.
 */
public final class Compactor {

    private static final Logger LOGGER = Logger.getLogger(Compactor.class.getName());

    private final Path dataDirectory;
    private final VersionManager versionManager;
    private final ExecutorService compactionExecutor;
    private final AtomicBoolean isRunning;
    private final AtomicBoolean degradedMode;
    private final long maxLevelBytes;
    private final int maxCompactionThreads;

    /**
     * Represents a peeked key-value entry from an SSTable iterator during multi-way merge.
     */
    private static final class MergeEntry implements Comparable<MergeEntry> {
        private final byte[] key;
        private final byte[] value;
        private final long sequenceNumber;
        private final boolean isTombstone;
        private final SSTableIterator sourceIterator;

        private MergeEntry(byte[] key, byte[] value, long sequenceNumber, boolean isTombstone, SSTableIterator sourceIterator) {
            this.key = key;
            this.value = value;
            this.sequenceNumber = sequenceNumber;
            this.isTombstone = isTombstone;
            this.sourceIterator = sourceIterator;
        }

        @Override
        public int compareTo(MergeEntry other) {
            int cmp = compareBytes(this.key, other.key);
            if (cmp != 0) {
                return cmp;
            }
            // Descending order of sequence number (higher/newer sequence number comes first)
            return Long.compare(other.sequenceNumber, this.sequenceNumber);
        }

        private static int compareBytes(byte[] a, byte[] b) {
            int len = Math.min(a.length, b.length);
            for (int i = 0; i < len; i++) {
                int result = (a[i] & 0xFF) - (b[i] & 0xFF);
                if (result != 0) {
                    return result;
                }
            }
            return Integer.compare(a.length, b.length);
        }
    }

    /**
     * Thread-safe iterator over an SSTable reader.
     */
    public interface SSTableIterator extends AutoCloseable {
        boolean hasNext() throws IOException;
        void next() throws IOException;
        byte[] getKey();
        byte[] getValue();
        long getSequenceNumber();
        boolean isTombstone();
        @Override
        void close() throws IOException;
    }

    /**
     * Reader interface for SSTable files.
     */
    public interface SSTableReaderInstance extends AutoCloseable {
        long getFileId();
        int getLevel();
        byte[] getMinKey();
        byte[] getMaxKey();
        long sizeBytes();
        SSTableIterator iterator() throws IOException;
        @Override
        void close() throws IOException;
    }

    /**
     * Builder interface to create new SSTable files on disk.
     */
    public interface SSTableBuilderInstance extends AutoCloseable {
        void append(byte[] key, byte[] value, long sequenceNumber, boolean isTombstone) throws IOException;
        SSTableReaderInstance finish() throws IOException;
        @Override
        void close() throws IOException;
    }

    /**
     * Factory interface for opening and building SSTables.
     */
    public interface SSTableFactory {
        SSTableReaderInstance open(Path path, int level, long fileId) throws IOException;
        SSTableBuilderInstance create(Path path, int level, long fileId) throws IOException;
    }

    private final SSTableFactory ssTableFactory;

    /**
     * Manages RCU Version sets for the LSM storage engine.
     */
    public static final class Version {
        private final long versionId;
        private final Map<Integer, List<SSTableReaderInstance>> levels;

        public Version(long versionId, Map<Integer, List<SSTableReaderInstance>> levels) {
            this.versionId = versionId;
            this.levels = Collections.unmodifiableMap(levels);
        }

        public long getVersionId() {
            return versionId;
        }

        public List<SSTableReaderInstance> getLevel(int level) {
            return levels.getOrDefault(level, Collections.emptyList());
        }

        public Map<Integer, List<SSTableReaderInstance>> getAllLevels() {
            return levels;
        }
    }

    /**
     * Thread-safe Version Manager providing RCU semantics.
     */
    public static final class VersionManager {
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private volatile Version currentVersion;
        private long sequenceCounter = 0;

        public VersionManager(Version initialVersion) {
            this.currentVersion = initialVersion;
        }

        public Version getCurrentVersion() {
            return currentVersion;
        }

        public Version commit(Map<Integer, List<SSTableReaderInstance>> newLevels) {
            lock.writeLock().lock();
            try {
                sequenceCounter++;
                Version newVersion = new Version(sequenceCounter, newLevels);
                this.currentVersion = newVersion;
                return newVersion;
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    public Compactor(Path dataDirectory,
                     VersionManager versionManager,
                     SSTableFactory ssTableFactory,
                     long maxLevelBytes,
                     int maxCompactionThreads,
                     AtomicBoolean degradedMode) {
        this.dataDirectory = dataDirectory;
        this.versionManager = versionManager;
        this.ssTableFactory = ssTableFactory;
        this.maxLevelBytes = maxLevelBytes;
        this.maxCompactionThreads = maxCompactionThreads;
        this.degradedMode = degradedMode;
        this.isRunning = new AtomicBoolean(false);

        this.compactionExecutor = Executors.newFixedThreadPool(maxCompactionThreads, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "LSM-Compactor-Worker-" + counter.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });
    }

    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            LOGGER.info("Compactor engine started successfully.");
        }
    }

    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            compactionExecutor.shutdown();
            try {
                if (!compactionExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    compactionExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                compactionExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            LOGGER.info("Compactor engine stopped.");
        }
    }

    /**
     * Evaluates compaction necessity and submits tasks to the worker pool.
     */
    public Future<Boolean> triggerCompaction() {
        if (!isRunning.get() || degradedMode.get()) {
            return CompletableFutureHolder.completedFuture(false);
        }

        Version version = versionManager.getCurrentVersion();
        int targetLevel = selectCompactionLevel(version);
        if (targetLevel < 0) {
            return CompletableFutureHolder.completedFuture(false);
        }

        List<SSTableReaderInstance> inputs = selectFilesForCompaction(version, targetLevel);
        if (inputs.isEmpty()) {
            return CompletableFutureHolder.completedFuture(false);
        }

        Callable<Boolean> compactionTask = () -> {
            try {
                executeCompaction(targetLevel, inputs);
                return true;
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Compaction failed for level " + targetLevel, e);
                handleCompactionFailure(e);
                return false;
            }
        };

        return compactionExecutor.submit(compactionTask);
    }

    private int selectCompactionLevel(Version version) {
        Map<Integer, List<SSTableReaderInstance>> allLevels = version.getAllLevels();
        int maxLevel = allLevels.keySet().stream().max(Integer::compareTo).orElse(0);

        for (int l = 0; l <= maxLevel; l++) {
            List<SSTableReaderInstance> tables = allLevels.get(l);
            if (tables == null || tables.isEmpty()) {
                continue;
            }
            long totalSize = tables.stream().mapToLong(SSTableReaderInstance::sizeBytes).sum();
            long levelCapacity = (l == 0) ? maxLevelBytes : (long) (maxLevelBytes * Math.pow(10, l));
            if (totalSize > levelCapacity) {
                return l;
            }
        }
        return -1;
    }

    private List<SSTableReaderInstance> selectFilesForCompaction(Version version, int level) {
        List<SSTableReaderInstance> levelFiles = version.getLevel(level);
        if (levelFiles.isEmpty()) {
            return Collections.emptyList();
        }

        if (level == 0) {
            // In L0, compaction typically includes all L0 files to resolve key overlaps
            return new ArrayList<>(levelFiles);
        } else {
            // Pick the oldest file or file with lowest sequence/largest size for compaction in Ln
            // For simplicity, select the first file in the level
            List<SSTableReaderInstance> selected = new ArrayList<>();
            selected.add(levelFiles.get(0));
            
            // Also fetch overlapping files in level + 1
            List<SSTableReaderInstance> nextLevelFiles = version.getLevel(level + 1);
            if (!nextLevelFiles.isEmpty()) {
                byte[] minKey = selected.get(0).getMinKey();
                byte[] maxKey = selected.get(0).getMaxKey();
                for (SSTableReaderInstance nextFile : nextLevelFiles) {
                    if (keysOverlap(minKey, maxKey, nextFile.getMinKey(), nextFile.getMaxKey())) {
                        selected.add(nextFile);
                    }
                }
            }
            return selected;
        }
    }

    private boolean keysOverlap(byte[] minA, byte[] maxA, byte[] minB, byte[] maxB) {
        return MergeEntry.compareBytes(maxA, minB) >= 0 && MergeEntry.compareBytes(minA, maxB) <= 0;
    }

    private void executeCompaction(int level, List<SSTableReaderInstance> inputs) throws IOException {
        LOGGER.info(String.format("Starting compaction from level %d involving %d SSTables", level, inputs.size()));

        int outputLevel = (level == 0) ? 1 : level + 1;
        List<SSTableReaderInstance> outputs = new ArrayList<>();
        PriorityQueue<MergeEntry> minHeap = new PriorityQueue<>();

        List<SSTableIterator> activeIterators = new ArrayList<>();
        try {
            for (SSTableReaderInstance reader : inputs) {
                SSTableIterator it = reader.iterator();
                activeIterators.add(it);
                if (it.hasNext()) {
                    it.next();
                    minHeap.add(new MergeEntry(it.getKey(), it.getValue(), it.getSequenceNumber(), it.isTombstone(), it));
                }
            }

            long fileIdCounter = System.currentTimeMillis();
            Path outputPath = dataDirectory.resolve(String.format("data_L%d_%d_%d.sst", outputLevel, fileIdCounter, UUID.randomUUID().getMostSignificantBits()));
            SSTableBuilderInstance builder = ssTableFactory.create(outputPath, outputLevel, fileIdCounter);

            byte[] lastKey = null;
            try {
                while (!minHeap.isEmpty()) {
                    MergeEntry entry = minHeap.poll();

                    // Deduplication and tombstone filtering:
                    // Since minHeap orders by key ascending and sequenceNumber descending,
                    // the first time we see a key, it is the highest sequence number version.
                    boolean isDuplicate = lastKey != null && MergeEntry.compareBytes(entry.key, lastKey) == 0;
                    
                    if (!isDuplicate) {
                        // If it's not a duplicate, check if it's a tombstone at the bottom-most level.
                        // If outputLevel is the maximum level (e.g., L3+), we can drop tombstones entirely.
                        // Otherwise, we retain tombstones unless we are sure no older levels contain this key.
                        boolean isBottomLevel = outputLevel >= 3;
                        if (!(entry.isTombstone && isBottomLevel)) {
                            builder.append(entry.key, entry.value, entry.sequenceNumber, entry.isTombstone);
                        }
                        lastKey = entry.key;
                    }

                    // Advance the iterator that supplied this entry
                    SSTableIterator it = entry.sourceIterator;
                    if (it.hasNext()) {
                        it.next();
                        minHeap.add(new MergeEntry(it.getKey(), it.getValue(), it.getSequenceNumber(), it.isTombstone(), it));
                    }
                }

                SSTableReaderInstance newReader = builder.finish();
                outputs.add(newReader);
            } finally {
                builder.close();
            }

        } finally {
            for (SSTableIterator it : activeIterators) {
                try {
                    it.close();
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Failed to close SSTable iterator during compaction", e);
                }
            }
        }

        // Apply RCU Version Switch
        updateVersionRegistry(level, outputLevel, inputs, outputs);

        // Cleanup old SSTable physical files asynchronously or synchronously
        cleanupOldFiles(inputs);

        LOGGER.info(String.format("Compaction completed successfully. Output level %d created %d new SSTables.", outputLevel, outputs.size()));
    }

    private void updateVersionRegistry(int level, int outputLevel, List<SSTableReaderInstance> inputs, List<SSTableReaderInstance> outputs) {
        boolean updated;
        do {
            Version current = versionManager.getCurrentVersion();
            Map<Integer, List<SSTableReaderInstance>> newLevels = new java.util.HashMap<>();
            
            for (Map.Entry<Integer, List<SSTableReaderInstance>> entry : current.getAllLevels().entrySet()) {
                newLevels.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }

            // Remove input files from their respective levels
            for (SSTableReaderInstance input : inputs) {
                List<SSTableReaderInstance> lvlList = newLevels.get(input.getLevel());
                if (lvlList != null) {
                    lvlList.removeIf(r -> r.getFileId() == input.getFileId());
                }
            }

            // Add output files to the output level
            List<SSTableReaderInstance> outList = newLevels.computeIfAbsent(outputLevel, k -> new ArrayList<>());
            outList.addAll(outputs);

            // Sort output level files by minKey
            outList.sort(Comparator.by(r -> r.getMinKey(), MergeEntry::compareBytes));

            Version newVersion = new Version(current.getVersionId() + 1, newLevels);
            // In a strict compare-and-swap pattern for version manager:
            versionManager.commit(newLevels);
            updated = true;
        } while (!updated);
    }

    private void cleanupOldFiles(List<SSTableReaderInstance> inputs) {
        for (SSTable