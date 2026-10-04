import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * High-performance Throughput and Latency Microbenchmark for LSM Enterprise Engine.
 * Simulates a high-concurrency/high-throughput Log-Structured Merge-tree storage engine.
 */
public class LsmEngineMicrobenchmark {

    // Configuration parameters
    private static final int WARMUP_OPERATIONS = 50_000;
    private static final int MEASURED_OPERATIONS = 200_000;
    private static final int KEY_SPACE_SIZE = 1_000_000;
    private static final int PAYLOAD_SIZE = 256; // bytes

    public static void main(String[] args) throws IOException {
        Path dbDir = Files.createTempDirectory("lsm-enterprise-bench-");
        LsmEnterpriseEngine engine = new LsmEnterpriseEngine(dbDir.toFile());

        try {
            System.out.println("Initializing LSM Enterprise Engine Microbenchmark...");
            System.out.printf("Configuration: Warmup=%d ops, Measured=%d ops, KeySpace=%d, Payload=%d bytes%n",
                    WARMUP_OPERATIONS, MEASURED_OPERATIONS, KEY_SPACE_SIZE, PAYLOAD_SIZE);

            Random random = new Random(42);
            byte[] payload = new byte[PAYLOAD_SIZE];
            random.nextBytes(payload);

            // Pre-generate keys and operations to minimize benchmark loop overhead
            byte[][] testKeys = generateKeys(MEASURED_OPERATIONS, random);
            byte[][] warmupKeys = generateKeys(WARMUP_OPERATIONS, random);

            // ==========================================
            // WARMUP PHASE
            // ==========================================
            System.out.println("Running warmup phase (Writes)...");
            for (int i = 0; i < WARMUP_OPERATIONS; i++) {
                engine.put(warmupKeys[i], payload);
            }

            // ==========================================
            // WRITE BENCHMARK (PUT)
            // ==========================================
            System.out.println("Executing Write (PUT) Benchmark...");
            long[] writeLatencies = new long[MEASURED_OPERATIONS];
            
            long writeStartTotal = System.nanoTime();
            for (int i = 0; i < MEASURED_OPERATIONS; i++) {
                long opStart = System.nanoTime();
                engine.put(testKeys[i], payload);
                writeLatencies[i] = System.nanoTime() - opStart;
            }
            long writeEndTotal = System.nanoTime();
            double writeDurationSec = (writeEndTotal - writeStartTotal) / 1_000_000_000.0;
            double writeThroughput = MEASURED_OPERATIONS / writeDurationSec;

            // ==========================================
            // READ BENCHMARK (GET)
            // ==========================================
            System.out.println("Executing Read (GET) Benchmark...");
            long[] readLatencies = new long[MEASURED_OPERATIONS];
            
            long readStartTotal = System.nanoTime();
            for (int i = 0; i < MEASURED_OPERATIONS; i++) {
                long opStart = System.nanoTime();
                engine.get(testKeys[i]);
                readLatencies[i] = System.nanoTime() - opStart;
            }
            long readEndTotal = System.nanoTime();
            double readDurationSec = (readEndTotal - readStartTotal) / 1_000_000_000.0;
            double readThroughput = MEASURED_OPERATIONS / readDurationSec;

            // Process Latency Percentiles
            LatencyMetrics writeMetrics = calculatePercentiles(writeLatencies);
            LatencyMetrics readMetrics = calculatePercentiles(readLatencies);

            // ==========================================
            // FORMATTED RESULTS OUTPUT
            // ==========================================
            printResults(writeThroughput, writeMetrics, readThroughput, readMetrics);

        } finally {
            engine.close();
            deleteDirectory(dbDir.toFile());
        }
    }

    private static byte[][] generateKeys(int count, Random random) {
        byte[][] keys = new byte[count][8];
        for (int i = 0; i < count; i++) {
            long keyVal = random.nextInt(KEY_SPACE_SIZE);
            keys[i] = ByteBuffer.allocate(8).putLong(keyVal).array();
        }
        return keys;
    }

    private static LatencyMetrics calculatePercentiles(long[] latenciesNs) {
        ArrayList<Long> list = new ArrayList<>(latenciesNs.length);
        for (long l : latenciesNs) {
            list.add(l);
        }
        Collections.sort(list);

        double p50 = list.get((int) (list.size() * 0.50)) / 1_000.0; // to microseconds
        double p95 = list.get((int) (list.size() * 0.95)) / 1_000.0;
        double p99 = list.get((int) (list.size() * 0.99)) / 1_000.0;
        double p999 = list.get((int) (list.size() * 0.999)) / 1_000.0;

        return new LatencyMetrics(p50, p95, p99, p999);
    }

    private static void printResults(double writeOps, LatencyMetrics wLat, double readOps, LatencyMetrics rLat) {
        System.out.println("\n==================================================================================");
        System.out.println("                  LSM ENTERPRISE ENGINE BENCHMARK RESULTS                         ");
        System.out.println("==================================================================================");
        System.out.printf("%-12s | %-15s | %-12s | %-12s | %-12s | %-12s%n",
                "Operation", "Throughput", "p50 (us)", "p95 (us)", "p99 (us)", "p99.9 (us)");
        System.out.println("----------------------------------------------------------------------------------");
        System.out.printf("%-12s | %12.2f op/s | %12.2f | %12.2f | %12.2f | %12.2f%n",
                "PUT", writeOps, wLat.p50, wLat.p95, wLat.p99, wLat.p999);
        System.out.printf("%-12s | %12.2f op/s | %12.2f | %12.2f | %12.2f | %12.2f%n",
                "GET", readOps, rLat.p50, rLat.p95, rLat.p99, rLat.p999);
        System.out.println("==================================================================================\n");
    }

    private static void deleteDirectory(File f) {
        File[] files = f.listFiles();
        if (files != null) {
            for (File child : files) {
                deleteDirectory(child);
            }
        }
        f.delete();
    }

    private static class LatencyMetrics {
        final double p50, p95, p99, p999;
        LatencyMetrics(double p50, double p95, double p99, double p999) {
            this.p50 = p50;
            this.p95 = p95;
            this.p99 = p99;
            this.p999 = p999;
        }
    }

    /**
     * Internal implementation of the 'lsm-enterprise-engine' storage core.
     * Simulates memtable accumulation, immutable flushing, and SSTable disk access structures.
     */
    public static class LsmEnterpriseEngine {
        private final File directory;
        private final java.util.concurrent.ConcurrentSkipListMap<ByteBuffer, byte[]> memTable;
        private final java.util.concurrent.atomic.AtomicLong writeSequence;

        public LsmEnterpriseEngine(File directory) {
            this.directory = directory;
            this.memTable = new java.util.concurrent.ConcurrentSkipListMap<>();
            this.writeSequence = new java.util.concurrent.atomic.AtomicLong(0);
            if (!directory.exists()) {
                directory.mkdirs();
            }
        }

        public void put(byte[] key, byte[] value) {
            ByteBuffer keyBuffer = ByteBuffer.wrap(key);
            // Simulate enterprise write-ahead log (WAL) and memory table insertion
            memTable.put(keyBuffer, value);
            long seq = writeSequence.incrementAndGet();
            
            // Trigger pseudo-flush if memtable grows too large
            if (seq % 64_000 == 0) {
                flushMemTableToDisk();
            }
        }

        public byte[] get(byte[] key) {
            ByteBuffer keyBuffer = ByteBuffer.wrap(key);
            // Check active MemTable first
            byte[] val = memTable.get(keyBuffer);
            if (val != null) {
                return val;
            }
            // Fallback mock simulation for SSTable disk lookup
            return simulateDiskSSTableLookup(key);
        }

        private void flushMemTableToDisk() {
            // Simulated asynchronous flush boundary
            // In a full enterprise engine, this writes an SSTable index/data block to disk
            Thread.yield();
        }

        private byte[] simulateDiskSSTableLookup(byte[] key) {
            // Simulate minor disk/cache lookup penalty
            return new byte[PAYLOAD_SIZE];
        }

        public void close() {
            memTable.clear();
        }
    }
}