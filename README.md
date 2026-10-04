# lsm-enterprise-engine
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/lsm-enterprise-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/lsm-enterprise-engine/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/lsm-enterprise-engine) [![Stars](https://img.shields.io/github/stars/sohamshewani/lsm-enterprise-engine?style=flat)](https://github.com/sohamshewani/lsm-enterprise-engine)

## 1. Executive Summary & Problem Space
`lsm-enterprise-engine` is a zero-allocation, lock-free (where possible) Log-Structured Merge-tree storage engine designed for mission-critical, high-throughput enterprise Java workloads. Traditional JVM-based storage engines suffer from unpredicted latency spikes induced by Garbage Collection (GC) pressure, heavyweight heap allocations for keys/values, and lock contention during concurrent write paths. 

`lsm-enterprise-engine` addresses these bottlenecks by utilizing direct off-heap memory arenas via custom allocators, immutable memtables backed by lock-free concurrent skiplists, sequential append-only write-ahead logging (WAL), memory-mapped SSTables, and background tiering compaction. It delivers predictable sub-millisecond latencies, making it an ideal drop-in storage subsystem for distributed databases, streaming analytics state stores, and transactional metadata systems.

## 2. Structural ASCII Architecture / Dataflow Diagram

```
 +-----------------------------------------------------------------------+
 |                        Client Write / Read Request                    |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                       WriteAheadLog (WAL)                             |
 |       - Sequential Disk Append (fsync on flush)                       |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                     SkipListMemTable (Active)                         |
 |       - Lock-Free Concurrent SkipList                                 |
 |       - Off-Heap Arena Allocator (Zero GC Overhead)                   |
 +-----------------------------------------------------------------------+
                                    | (Memtable Full / Threshold Reached)
                                    v
 +-----------------------------------------------------------------------+
 |                    Immutable MemTable (Flushing)                      |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                       SSTable (Level 0 - N)                           |
 |       - Block-based Indexing & Bloom Filters                          |
 |       - Memory-Mapped File Access (mmap)                              |
 +-----------------------------------------------------------------------+
                                    ^
                                    | (Background Level Compaction)
 +-----------------------------------------------------------------------+
 |                           Compactor Thread                            |
 |       - K-Way Merge Sort Across Levels                                |
 |       - Tombstone Excision & Garbage Collection                       |
 +-----------------------------------------------------------------------+
```

## 3. Concurrency Model & Memory Invariants
* **Thread Safety:** Write operations target the active `SkipListMemTable` concurrently via lock-free concurrent algorithms. Reads query the active memtable first, falling back to immutable flushing memtables and sorted string tables (SSTables) in descending sequence order.
* **Lock Contention Prevention:** Synchronization points are isolated to atomic pointer swaps during memtable rotation. Background compaction threads operate on immutable snapshots of SSTables, eliminating read-write interference.
* **Memory Ownership & Off-Heap Allocation (`Allocator.java`):** To circumvent JVM GC overhead during heavy throughput bursts, keys and values are allocated inside off-heap memory arenas managed directly by the engine. Memory arenas enforce strict lifecycle ownership boundaries; buffers are safely recycled or explicitly deallocated upon SSTable sealing or compaction completion.
* **Atomic Primitives:** State transitions (e.g., active vs. immutable memtable pointers) are governed by atomic volatile references (`AtomicReference`), guaranteeing memory visibility across multi-core architectures without explicit monitor locks.

## 4. Performance Characteristics & Algorithmic Complexity Table

| Operation | Average Case | Worst Case | Estimated Latency (P99) | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Point Write (Put)** | $O(1)$ | $O(\log N)$ | $< 250\mu s$ | Append-only WAL + SkipList insertion |
| **Point Read (Get)** | $O(\log N)$ | $O(L \cdot \log N)$ | $< 500\mu s$ | Memtable -> Bloom Filter Check -> SSTable binary search |
| **Range Scan** | $O(K + \log N)$ | $O(K + L \cdot \log N)$ | $< 2ms$ | Sorted iterator merge across memtables and levels |
| **Compaction (Merge)**| $O(N)$ | $O(N \log N)$ | Background | K-way merge sort running out-of-core |

*Where $N$ is the total dataset size, $L$ is the number of LSM levels, and $K$ is the number of scanned keys.*

## 5. Security Architecture & Threat Model
* **Input Boundaries & Sanitization:** All incoming byte arrays, keys, and values undergo strict length validation and bounds-checking at the engine boundary to prevent buffer over-reads and segmentation faults via JNI/Unsafe pathways.
* **Memory Corruption Protections:** Off-heap allocations utilize guard pages and magic bytes where applicable. Checksums (CRC32C) are appended to all WAL records and SSTable data blocks to detect data corruption at rest.
* **Threat Vectors Mitigated:** 
  * *Denial of Service (Heap Exhaustion):* Prevented by hard limits on active memtable sizing and off-heap memory quotas.
  * *Data Tampering / Bit Rot:* Mitigated via persistent block-level checksum verification during reads and compaction cycles.

## 6. Build, Verification & Benchmarking Recipes

### Prerequisites
* Java Development Kit (JDK) 17+ (Temurin or OpenJDK recommended)
* Apache Maven 3.8+
* Python 3.9+ (Required for test invariants and benchmarking harnesses)

### Compilation
```bash
# Clean and compile the enterprise engine distribution
mvn clean compile package -DskipTests
```

### Verification (Unit & Invariant Tests)
```bash
# Execute Maven unit and integration test suite
mvn test

# Execute Python state-machine and concurrency invariant tests
python3 tests/test_invariants.py
```

### Benchmarking Execution
```bash
# Run high-throughput concurrency and latency benchmarks
python3 benchmarks/bench_engine.py --threads 16 --operations 1000000