package com.enterprise.lsm.core;

import java.util.Objects;

/**
 * Core type definitions, operation codes, engine states, and structural constants
 * for the lsm-enterprise-engine.
 */
public final class Types {

    private Types() {
        // Prevent instantiation of utility class
    }

    /**
     * Supported mutation operations in the LSM-Tree engine.
     */
    public enum OperationType {
        PUT((byte) 0x01),
        DELETE((byte) 0x02);

        private final byte code;

        OperationType(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return code;
        }

        public static OperationType fromCode(byte code) {
            for (OperationType op : values()) {
                if (op.code == code) {
                    return op;
                }
            }
            throw new IllegalArgumentException("Unknown OperationType code: " + code);
        }
    }

    /**
     * Engine operational modes governing read/write availability and failure recovery states.
     */
    public enum EngineState {
        INITIALIZING,
        RUNNING,
        READ_ONLY_DEGRADED,
        RECOVERING,
        SHUTTING_DOWN,
        TERMINATED
    }

    /**
     * Compaction tier levels from MemTable flush ($L_0$) up to deep background levels ($L_k$).
     */
    public enum CompactionLevel {
        L0(0),
        L1(1),
        L2(2),
        L3(3),
        L4(4);

        private final int level;

        CompactionLevel(int level) {
            this.level = level;
        }

        public int getLevel() {
            return level;
        }

        public CompactionLevel next() {
            int nextLevel = Math.min(this.level + 1, L4.level);
            return fromInt(nextLevel);
        }

        public static CompactionLevel fromInt(int level) {
            for (CompactionLevel cl : values()) {
                if (cl.level == level) {
                    return cl;
                }
            }
            throw new IllegalArgumentException("Unknown CompactionLevel integer: " + level);
        }
    }

    /**
     * Immutable wrapper representing a raw byte slice key or value, ensuring
     * safe off-heap or on-heap boundary handling and equality checks.
     */
    public static final class Slice implements Comparable<Slice> {
        private final byte[] data;
        private final int offset;
        private final int length;

        public Slice(byte[] data) {
            this.data = Objects.requireNonNull(data, "Data array cannot be null");
            this.offset = 0;
            this.length = data.length;
        }

        public Slice(byte[] data, int offset, int length) {
            this.data = Objects.requireNonNull(data, "Data array cannot be null");
            if (offset < 0 || length < 0 || offset + length > data.length) {
                throw new IndexOutOfBoundsException("Invalid offset (" + offset + ") or length (" + length + ") for array size " + data.length);
            }
            this.offset = offset;
            this.length = length;
        }

        public byte[] getData() {
            return data;
        }

        public int getOffset() {
            return offset;
        }

        public int getLength() {
            return length;
        }

        public byte[] getBytes() {
            byte[] copy = new byte[length];
            System.arraycopy(data, offset, copy, 0, length);
            return copy;
        }

        @Override
        public int compareTo(Slice other) {
            int minLen = Math.min(this.length, other.length);
            for (int i = 0; i < minLen; i++) {
                int b1 = this.data[this.offset + i] & 0xFF;
                int b2 = other.data[other.offset + i] & 0xFF;
                if (b1 != b2) {
                    return b1 - b2;
                }
            }
            return this.length - other.length;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Slice)) return false;
            Slice slice = (Slice) o;
            if (this.length != slice.length) return false;
            for (int i = 0; i < length; i++) {
                if (this.data[this.offset + i] != slice.data[slice.offset + i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = 1;
            for (int i = 0; i < length; i++) {
                result = 31 * result + (data[offset + i] & 0xFF);
            }
            return result;
        }

        @Override
        public String toString() {
            return new String(data, offset, length, java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    /**
     * System-wide sizing and operational limits conforming to the LSM engine specification.
     */
    public static final class Constants {
        private Constants() {}

        public static final int BLOCK_SIZE_BYTES = 4096; // 4KB SSTable data blocks
        public static final int WAL_HEADER_SIZE = 21;    // CRC32(4) + Length(4) + Seq(8) + Op(1) + KeyLen(4)
        public static final double BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;
        public static final int MEMTABLE_ARENA_CHUNK_SIZE = 4 * 1024 * 1024; // 4MB Arena chunks
        public static final int GROUP_COMMIT_RING_BUFFER_SIZE = 4096;
    }
}