package com.enterprise.lsm.core;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class SkipListMemTable implements Iterable<Map.Entry<byte[], byte[]>> {

    private final ConcurrentSkipListMap<ByteArrayWrapper, byte[]> map;
    private final AtomicLong sequenceNumber;
    private final LongAdder byteSize;
    private final long capacityBytes;
    private volatile boolean frozen;

    public SkipListMemTable(long capacityBytes) {
        this.map = new ConcurrentSkipListMap<>();
        this.sequenceNumber = new AtomicLong(0);
        this.byteSize = new LongAdder();
        this.capacityBytes = capacityBytes;
        this.frozen = false;
    }

    public boolean put(long seq, byte[] key, byte[] value) {
        if (frozen) {
            return false;
        }
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }

        ByteArrayWrapper wrappedKey = new ByteArrayWrapper(key);
        byte[] oldVal = map.put(wrappedKey, value);
        
        long addedSize = key.length + value.length;
        if (oldVal != null) {
            addedSize -= oldVal.length;
        }
        byteSize.add(addedSize);
        sequenceNumber.set(Math.max(sequenceNumber.get(), seq));

        return true;
    }

    public boolean delete(long seq, byte[] key) {
        if (frozen) {
            return false;
        }
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        ByteArrayWrapper wrappedKey = new ByteArrayWrapper(key);
        byte[] oldVal = map.remove(wrappedKey);
        
        if (oldVal != null) {
            byteSize.add(-(key.length + oldVal.length));
        } else {
            byteSize.add(key.length);
        }
        sequenceNumber.set(Math.max(sequenceNumber.get(), seq));

        return true;
    }

    public byte[] get(byte[] key) {
        if (key == null) {
            return null;
        }
        return map.get(new ByteArrayWrapper(key));
    }

    public void freeze() {
        this.frozen = true;
    }

    public boolean isFrozen() {
        return this.frozen;
    }

    public boolean isFull() {
        return byteSize.sum() >= capacityBytes;
    }

    public long getByteSize() {
        return byteSize.sum();
    }

    public long getSequenceNumber() {
        return sequenceNumber.get();
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
        Iterator<Map.Entry<ByteArrayWrapper, byte[]>> internalIter = map.entrySet().iterator();
        return new Iterator<Map.Entry<byte[], byte[]>>() {
            @Override
            public boolean hasNext() {
                return internalIter.hasNext();
            }

            @Override
            public Map.Entry<byte[], byte[]> next() {
                Map.Entry<ByteArrayWrapper, byte[]> entry = internalIter.next();
                return new Map.Entry<byte[], byte[]>() {
                    @Override
                    public byte[] getKey() {
                        return entry.getKey().getBytes();
                    }

                    @Override
                    public byte[] getValue() {
                        return entry.getValue();
                    }

                    @Override
                    public byte[] setValue(byte[] value) {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public static final class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
        private final byte[] data;

        public ByteArrayWrapper(byte[] data) {
            this.data = data;
        }

        public byte[] getBytes() {
            return data;
        }

        @Override
        public int compareTo(ByteArrayWrapper o) {
            byte[] other = o.data;
            int len = Math.min(data.length, other.length);
            for (int i = 0; i < len; i++) {
                int cmp = Byte.compare(data[i], other[i]);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return Integer.compare(data.length, other.length);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            ByteArrayWrapper that = (ByteArrayWrapper) obj;
            return java.util.Arrays.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            return java.util.Arrays.hashCode(data);
        }
    }
}