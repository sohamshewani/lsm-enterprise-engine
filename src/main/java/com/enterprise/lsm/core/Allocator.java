package com.enterprise.lsm.core;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

public final class Allocator {

    private static final int CHUNK_SIZE = 4 * 1024 * 1024; // 4MB per arena chunk
    private static final int MAX_CHUNKS = 1024; // Max 4GB allocated via arena

    private static final class Chunk {
        final ByteBuffer buffer;
        final AtomicLong offset = new AtomicLong(0);

        Chunk(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        ByteBuffer allocate(int size) {
            while (true) {
                long currentOffset = offset.get();
                if (currentOffset + size > CHUNK_SIZE) {
                    return null; // Chunk is full
                }
                if (offset.compareAndSet(currentOffset, currentOffset + size)) {
                    // Create a slice for the requested allocation
                    ByteBuffer slice = buffer.duplicate();
                    slice.position((int) currentOffset);
                    slice.limit((int) (currentOffset + size));
                    return slice.slice();
                }
            }
        }
    }

    private final AtomicReferenceArray<Chunk> chunks;
    private final AtomicLong activeChunkIndex;
    private final ConcurrentLinkedQueue<ByteBuffer> smallObjectFallbackQueue;

    public Allocator() {
        this.chunks = new AtomicReferenceArray<>(MAX_CHUNKS);
        this.activeChunkIndex = new AtomicLong(0);
        this.smallObjectFallbackQueue = new ConcurrentLinkedQueue<>();
        
        // Initialize the first chunk
        ByteBuffer initialBuffer = ByteBuffer.allocateDirect(CHUNK_SIZE);
        chunks.set(0, new Chunk(initialBuffer));
    }

    public ByteBuffer allocate(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Allocation size must be positive: " + size);
        }

        // If allocation size is larger than the chunk size, fallback to direct allocation
        if (size > CHUNK_SIZE) {
            ByteBuffer fallback = ByteBuffer.allocateDirect(size);
            smallObjectFallbackQueue.offer(fallback);
            return fallback;
        }

        while (true) {
            long idx = activeChunkIndex.get();
            Chunk currentChunk = chunks.get((int) idx);

            if (currentChunk != null) {
                ByteBuffer allocation = currentChunk.allocate(size);
                if (allocation != null) {
                    return allocation;
                }
            }

            // Current chunk is full, try to advance to the next chunk or allocate a new one
            long nextIdx = idx + 1;
            if (nextIdx >= MAX_CHUNKS) {
                // Arena exhaustion fallback
                ByteBuffer fallback = ByteBuffer.allocateDirect(size);
                smallObjectFallbackQueue.offer(fallback);
                return fallback;
            }

            // Attempt to create and set the next chunk
            Chunk newChunk = new Chunk(ByteBuffer.allocateDirect(CHUNK_SIZE));
            if (chunks.compareAndSet((int) nextIdx, null, newChunk)) {
                activeChunkIndex.compareAndSet(idx, nextIdx);
            } else {
                // Another thread already initialized the next chunk, just update index if needed
                activeChunkIndex.compareAndSet(idx, nextIdx);
            }
        }
    }

    public void reset() {
        long currentActive = activeChunkIndex.get();
        for (int i = 0; i <= currentActive; i++) {
            Chunk chunk = chunks.get(i);
            if (chunk != null) {
                chunk.offset.set(0);
            }
        }
        activeChunkIndex.set(0);
        smallObjectFallbackQueue.clear();
    }

    public long getCapacityBytes() {
        long currentActive = activeChunkIndex.get();
        return (currentActive + 1) * (long) CHUNK_SIZE;
    }

    public long getUsedBytes() {
        long totalUsed = 0;
        long currentActive = activeChunkIndex.get();
        for (int i = 0; i <= currentActive; i++) {
            Chunk chunk = chunks.get(i);
            if (chunk != null) {
                totalUsed += chunk.offset.get();
            }
        }
        return totalUsed;
    }
}