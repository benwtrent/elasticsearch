/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector.scorer;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.DataType;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.Function;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.Operation;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Kernel-only benchmark for row-major vs vertical int7 native bulk dot products.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class VectorInt7LayoutKernelBenchmark {
    static {
        Utils.configureBenchmarkLogging();
    }

    public enum Implementation {
        ROW_NATIVE,
        VERTICAL_NATIVE
    }

    @Param({ "384", "768", "1024" })
    public int dims;

    @Param
    public Implementation implementation;

    private static final int BULK_SIZE = 64;
    private static final int NUM_VECTORS = BULK_SIZE * 10;
    private static final int NUM_QUERIES = 10;

    private MethodHandle rowBulkHandle;
    private MethodHandle verticalBulkHandle;
    private MemorySegment[] rowBlocks;
    private MemorySegment[] verticalBlocks;
    private MemorySegment[] queries;
    private float[] scores;
    private MemorySegment scoresSegment;
    private int blocks;

    @Setup
    public void setup() {
        final VectorSimilarityFunctions similarityFunctions = NativeAccess.instance()
            .getVectorSimilarityFunctions()
            .orElseThrow(() -> new IllegalStateException("native vector similarity functions unavailable"));
        rowBulkHandle = similarityFunctions.getHandle(Function.DOT_PRODUCT, DataType.INT7U, Operation.BULK);
        final Optional<MethodHandle> vertical = similarityFunctions.getInt7uVerticalDotProductBulkHandle();
        if (implementation == Implementation.VERTICAL_NATIVE && vertical.isEmpty()) {
            throw new IllegalStateException("vertical int7 native kernel is unavailable on this platform");
        }
        verticalBulkHandle = vertical.orElse(null);

        final Random random = new Random(123L);
        final byte[] rowData = new byte[NUM_VECTORS * dims];
        final byte[][] queryData = new byte[NUM_QUERIES][dims];
        fillInt7u(random, rowData);
        for (int i = 0; i < NUM_QUERIES; i++) {
            fillInt7u(random, queryData[i]);
        }

        blocks = NUM_VECTORS / BULK_SIZE;
        final int rowBlockBytes = dims * BULK_SIZE;
        final int verticalBlockBytes = bytesForCount(dims, BULK_SIZE);
        final byte[] verticalData = new byte[blocks * verticalBlockBytes];
        for (int b = 0; b < blocks; b++) {
            transposeBlockToVertical(rowData, b * rowBlockBytes, dims, BULK_SIZE, verticalData, b * verticalBlockBytes);
        }

        final MemorySegment rowSegment = MemorySegment.ofArray(rowData);
        final MemorySegment verticalSegment = MemorySegment.ofArray(verticalData);
        rowBlocks = new MemorySegment[blocks];
        verticalBlocks = new MemorySegment[blocks];
        for (int b = 0; b < blocks; b++) {
            rowBlocks[b] = rowSegment.asSlice((long) b * rowBlockBytes, rowBlockBytes);
            verticalBlocks[b] = verticalSegment.asSlice((long) b * verticalBlockBytes, verticalBlockBytes);
        }

        queries = new MemorySegment[NUM_QUERIES];
        for (int i = 0; i < NUM_QUERIES; i++) {
            queries[i] = MemorySegment.ofArray(queryData[i]);
        }
        scores = new float[BULK_SIZE];
        scoresSegment = MemorySegment.ofArray(scores);
    }

    @Benchmark
    public float bulkKernel() throws Throwable {
        float checksum = 0f;
        final MethodHandle handle = implementation == Implementation.ROW_NATIVE ? rowBulkHandle : verticalBulkHandle;
        final MemorySegment[] blocksToUse = implementation == Implementation.ROW_NATIVE ? rowBlocks : verticalBlocks;
        for (int q = 0; q < NUM_QUERIES; q++) {
            final MemorySegment query = queries[q];
            for (int b = 0; b < blocks; b++) {
                handle.invokeExact(blocksToUse[b], query, dims, BULK_SIZE, scoresSegment);
                for (int i = 0; i < BULK_SIZE; i++) {
                    checksum += scores[i];
                }
            }
        }
        return checksum;
    }

    private static void fillInt7u(Random random, byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) random.nextInt(128);
        }
    }

    private static int bytesForCount(int dimensions, int count) {
        final int groupedDims = dimensions & ~3;
        final int groups = groupedDims / 4;
        final int tailDims = dimensions - groupedDims;
        return groups * count * 4 + tailDims * count;
    }

    private static void transposeBlockToVertical(byte[] rowData, int rowOffset, int dims, int count, byte[] out, int outOffset) {
        final int groupedDims = dims & ~3;
        final int groups = groupedDims / 4;
        final int tailDims = dims - groupedDims;
        int cursor = outOffset;

        for (int g = 0; g < groups; g++) {
            final int dimBase = g * 4;
            for (int v = 0; v < count; v++) {
                final int src = rowOffset + v * dims + dimBase;
                out[cursor++] = rowData[src];
                out[cursor++] = rowData[src + 1];
                out[cursor++] = rowData[src + 2];
                out[cursor++] = rowData[src + 3];
            }
        }

        for (int v = 0; v < count; v++) {
            final int src = rowOffset + v * dims + groupedDims;
            for (int d = 0; d < tailDims; d++) {
                out[cursor++] = rowData[src + d];
            }
        }
    }
}
