/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.NodeNamePatternConverter;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.simdvec.VectorSimilarityType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.rethrow;

@Fork(value = 3, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class VectorScorerSignedInt8OperationBenchmark {

    static {
        NodeNamePatternConverter.setGlobalNodeName("foo");
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    byte[] bytesA;
    byte[] bytesB;
    byte[] scratch;
    MemorySegment heapSegA, heapSegB;
    MemorySegment nativeSegA, nativeSegB;

    Arena arena;

    @Param({ "1", "128", "207", "256", "300", "512", "702", "1024", "1536", "2048" })
    public int size;

    @Param({ "DOT_PRODUCT" })
    public VectorSimilarityType function;

    @FunctionalInterface
    private interface LuceneFunction {
        int run(byte[] vec1, byte[] vec2);
    }

    @FunctionalInterface
    private interface NativeFunction {
        int run(MemorySegment vec1, MemorySegment vec2, int length);
    }

    private LuceneFunction luceneImpl;
    private NativeFunction nativeImpl;

    @Setup(Level.Iteration)
    public void init() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        bytesA = new byte[size];
        bytesB = new byte[size];
        scratch = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytesA);
        ThreadLocalRandom.current().nextBytes(bytesB);

        heapSegA = MemorySegment.ofArray(bytesA);
        heapSegB = MemorySegment.ofArray(bytesB);

        arena = Arena.ofConfined();
        nativeSegA = arena.allocate((long) bytesA.length);
        MemorySegment.copy(MemorySegment.ofArray(bytesA), 0L, nativeSegA, 0L, bytesA.length);
        nativeSegB = arena.allocate((long) bytesB.length);
        MemorySegment.copy(MemorySegment.ofArray(bytesB), 0L, nativeSegB, 0L, bytesB.length);

        luceneImpl = switch (function) {
            case DOT_PRODUCT -> VectorUtil::dotProduct;
            default -> throw new UnsupportedOperationException("Not used");
        };
        nativeImpl = switch (function) {
            case DOT_PRODUCT -> VectorScorerSignedInt8OperationBenchmark::dotProductbyte;
            default -> throw new UnsupportedOperationException("Not used");
        };
    }

    @TearDown
    public void teardown() {
        arena.close();
    }

    @Benchmark
    public int lucene() {
        return luceneImpl.run(bytesA, bytesB);
    }

    @Benchmark
    public int luceneWithCopy() {
        // add a copy to better reflect what Lucene has to do to get the target vector on-heap
        MemorySegment.copy(nativeSegB, ValueLayout.JAVA_INT, 0L, scratch, 0, scratch.length);
        return luceneImpl.run(bytesA, scratch);
    }

    @Benchmark
    public int nativeWithNativeSeg() {
        return nativeImpl.run(nativeSegA, nativeSegB, size);
    }

    @Benchmark
    public int nativeWithHeapSeg() {
        return nativeImpl.run(heapSegA, heapSegB, size);
    }

    static final VectorSimilarityFunctions vectorSimilarityFunctions = vectorSimilarityFunctions();

    static VectorSimilarityFunctions vectorSimilarityFunctions() {
        return NativeAccess.instance().getVectorSimilarityFunctions().get();
    }

    static int dotProductbyte(MemorySegment a, MemorySegment b, int length) {
        try {
            return (int) vectorSimilarityFunctions.dotProductHandle8s().invokeExact(a, b, length);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

}
