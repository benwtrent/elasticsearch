/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorizationProvider;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
// first iteration is complete garbage, so make sure we really warmup
@Warmup(iterations = 4, time = 1)
// real iterations. not useful to spend tons of time here, better to fork more
@Measurement(iterations = 5, time = 1)
// engage some noise reduction
@Fork(value = 1)
public class OSQNativeScorerBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Param({ "1024" })
    int dims;

    int length;
    int bulkSize = ES91OSQVectorsScorer.BULK_SIZE;

    int numVectors = bulkSize;
    int numQueries = 10;

    byte[][] binaryVectors;
    byte[][] binaryQueries;

    byte[] scratch;
    ES91OSQVectorsScorer scorer;

    IndexInput in;

    float[] scratchScores;

    @Setup
    public void setup() throws IOException {
        Random random = new Random(123);

        this.length = OptimizedScalarQuantizer.discretize(dims, 64) / 8;

        binaryVectors = new byte[numVectors][length];
        for (byte[] binaryVector : binaryVectors) {
            random.nextBytes(binaryVector);
        }

        Directory dir = new MMapDirectory(Files.createTempDirectory("vectorData"));
        IndexOutput out = dir.createOutput("vectors", IOContext.DEFAULT);
        for (int i = 0; i < numVectors; i += bulkSize) {
            for (int j = 0; j < bulkSize; j++) {
                out.writeBytes(binaryVectors[i + j], 0, binaryVectors[i + j].length);
            }
        }
        out.close();
        in = dir.openInput("vectors", IOContext.DEFAULT);

        binaryQueries = new byte[numVectors][4 * length];
        for (byte[] binaryVector : binaryVectors) {
            random.nextBytes(binaryVector);
        }

        scratch = new byte[length];
        scorer = ESVectorizationProvider.getInstance().newES91OSQVectorsScorer(in, length);
        scratchScores = new float[bulkSize];
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public void scoreFromMemorySegmentBulk(Blackhole bh) throws IOException {
        for (int j = 0; j < numQueries; j++) {
            in.seek(0);
            for (int i = 0; i < numVectors; i += bulkSize) {
                scorer.quantizeScoreBulk(
                    binaryQueries[j],
                    bulkSize,
                    scratchScores
                );
                bh.consume(scratchScores);
            }
        }
    }
}
