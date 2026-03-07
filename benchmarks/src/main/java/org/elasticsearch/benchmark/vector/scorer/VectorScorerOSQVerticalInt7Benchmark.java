/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;
import org.elasticsearch.simdvec.ESNextOSQVectorsScorer;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorizationProvider;
import org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.createOSQIndexData;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.createOSQQueryData;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.randomVector;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.writeBulkOSQVectorData;

/**
 * Benchmark for experimental vertical-only int7 dot-product against existing OSQ scorers.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class VectorScorerOSQVerticalInt7Benchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    public enum Implementation {
        ROW_SCALAR,
        ROW_VECTORIZED,
        VERTICAL_EXPERIMENTAL
    }

    @Param({ "768" })
    public int dims;

    @Param
    public Implementation implementation;

    @Param({ "64", "256", "512", "1024" })
    public int bulkSize;

    private static final VectorSimilarityFunction SIMILARITY = VectorSimilarityFunction.DOT_PRODUCT;
    static final int NUM_QUERIES = 10;

    record VectorData(
        VectorScorerTestUtils.OSQVectorData[] indexVectors,
        VectorScorerTestUtils.OSQVectorData[] queries,
        float centroidDp
    ) {}

    private VectorScorerTestUtils.OSQVectorData[] binaryQueries;
    private float centroidDp;

    private Path rowTempDir;
    private Path verticalTempDir;
    private Directory rowDirectory;
    private Directory verticalDirectory;
    private IndexInput rowInput;
    private IndexInput verticalInput;
    private IndexInput activeInput;

    private int numVectors;
    private ESNextOSQVectorsScorer rowScorer;
    private ES92Int7VectorsScorer verticalScorer;
    private float[] scratchScores;

    static VectorData generateRandomVectorData(Random random, int dims, int numVectors) {
        final float[] centroid = new float[dims];
        randomVector(random, centroid, SIMILARITY);
        final var quantizer = new OptimizedScalarQuantizer(SIMILARITY);

        final VectorScorerTestUtils.OSQVectorData[] indexVectors = new VectorScorerTestUtils.OSQVectorData[numVectors];
        final float[] vector = new float[dims];
        for (int i = 0; i < numVectors; i++) {
            randomVector(random, vector, SIMILARITY);
            indexVectors[i] = createOSQIndexData(vector, centroid, quantizer, dims, (byte) 7, dims);
        }

        final VectorScorerTestUtils.OSQVectorData[] queryVectors = new VectorScorerTestUtils.OSQVectorData[numVectors];
        final float[] query = new float[dims];
        for (int i = 0; i < numVectors; i++) {
            randomVector(random, query, SIMILARITY);
            queryVectors[i] = createOSQQueryData(query, centroid, quantizer, dims, (byte) 7, dims);
        }

        return new VectorData(indexVectors, queryVectors, VectorUtil.dotProduct(centroid, centroid));
    }

    @Setup
    public void setup() throws IOException {
        numVectors = bulkSize * 10;
        setup(generateRandomVectorData(new Random(123), dims, numVectors));
    }

    void setup(VectorData data) throws IOException {
        rowTempDir = Files.createTempDirectory("osqInt7Row");
        verticalTempDir = Files.createTempDirectory("osqInt7Vertical");
        rowDirectory = new MMapDirectory(rowTempDir);
        verticalDirectory = new MMapDirectory(verticalTempDir);

        try (IndexOutput out = rowDirectory.createOutput("vectors", IOContext.DEFAULT)) {
            for (int i = 0; i < numVectors; i += bulkSize) {
                writeBulkOSQVectorData(bulkSize, out, data.indexVectors(), i);
            }
            CodecUtil.writeFooter(out);
        }
        try (IndexOutput out = verticalDirectory.createOutput("vectors", IOContext.DEFAULT)) {
            for (int i = 0; i < numVectors; i += bulkSize) {
                byte[] verticalBlock = toVerticalInterleave4(data.indexVectors(), i, bulkSize, dims);
                out.writeBytes(verticalBlock, verticalBlock.length);
                writeBulkCorrections(data.indexVectors(), i, bulkSize, out);
            }
            CodecUtil.writeFooter(out);
        }

        rowInput = rowDirectory.openInput("vectors", IOContext.DEFAULT);
        verticalInput = verticalDirectory.openInput("vectors", IOContext.DEFAULT);
        binaryQueries = data.queries();
        centroidDp = data.centroidDp();
        scratchScores = new float[bulkSize];

        switch (implementation) {
            case ROW_SCALAR -> {
                rowScorer = new ESNextOSQVectorsScorer(rowInput, (byte) 7, (byte) 7, dims, dims, bulkSize);
                activeInput = rowInput;
            }
            case ROW_VECTORIZED -> {
                rowScorer = ESVectorizationProvider.getInstance()
                    .newESNextOSQVectorsScorer(rowInput, (byte) 7, (byte) 7, dims, dims, bulkSize);
                activeInput = rowInput;
            }
            case VERTICAL_EXPERIMENTAL -> {
                verticalScorer = newVerticalScorer(verticalInput, dims, bulkSize);
                activeInput = verticalInput;
            }
            default -> throw new IllegalArgumentException("Unsupported implementation: " + implementation);
        }
    }

    @TearDown
    public void teardown() throws IOException {
        IOUtils.close(rowDirectory, verticalDirectory, rowInput, verticalInput);
        IOUtils.rm(rowTempDir, verticalTempDir);
    }

    @Benchmark
    public float[] bulkScore() throws IOException {
        float[] results = new float[NUM_QUERIES * numVectors];
        for (int j = 0; j < NUM_QUERIES; j++) {
            activeInput.seek(0);
            for (int i = 0; i < numVectors; i += bulkSize) {
                if (implementation == Implementation.VERTICAL_EXPERIMENTAL) {
                    verticalScorer.scoreBulk(
                        binaryQueries[j].quantizedVector(),
                        binaryQueries[j].lowerInterval(),
                        binaryQueries[j].upperInterval(),
                        binaryQueries[j].quantizedComponentSum(),
                        binaryQueries[j].additionalCorrection(),
                        SIMILARITY,
                        centroidDp,
                        scratchScores,
                        bulkSize
                    );
                } else {
                    rowScorer.scoreBulk(
                        binaryQueries[j].quantizedVector(),
                        binaryQueries[j].lowerInterval(),
                        binaryQueries[j].upperInterval(),
                        binaryQueries[j].quantizedComponentSum(),
                        binaryQueries[j].additionalCorrection(),
                        SIMILARITY,
                        centroidDp,
                        scratchScores,
                        bulkSize
                    );
                }
                System.arraycopy(scratchScores, 0, results, j * numVectors + i, bulkSize);
            }
        }
        return results;
    }

    static byte[] toVerticalInterleave4(VectorScorerTestUtils.OSQVectorData[] vectors, int offset, int count, int dims) {
        final int groupedDims = dims & ~3;
        final int groups = groupedDims / 4;
        final int tailDims = dims - groupedDims;
        final byte[] out = new byte[groups * count * 4 + tailDims * count];

        long cursor = 0L;
        for (int g = 0; g < groups; g++) {
            final int dimBase = g * 4;
            for (int v = 0; v < count; v++) {
                byte[] src = vectors[offset + v].quantizedVector();
                out[(int) cursor++] = src[dimBase];
                out[(int) cursor++] = src[dimBase + 1];
                out[(int) cursor++] = src[dimBase + 2];
                out[(int) cursor++] = src[dimBase + 3];
            }
        }

        for (int v = 0; v < count; v++) {
            byte[] src = vectors[offset + v].quantizedVector();
            for (int d = 0; d < tailDims; d++) {
                out[(int) cursor++] = src[groupedDims + d];
            }
        }
        return out;
    }

    private static void writeBulkCorrections(VectorScorerTestUtils.OSQVectorData[] vectors, int offset, int count, IndexOutput out)
        throws IOException {
        for (int i = 0; i < count; i++) {
            out.writeInt(Float.floatToIntBits(vectors[offset + i].lowerInterval()));
        }
        for (int i = 0; i < count; i++) {
            out.writeInt(Float.floatToIntBits(vectors[offset + i].upperInterval()));
        }
        for (int i = 0; i < count; i++) {
            out.writeInt(vectors[offset + i].quantizedComponentSum());
        }
        for (int i = 0; i < count; i++) {
            out.writeInt(Float.floatToIntBits(vectors[offset + i].additionalCorrection()));
        }
    }

    private static ES92Int7VectorsScorer newVerticalScorer(IndexInput input, int dims, int bulkSize) {
        try {
            Class<?> scorerClass = Class.forName("org.elasticsearch.simdvec.internal.MemorySegmentES92VerticalInt7VectorsScorer");
            return (ES92Int7VectorsScorer) scorerClass.getConstructor(IndexInput.class, int.class, int.class)
                .newInstance(input, dims, bulkSize);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Unable to load vertical int7 scorer implementation", e);
        }
    }
}
