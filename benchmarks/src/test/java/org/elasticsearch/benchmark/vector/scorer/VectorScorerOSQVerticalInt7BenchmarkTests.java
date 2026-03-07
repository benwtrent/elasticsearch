/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector.scorer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.Constants;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;
import org.openjdk.jmh.annotations.Param;

import java.util.Arrays;
import java.util.Random;

public class VectorScorerOSQVerticalInt7BenchmarkTests extends ESTestCase {

    private static final int REPETITIONS = 5;
    private static final float DELTA_PERCENT = 0.1f;

    private final int dims;

    public VectorScorerOSQVerticalInt7BenchmarkTests(int dims) {
        this.dims = dims;
    }

    @BeforeClass
    public static void skipWindows() {
        assumeFalse("doesn't work on windows yet", Constants.WINDOWS);
    }

    public void testVerticalMatchesRowScalarAndVectorized() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {
            long seed = randomLong();
            var data = VectorScorerOSQVerticalInt7Benchmark.generateRandomVectorData(
                new Random(seed),
                dims,
                VectorScorerOSQVerticalInt7Benchmark.NUM_VECTORS
            );

            var rowScalar = new VectorScorerOSQVerticalInt7Benchmark();
            var rowVectorized = new VectorScorerOSQVerticalInt7Benchmark();
            var vertical = new VectorScorerOSQVerticalInt7Benchmark();
            try {
                rowScalar.dims = dims;
                rowScalar.implementation = VectorScorerOSQVerticalInt7Benchmark.Implementation.ROW_SCALAR;
                rowScalar.setup(data);
                float[] expectedScalar = rowScalar.bulkScore();

                rowVectorized.dims = dims;
                rowVectorized.implementation = VectorScorerOSQVerticalInt7Benchmark.Implementation.ROW_VECTORIZED;
                rowVectorized.setup(data);
                float[] expectedVectorized = rowVectorized.bulkScore();

                vertical.dims = dims;
                vertical.implementation = VectorScorerOSQVerticalInt7Benchmark.Implementation.VERTICAL_EXPERIMENTAL;
                vertical.setup(data);
                float[] actualVertical = vertical.bulkScore();

                assertArrayEqualsPercent("vertical VS row scalar", expectedScalar, actualVertical, DELTA_PERCENT, DEFAULT_DELTA);
                assertArrayEqualsPercent("vertical VS row vectorized", expectedVectorized, actualVertical, DELTA_PERCENT, DEFAULT_DELTA);
            } finally {
                rowScalar.teardown();
                rowVectorized.teardown();
                vertical.teardown();
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        try {
            String[] dimsValues = VectorScorerOSQVerticalInt7Benchmark.class.getField("dims").getAnnotationsByType(Param.class)[0].value();
            return () -> Arrays.stream(dimsValues).map(Integer::parseInt).map(v -> new Object[] { v }).iterator();
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }
}
