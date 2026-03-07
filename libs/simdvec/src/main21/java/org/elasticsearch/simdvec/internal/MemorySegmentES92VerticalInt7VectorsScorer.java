/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.nativeaccess.NativeAccess;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Experimental scorer for int7 vectors stored in a vertical layout.
 *
 * <p>The layout groups dimensions in chunks of 4 and stores vectors as:
 * [v0d0,v0d1,v0d2,v0d3, v1d0,...] for each group. If dimensions % 4 != 0, the
 * tail group is stored densely with its remaining dimensions per vector.
 */
public final class MemorySegmentES92VerticalInt7VectorsScorer extends MemorySegmentES92PanamaInt7VectorsScorer {

    private static final boolean NATIVE_SUPPORTED = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .flatMap(functions -> functions.getInt7uVerticalDotProductBulkHandle())
        .isPresent();

    public MemorySegmentES92VerticalInt7VectorsScorer(IndexInput in, int dimensions, int bulkSize) {
        super(in, dimensions, bulkSize);
    }

    @Override
    public boolean hasNativeAccess() {
        return NATIVE_SUPPORTED;
    }

    @Override
    public long int7DotProduct(byte[] q) throws IOException {
        throw new UnsupportedOperationException("single scoring is not supported for vertical int7 layout");
    }

    @Override
    public void int7DotProductBulk(byte[] q, int count, float[] scores) throws IOException {
        assert q.length == dimensions;
        final int bytesToRead = bytesForCount(dimensions, count);
        IndexInputUtils.withSlice(in, bytesToRead, this::getScratch, segment -> {
            if (NATIVE_SUPPORTED) {
                Similarities.dotProductI7uVerticalBulk(segment, MemorySegment.ofArray(q), dimensions, count, MemorySegment.ofArray(scores));
            } else {
                scalarVerticalDotProductBulk(q, segment, dimensions, count, scores);
            }
            return null;
        });
    }

    @Override
    public float score(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp
    ) throws IOException {
        throw new UnsupportedOperationException("single scoring is not supported for vertical int7 layout");
    }

    @Override
    public void scoreBulk(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores,
        int bulkSize
    ) throws IOException {
        int7DotProductBulk(q, bulkSize, scores);
        applyCorrectionsBulk(
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            similarityFunction,
            centroidDp,
            scores,
            bulkSize
        );
    }

    private static int bytesForCount(int dimensions, int count) {
        final int groupedDims = dimensions & ~3;
        final int groups = groupedDims / 4;
        final int tailDims = dimensions - groupedDims;
        return groups * count * 4 + tailDims * count;
    }

    private static void scalarVerticalDotProductBulk(byte[] query, MemorySegment dataset, int dimensions, int count, float[] scores) {
        for (int i = 0; i < count; i++) {
            scores[i] = 0f;
        }

        final int groupedDims = dimensions & ~3;
        final int groups = groupedDims / 4;
        long cursor = 0L;
        for (int g = 0; g < groups; g++) {
            final int q0 = query[g * 4] & 0xFF;
            final int q1 = query[g * 4 + 1] & 0xFF;
            final int q2 = query[g * 4 + 2] & 0xFF;
            final int q3 = query[g * 4 + 3] & 0xFF;
            for (int v = 0; v < count; v++) {
                final long base = cursor + (long) v * 4;
                final int partial = (dataset.get(ValueLayout.JAVA_BYTE, base) & 0xFF) * q0 + (dataset.get(ValueLayout.JAVA_BYTE, base + 1)
                    & 0xFF) * q1 + (dataset.get(ValueLayout.JAVA_BYTE, base + 2) & 0xFF) * q2 + (dataset.get(
                        ValueLayout.JAVA_BYTE,
                        base + 3
                    ) & 0xFF) * q3;
                scores[v] += partial;
            }
            cursor += (long) count * 4;
        }

        final int tailDims = dimensions - groupedDims;
        if (tailDims > 0) {
            for (int v = 0; v < count; v++) {
                final long base = cursor + (long) v * tailDims;
                int partial = 0;
                for (int d = 0; d < tailDims; d++) {
                    partial += (dataset.get(ValueLayout.JAVA_BYTE, base + d) & 0xFF) * (query[groupedDims + d] & 0xFF);
                }
                scores[v] += partial;
            }
        }
    }
}
