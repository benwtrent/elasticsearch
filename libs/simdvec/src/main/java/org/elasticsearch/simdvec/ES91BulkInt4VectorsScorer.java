/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

import java.io.IOException;

/** Scorer for quantized vectors stored as an {@link IndexInput}.
 * <p>
 * Similar to {@link org.apache.lucene.util.VectorUtil#int4DotProduct(byte[], byte[])} but
 * one value is read directly from an {@link IndexInput}.
 *
 * */
public class ES91BulkInt4VectorsScorer {

    /** The wrapper {@link IndexInput}. */
    protected final IndexInput in;
    protected final int dimensions;
    protected byte[] scratch4, scratch3, scratch2, scratch1;
    protected final byte[] scratch; // 16 * 4 bytes for int4 vectors
    protected final int bulkSize = 16;
    protected final int[] scores;
    protected final int bitsize;

    /** Sole constructor, called by sub-classes. */
    public ES91BulkInt4VectorsScorer(IndexInput in, int dimensions) {
        this.in = in;
        this.dimensions = dimensions;
        this.bitsize = OptimizedScalarQuantizer.discretize(dimensions, 64) / 8;
        this.scratch4 = new byte[bitsize * bulkSize];
        this.scratch3 = new byte[bitsize * bulkSize];
        this.scratch2 = new byte[bitsize * bulkSize];
        this.scratch1 = new byte[bitsize * bulkSize];
        this.scratch = new byte[bitsize * 4];
        this.scores = new int[bulkSize];
    }

    public void scores(int[] scores, byte[] query) throws IOException {
        if (scores.length != bulkSize) {
            throw new IllegalArgumentException("scores array must be of size " + bulkSize);
        }
        in.readBytes(scratch4, 0, bitsize * bulkSize);
        in.readBytes(scratch3, 0, bitsize * bulkSize);
        in.readBytes(scratch2, 0, bitsize * bulkSize);
        in.readBytes(scratch1, 0, bitsize * bulkSize);
        for (int i = 0; i < bulkSize; i++) {
            int score = (int) ESVectorUtil.ipByteBinByte(query, scratch4, bitsize, i * bitsize);
            scores[i] += (score << 3);
        }
        for (int i = 0; i < bulkSize; i++) {
            int score = (int) ESVectorUtil.ipByteBinByte(query, scratch3, bitsize, i * bitsize);
            scores[i] += (score << 2);
        }
        for (int i = 0; i < bulkSize; i++) {
            int score = (int) ESVectorUtil.ipByteBinByte(query, scratch2, bitsize, i * bitsize);
            scores[i] += (score << 1);
        }
        for (int i = 0; i < bulkSize; i++) {
            int score = (int) ESVectorUtil.ipByteBinByte(query, scratch1, bitsize, i * bitsize);
            scores[i] += score;

        }
    }
}
