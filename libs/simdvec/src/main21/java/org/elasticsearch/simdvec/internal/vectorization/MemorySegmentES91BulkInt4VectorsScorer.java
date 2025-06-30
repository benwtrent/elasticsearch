/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal.vectorization;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.elasticsearch.simdvec.ES91BulkInt4VectorsScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

/** Panamized scorer for quantized vectors stored as an {@link IndexInput}.
 * <p>
 * Similar to {@link org.apache.lucene.util.VectorUtil#int4DotProduct(byte[], byte[])} but
 *  one value is read directly from a {@link MemorySegment}.
 * */
public final class MemorySegmentES91BulkInt4VectorsScorer extends ES91BulkInt4VectorsScorer {

    private static final VectorSpecies<Byte> BYTE_SPECIES_64 = ByteVector.SPECIES_64;
    private static final VectorSpecies<Byte> BYTE_SPECIES_128 = ByteVector.SPECIES_128;

    private static final VectorSpecies<Short> SHORT_SPECIES_128 = ShortVector.SPECIES_128;
    private static final VectorSpecies<Short> SHORT_SPECIES_256 = ShortVector.SPECIES_256;

    private static final VectorSpecies<Integer> INT_SPECIES_128 = IntVector.SPECIES_128;
    private static final VectorSpecies<Integer> INT_SPECIES_256 = IntVector.SPECIES_256;
    private static final VectorSpecies<Integer> INT_SPECIES_512 = IntVector.SPECIES_512;

    private final MemorySegment memorySegment;
    private final int length;

    public MemorySegmentES91BulkInt4VectorsScorer(IndexInput in, int dimensions, MemorySegment memorySegment) {
        super(in, dimensions);
        System.out.println("Using MemorySegmentES91BulkInt4VectorsScorer with dimensions: " + dimensions);
        this.memorySegment = memorySegment;
        this.length = OptimizedScalarQuantizer.discretize(dimensions, 64) / 8;
    }

    @Override
    public void scores(int[] scores, byte[] query) throws IOException {
        ipByteBin128(query, 0, scores);
        ipByteBin128(query, 1, scores);
        ipByteBin128(query, 2, scores);
        ipByteBin128(query, 3, scores);
    }

    public void ipByteBin128(byte[] q, int shift, int[] scores) throws IOException {
        for (int i = 0; i < scores.length; i += 4) {
            scores[i] += (quantizeScore128(q) << shift);
            scores[i + 1] += (quantizeScore128(q) << shift);
            scores[i + 2] += (quantizeScore128(q) << shift);
            scores[i + 3] += (quantizeScore128(q) << shift);
        }
    }

    private long quantizeScore128(byte[] q) throws IOException {
        long subRet0 = 0;
        long subRet1 = 0;
        long subRet2 = 0;
        long subRet3 = 0;
        int i = 0;
        long offset = in.getFilePointer();

        var sum0 = IntVector.zero(INT_SPECIES_128);
        var sum1 = IntVector.zero(INT_SPECIES_128);
        var sum2 = IntVector.zero(INT_SPECIES_128);
        var sum3 = IntVector.zero(INT_SPECIES_128);
        int limit = ByteVector.SPECIES_128.loopBound(length);
        for (; i < limit; i += ByteVector.SPECIES_128.length(), offset += INT_SPECIES_128.vectorByteSize()) {
            var vd = IntVector.fromMemorySegment(INT_SPECIES_128, memorySegment, offset, ByteOrder.LITTLE_ENDIAN);
            var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsInts();
            var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length).reinterpretAsInts();
            var vq2 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length * 2).reinterpretAsInts();
            var vq3 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length * 3).reinterpretAsInts();
            sum0 = sum0.add(vd.and(vq0).lanewise(VectorOperators.BIT_COUNT));
            sum1 = sum1.add(vd.and(vq1).lanewise(VectorOperators.BIT_COUNT));
            sum2 = sum2.add(vd.and(vq2).lanewise(VectorOperators.BIT_COUNT));
            sum3 = sum3.add(vd.and(vq3).lanewise(VectorOperators.BIT_COUNT));
        }
        subRet0 += sum0.reduceLanes(VectorOperators.ADD);
        subRet1 += sum1.reduceLanes(VectorOperators.ADD);
        subRet2 += sum2.reduceLanes(VectorOperators.ADD);
        subRet3 += sum3.reduceLanes(VectorOperators.ADD);
        // tail as bytes
        in.seek(offset);
        for (; i < length; i++) {
            int dValue = in.readByte() & 0xFF;
            subRet0 += Integer.bitCount((dValue & q[i]) & 0xFF);
            subRet1 += Integer.bitCount((dValue & q[i + length]) & 0xFF);
            subRet2 += Integer.bitCount((dValue & q[i + 2 * length]) & 0xFF);
            subRet3 += Integer.bitCount((dValue & q[i + 3 * length]) & 0xFF);
        }
        return (int) (subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3));
    }

}
