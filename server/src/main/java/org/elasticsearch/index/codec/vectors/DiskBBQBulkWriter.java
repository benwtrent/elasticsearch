/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.hnsw.IntToIntFunction;

import java.io.IOException;

import static org.elasticsearch.index.codec.vectors.BQSpaceUtils.transposeHalfByte;
import static org.elasticsearch.index.codec.vectors.BQVectorUtils.discretize;
import static org.elasticsearch.index.codec.vectors.BQVectorUtils.packAsBinary;

/**
 * Base class for bulk writers that write vectors to disk using the BBQ encoding.
 * This class provides the structure for writing vectors in bulk, with specific
 * implementations for different bit sizes strategies.
 */
public abstract class DiskBBQBulkWriter {
    protected final int bulkSize;
    protected final OptimizedScalarQuantizer quantizer;
    protected final IndexOutput out;
    protected final FloatVectorValues fvv;

    protected DiskBBQBulkWriter(int bulkSize, OptimizedScalarQuantizer quantizer, FloatVectorValues fvv, IndexOutput out) {
        this.bulkSize = bulkSize;
        this.quantizer = quantizer;
        this.out = out;
        this.fvv = fvv;
    }

    public abstract void writeOrds(IntToIntFunction ords, int count, float[] centroid) throws IOException;

    private static void writeCorrections(OptimizedScalarQuantizer.QuantizationResult[] corrections, IndexOutput out) throws IOException {
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.upperInterval()));
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            int targetComponentSum = correction.quantizedComponentSum();
            assert targetComponentSum >= 0 && targetComponentSum <= 0xffff;
            out.writeShort((short) targetComponentSum);
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
        }
    }

    private static void writeCorrection(OptimizedScalarQuantizer.QuantizationResult correction, IndexOutput out) throws IOException {
        out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
        out.writeInt(Float.floatToIntBits(correction.upperInterval()));
        int targetComponentSum = correction.quantizedComponentSum();
        assert targetComponentSum >= 0 && targetComponentSum <= 0xffff;
        out.writeShort((short) targetComponentSum);
        out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
    }

    public static class OneBitDiskBBQBulkWriter extends DiskBBQBulkWriter {
        private final byte[] binarized;
        private final byte[] initQuantized;
        private final OptimizedScalarQuantizer.QuantizationResult[] corrections;

        public OneBitDiskBBQBulkWriter(int bulkSize, OptimizedScalarQuantizer quantizer, FloatVectorValues fvv, IndexOutput out) {
            super(bulkSize, quantizer, fvv, out);
            this.binarized = new byte[discretize(fvv.dimension(), 64) / 8];
            this.initQuantized = new byte[fvv.dimension()];
            this.corrections = new OptimizedScalarQuantizer.QuantizationResult[bulkSize];
        }

        @Override
        public void writeOrds(IntToIntFunction ords, int count, float[] centroid) throws IOException {
            int limit = count - bulkSize + 1;
            int i = 0;
            for (; i < limit; i += bulkSize) {
                for (int j = 0; j < bulkSize; j++) {
                    int ord = ords.apply(i + j);
                    float[] fv = fvv.vectorValue(ord);
                    corrections[j] = quantizer.scalarQuantize(fv, initQuantized, (byte) 1, centroid);
                    packAsBinary(initQuantized, binarized);
                    out.writeBytes(binarized, binarized.length);
                }
                writeCorrections(corrections, out);
            }
            // write tail
            for (; i < count; ++i) {
                int ord = ords.apply(i);
                float[] fv = fvv.vectorValue(ord);
                OptimizedScalarQuantizer.QuantizationResult correction = quantizer.scalarQuantize(fv, initQuantized, (byte) 1, centroid);
                packAsBinary(initQuantized, binarized);
                out.writeBytes(binarized, binarized.length);
                writeCorrection(correction, out);
            }
        }
    }

    public static class FourBitDiskBBQBulkWriter extends DiskBBQBulkWriter {
        private final byte[] first, second, third, fourth;
        private final byte[] initQuantized, packed;
        private final int binarySize;
        private final OptimizedScalarQuantizer.QuantizationResult[] corrections;

        public static void transposeHalfByteBulk(
            byte[] q,
            int offset,
            byte[] fourthBit,
            byte[] thirdBit,
            byte[] secondBit,
            byte[] firstBit
        ) {
            for (int i = 0; i < q.length;) {
                assert q[i] >= 0 && q[i] <= 15;
                int lowerByte = 0;
                int lowerMiddleByte = 0;
                int upperMiddleByte = 0;
                int upperByte = 0;
                for (int j = 7; j >= 0 && i < q.length; j--) {
                    lowerByte |= (q[i] & 1) << j;
                    lowerMiddleByte |= ((q[i] >> 1) & 1) << j;
                    upperMiddleByte |= ((q[i] >> 2) & 1) << j;
                    upperByte |= ((q[i] >> 3) & 1) << j;
                    i++;
                }
                int index = ((i + 7) / 8) - 1;
                firstBit[index + offset] = (byte) lowerByte;
                secondBit[index + offset] = (byte) lowerMiddleByte;
                thirdBit[index + offset] = (byte) upperMiddleByte;
                fourthBit[index + offset] = (byte) upperByte;
            }
        }

        public FourBitDiskBBQBulkWriter(int bulkSize, OptimizedScalarQuantizer quantizer, FloatVectorValues fvv, IndexOutput out) {
            super(bulkSize, quantizer, fvv, out);
            this.binarySize = discretize(fvv.dimension(), 64) / 8;
            int bulkSizeInBytes = binarySize * bulkSize;
            this.packed = new byte[discretize(fvv.dimension(), 64) / 2];
            this.first = new byte[bulkSizeInBytes];
            this.second = new byte[bulkSizeInBytes];
            this.third = new byte[bulkSizeInBytes];
            this.fourth = new byte[bulkSizeInBytes];
            this.initQuantized = new byte[fvv.dimension()];
            this.corrections = new OptimizedScalarQuantizer.QuantizationResult[bulkSize];
        }

        @Override
        public void writeOrds(IntToIntFunction ords, int count, float[] centroid) throws IOException {
            int limit = count - bulkSize + 1;
            int i = 0;
            for (; i < limit; i += bulkSize) {
                for (int j = 0; j < bulkSize; j++) {
                    int ord = ords.apply(i + j);
                    float[] fv = fvv.vectorValue(ord);
                    corrections[j] = quantizer.scalarQuantize(fv, initQuantized, (byte) 4, centroid);
                    transposeHalfByteBulk(initQuantized, j * binarySize, fourth, third, second, first);
                }
                out.writeBytes(fourth, binarySize);
                out.writeBytes(third, binarySize);
                out.writeBytes(second, binarySize);
                out.writeBytes(first, binarySize);
                writeCorrections(corrections, out);
            }
            // write tail
            for (; i < count; ++i) {
                int ord = ords.apply(i);
                float[] fv = fvv.vectorValue(ord);
                OptimizedScalarQuantizer.QuantizationResult correction = quantizer.scalarQuantize(fv, initQuantized, (byte) 4, centroid);
                transposeHalfByte(initQuantized, packed);
                out.writeBytes(packed, packed.length);
                writeCorrection(correction, out);
            }
        }
    }
}
