/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;

/**
 * Base class for bulk writers that write vectors to disk using the BBQ encoding.
 * This class provides the structure for writing vectors in bulk, with specific
 * implementations for different bit sizes strategies.
 */
abstract class DiskBBQBulkWriter {
    private static final Logger logger = LogManager.getLogger(DiskBBQBulkWriter.class);

    protected final int bulkSize;
    protected final IndexOutput out;

    protected DiskBBQBulkWriter(int bulkSize, IndexOutput out) {
        this.bulkSize = bulkSize;
        this.out = out;
    }

    abstract void writeVectors(DefaultIVFVectorsWriter.QuantizedVectorValues qvv) throws IOException;

    static class OneBitDiskBBQBulkWriter extends DiskBBQBulkWriter {
        private final OptimizedScalarQuantizer.QuantizationResult[] corrections;
        final byte[][] packed;

        public static byte[] averageBitwisePacked(byte[][] packedVectors) {
            int numVectors = packedVectors.length;
            int numBytes = packedVectors[0].length;
            int[] bitCounts = new int[numBytes * 8];

            for (byte[] packed : packedVectors) {
                for (int bit = 0; bit < bitCounts.length; bit++) {
                    int byteIdx = bit / 8;
                    int bitIdx = 7 - (bit % 8);
                    if (((packed[byteIdx] >> bitIdx) & 1) != 0) {
                        bitCounts[bit]++;
                    }
                }
            }

            byte[] result = new byte[numBytes];
            for (int bit = 0; bit < bitCounts.length; bit++) {
                int byteIdx = bit / 8;
                int bitIdx = 7 - (bit % 8);
                if (bitCounts[bit] >= numVectors / 2) {
                    result[byteIdx] |= (byte) (1 << bitIdx);
                }
            }
            return result;
        }

        OneBitDiskBBQBulkWriter(int bulkSize, IndexOutput out) {
            super(bulkSize, out);
            this.corrections = new OptimizedScalarQuantizer.QuantizationResult[bulkSize];
            this.packed = new byte[bulkSize][];
        }

        @Override
        void writeVectors(DefaultIVFVectorsWriter.QuantizedVectorValues qvv) throws IOException {
            int limit = qvv.count() - bulkSize + 1;
            int i = 0;
            // take the average of the corrections & the quantize vectors, note, the quantized vectors are in bits
            float lowerInterval = 0.0f;
            float upperInterval = 0.0f;
            int targetComponentSum = 0;
            float additionalCorrection = 0.0f;
            for (; i < limit; i += bulkSize) {
                lowerInterval = 0.0f;
                upperInterval = 0.0f;
                targetComponentSum = 0;
                additionalCorrection = 0.0f;
                for (int j = 0; j < bulkSize; j++) {
                    byte[] qv = qvv.next();
                    packed[j] = qv.clone();
                    corrections[j] = qvv.getCorrections();
                    lowerInterval += corrections[j].lowerInterval();
                    upperInterval += corrections[j].upperInterval();
                    additionalCorrection += corrections[j].additionalCorrection();
                }
                // average the packed vectors
                byte[] avged = averageBitwisePacked(packed);
                lowerInterval /= bulkSize;
                upperInterval /= bulkSize;
                additionalCorrection /= bulkSize;
                targetComponentSum = BQVectorUtils.popcount(avged);
                // write the average packed vector
                out.writeBytes(avged, avged.length);
                // write the average corrections
                out.writeInt(Float.floatToIntBits(lowerInterval));
                out.writeInt(Float.floatToIntBits(upperInterval));
                out.writeInt(Float.floatToIntBits(additionalCorrection));
                assert targetComponentSum >= 0 && targetComponentSum <= 0xffff;
                out.writeShort((short) targetComponentSum);
                for (var qv : packed) {
                    out.writeBytes(qv, qv.length);
                }
                writeCorrections(corrections);
            }
            // write tail
            for (; i < qvv.count(); ++i) {
                byte[] qv = qvv.next();
                OptimizedScalarQuantizer.QuantizationResult correction = qvv.getCorrections();
                out.writeBytes(qv, qv.length);
                writeCorrection(correction);
            }
        }

        private void writeCorrections(OptimizedScalarQuantizer.QuantizationResult[] corrections) throws IOException {
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

        private void writeCorrection(OptimizedScalarQuantizer.QuantizationResult correction) throws IOException {
            out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
            out.writeInt(Float.floatToIntBits(correction.upperInterval()));
            out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
            int targetComponentSum = correction.quantizedComponentSum();
            assert targetComponentSum >= 0 && targetComponentSum <= 0xffff;
            out.writeShort((short) targetComponentSum);
        }
    }

    static class SevenBitDiskBBQBulkWriter extends DiskBBQBulkWriter {
        private final OptimizedScalarQuantizer.QuantizationResult[] corrections;

        SevenBitDiskBBQBulkWriter(int bulkSize, IndexOutput out) {
            super(bulkSize, out);
            this.corrections = new OptimizedScalarQuantizer.QuantizationResult[bulkSize];
        }

        @Override
        void writeVectors(DefaultIVFVectorsWriter.QuantizedVectorValues qvv) throws IOException {
            int limit = qvv.count() - bulkSize + 1;
            int i = 0;
            for (; i < limit; i += bulkSize) {
                for (int j = 0; j < bulkSize; j++) {
                    byte[] qv = qvv.next();
                    corrections[j] = qvv.getCorrections();
                    out.writeBytes(qv, qv.length);
                }
                writeCorrections(corrections);
            }
            // write tail
            for (; i < qvv.count(); ++i) {
                byte[] qv = qvv.next();
                OptimizedScalarQuantizer.QuantizationResult correction = qvv.getCorrections();
                out.writeBytes(qv, qv.length);
                writeCorrection(correction);
            }
        }

        private void writeCorrections(OptimizedScalarQuantizer.QuantizationResult[] corrections) throws IOException {
            for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
                out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
            }
            for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
                out.writeInt(Float.floatToIntBits(correction.upperInterval()));
            }
            for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
                out.writeInt(correction.quantizedComponentSum());
            }
            for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
                out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
            }
        }

        private void writeCorrection(OptimizedScalarQuantizer.QuantizationResult correction) throws IOException {
            out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
            out.writeInt(Float.floatToIntBits(correction.upperInterval()));
            out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
            out.writeInt(correction.quantizedComponentSum());
        }
    }
}
