/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es94;

import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

final class ES94JinaCompressionUtils {

    static final byte MODE_NONE = 0;
    static final byte MODE_JZIP_ZSTD = 1;
    static final int DEFAULT_ZSTD_LEVEL = 1;

    private static final byte[] EMPTY = new byte[0];

    record CompressedPayload(byte mode, int originalBytes, byte[] payload) {}

    private ES94JinaCompressionUtils() {}

    static CompressedPayload compress(List<float[]> vectors, int dimension, boolean useZstd) throws IOException {
        if (vectors.isEmpty() || dimension < 2) {
            return new CompressedPayload(MODE_NONE, 0, EMPTY);
        }

        final int vectorCount = vectors.size();
        final int angleDims = dimension - 1;
        final int transformedFloats = Math.multiplyExact(vectorCount, angleDims);
        final int transformedBytes = Math.multiplyExact(transformedFloats, Float.BYTES);
        final int originalBytes = Math.multiplyExact(Math.multiplyExact(vectorCount, dimension), Float.BYTES);

        float[] spherical = new float[transformedFloats];
        int sphericalOffset = 0;
        for (float[] vector : vectors) {
            cartesianToSpherical(vector, spherical, sphericalOffset, dimension);
            sphericalOffset += angleDims;
        }

        float[] transposed = transpose(spherical, vectorCount, angleDims);
        byte[] transposedBytes = floatsToBytesLE(transposed);
        byte[] shuffled = byteShuffle(transposedBytes, transformedFloats);
        if (useZstd == false) {
            return new CompressedPayload(MODE_NONE, originalBytes, shuffled);
        }

        Zstd zstd = NativeAccess.instance().getZstd();
        if (zstd == null) {
            return new CompressedPayload(MODE_NONE, originalBytes, shuffled);
        }

        return new CompressedPayload(MODE_JZIP_ZSTD, originalBytes, zstdCompress(shuffled, zstd, DEFAULT_ZSTD_LEVEL));
    }

    static float[][] decompress(CompressedPayload payload, int vectorCount, int dimension) throws IOException {
        if (vectorCount == 0) {
            return new float[0][dimension];
        }
        final int angleDims = dimension - 1;
        final int transformedFloats = Math.multiplyExact(vectorCount, angleDims);
        final int transformedBytes = Math.multiplyExact(transformedFloats, Float.BYTES);
        byte[] shuffled = switch (payload.mode()) {
            case MODE_NONE -> payload.payload();
            case MODE_JZIP_ZSTD -> {
                Zstd zstd = NativeAccess.instance().getZstd();
                if (zstd == null) {
                    throw new IOException("zstd is not available for decompression");
                }
                yield zstdDecompress(payload.payload(), transformedBytes, zstd);
            }
            default -> throw new IOException("Unknown compression mode [" + payload.mode() + "]");
        };

        byte[] unshuffled = byteUnshuffle(shuffled, transformedFloats);
        float[] transposed = bytesToFloatsLE(unshuffled);
        float[] spherical = transpose(transposed, angleDims, vectorCount);
        float[][] vectors = new float[vectorCount][dimension];
        for (int i = 0; i < vectorCount; i++) {
            sphericalToCartesian(spherical, i * angleDims, vectors[i], dimension);
        }
        return vectors;
    }

    private static void cartesianToSpherical(float[] input, float[] output, int outputOffset, int dimension) {
        double[] r2 = new double[dimension];
        int last = dimension - 1;
        r2[last] = (double) input[last] * input[last];
        for (int i = last - 1; i >= 0; i--) {
            double v = input[i];
            r2[i] = r2[i + 1] + v * v;
        }
        for (int i = 0; i < dimension - 2; i++) {
            double r = Math.sqrt(r2[i]);
            double value = r == 0d ? 1d : input[i] / r;
            value = Math.max(-1d, Math.min(1d, value));
            output[outputOffset + i] = (float) Math.acos(value);
        }
        output[outputOffset + dimension - 2] = (float) Math.atan2(input[dimension - 1], input[dimension - 2]);
    }

    private static void sphericalToCartesian(float[] spherical, int sphericalOffset, float[] output, int dimension) {
        double scale = 1d;
        for (int i = 0; i < dimension - 2; i++) {
            double angle = spherical[sphericalOffset + i];
            output[i] = (float) (scale * Math.cos(angle));
            scale *= Math.sin(angle);
        }
        double lastAngle = spherical[sphericalOffset + dimension - 2];
        output[dimension - 2] = (float) (scale * Math.cos(lastAngle));
        output[dimension - 1] = (float) (scale * Math.sin(lastAngle));
    }

    private static float[] transpose(float[] src, int rows, int cols) {
        float[] dst = new float[src.length];
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                dst[col * rows + row] = src[row * cols + col];
            }
        }
        return dst;
    }

    private static byte[] byteShuffle(byte[] src, int floatCount) {
        byte[] dst = new byte[src.length];
        int b0 = 0;
        int b1 = floatCount;
        int b2 = floatCount * 2;
        int b3 = floatCount * 3;
        for (int i = 0; i < floatCount; i++) {
            int offset = i * 4;
            dst[b0 + i] = src[offset];
            dst[b1 + i] = src[offset + 1];
            dst[b2 + i] = src[offset + 2];
            dst[b3 + i] = src[offset + 3];
        }
        return dst;
    }

    private static byte[] byteUnshuffle(byte[] src, int floatCount) {
        byte[] dst = new byte[src.length];
        int b0 = 0;
        int b1 = floatCount;
        int b2 = floatCount * 2;
        int b3 = floatCount * 3;
        for (int i = 0; i < floatCount; i++) {
            int offset = i * 4;
            dst[offset] = src[b0 + i];
            dst[offset + 1] = src[b1 + i];
            dst[offset + 2] = src[b2 + i];
            dst[offset + 3] = src[b3 + i];
        }
        return dst;
    }

    private static byte[] zstdCompress(byte[] input, Zstd zstd, int level) throws IOException {
        int bound = zstd.compressBound(input.length);
        try (CloseableByteBuffer src = NativeAccess.instance().newConfinedBuffer(input.length);
            CloseableByteBuffer dst = NativeAccess.instance().newConfinedBuffer(bound)) {
            src.buffer().put(input);
            src.buffer().flip();
            int compressedLength = zstd.compress(dst, src, level);
            byte[] output = new byte[compressedLength];
            dst.buffer().get(output, 0, compressedLength);
            return output;
        }
    }

    private static byte[] zstdDecompress(byte[] input, int decompressedLength, Zstd zstd) throws IOException {
        try (CloseableByteBuffer src = NativeAccess.instance().newConfinedBuffer(input.length);
            CloseableByteBuffer dst = NativeAccess.instance().newConfinedBuffer(decompressedLength)) {
            src.buffer().put(input);
            src.buffer().flip();
            int actualLength = zstd.decompress(dst, src);
            if (actualLength != decompressedLength) {
                throw new IOException("Expected " + decompressedLength + " decompressed bytes but got " + actualLength);
            }
            byte[] output = new byte[actualLength];
            dst.buffer().get(output, 0, actualLength);
            return output;
        }
    }

    private static byte[] floatsToBytesLE(float[] values) {
        ByteBuffer buffer = ByteBuffer.allocate(values.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (float value : values) {
            buffer.putFloat(value);
        }
        return buffer.array();
    }

    private static float[] bytesToFloatsLE(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        float[] values = new float[bytes.length / Float.BYTES];
        for (int i = 0; i < values.length; i++) {
            values[i] = buffer.getFloat();
        }
        return values;
    }
}
