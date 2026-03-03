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
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

final class ES94JinaCompressionUtils {

    enum CompressionMode {
        JINA,
        ZSTD_ONLY
    }

    static final byte MODE_JINA_NO_ZSTD = 0;
    static final byte MODE_JINA_ZSTD = 1;
    static final byte MODE_RAW_NO_ZSTD = 2;
    static final byte MODE_RAW_ZSTD = 3;
    static final int DEFAULT_ZSTD_LEVEL = 1;

    private static final byte[] EMPTY = new byte[0];

    record CompressedPayload(byte mode, int originalBytes, byte[] payload) {}

    private ES94JinaCompressionUtils() {}

    static CompressedPayload compress(List<float[]> vectors, int dimension, boolean useZstd, CompressionMode compressionMode)
        throws IOException {
        if (vectors.isEmpty() || dimension < 2) {
            return new CompressedPayload(MODE_RAW_NO_ZSTD, 0, EMPTY);
        }
        return switch (compressionMode) {
            case JINA -> compressWithJina(vectors, dimension, useZstd);
            case ZSTD_ONLY -> compressRaw(vectors, dimension, useZstd);
        };
    }

    private static CompressedPayload compressWithJina(List<float[]> vectors, int dimension, boolean useZstd) throws IOException {
        final int vectorCount = vectors.size();
        final int angleDims = dimension - 1;
        final int transformedFloats = Math.multiplyExact(vectorCount, angleDims);
        final int originalBytes = Math.multiplyExact(Math.multiplyExact(vectorCount, dimension), Float.BYTES);

        float[] spherical = new float[transformedFloats];
        int sphericalOffset = 0;
        for (float[] vector : vectors) {
            ESVectorUtil.jinaCartesianToSpherical(vector, spherical, sphericalOffset, dimension);
            sphericalOffset += angleDims;
        }

        float[] transposed = new float[spherical.length];
        ESVectorUtil.jinaTranspose(spherical, vectorCount, angleDims, transposed);
        byte[] transposedBytes = floatsToBytesLE(transposed);
        byte[] shuffled = byteShuffle(transposedBytes, transformedFloats);
        if (useZstd == false) {
            return new CompressedPayload(MODE_JINA_NO_ZSTD, originalBytes, shuffled);
        }

        Zstd zstd = NativeAccess.instance().getZstd();
        if (zstd == null) {
            return new CompressedPayload(MODE_JINA_NO_ZSTD, originalBytes, shuffled);
        }

        return new CompressedPayload(MODE_JINA_ZSTD, originalBytes, zstdCompress(shuffled, zstd, DEFAULT_ZSTD_LEVEL));
    }

    private static CompressedPayload compressRaw(List<float[]> vectors, int dimension, boolean useZstd) throws IOException {
        final int vectorCount = vectors.size();
        final int originalBytes = Math.multiplyExact(Math.multiplyExact(vectorCount, dimension), Float.BYTES);
        byte[] raw = floatsToBytesLE(vectors, dimension);
        if (useZstd == false) {
            return new CompressedPayload(MODE_RAW_NO_ZSTD, originalBytes, raw);
        }
        Zstd zstd = NativeAccess.instance().getZstd();
        if (zstd == null) {
            return new CompressedPayload(MODE_RAW_NO_ZSTD, originalBytes, raw);
        }
        return new CompressedPayload(MODE_RAW_ZSTD, originalBytes, zstdCompress(raw, zstd, DEFAULT_ZSTD_LEVEL));
    }

    static float[][] decompress(CompressedPayload payload, int vectorCount, int dimension) throws IOException {
        if (vectorCount == 0) {
            return new float[0][dimension];
        }
        return switch (payload.mode()) {
            case MODE_JINA_NO_ZSTD, MODE_JINA_ZSTD -> decompressJina(payload, vectorCount, dimension);
            case MODE_RAW_NO_ZSTD, MODE_RAW_ZSTD -> decompressRaw(payload, vectorCount, dimension);
            default -> throw new IOException("Unknown compression mode [" + payload.mode() + "]");
        };
    }

    private static float[][] decompressJina(CompressedPayload payload, int vectorCount, int dimension) throws IOException {
        final int angleDims = dimension - 1;
        final int transformedFloats = Math.multiplyExact(vectorCount, angleDims);
        final int transformedBytes = Math.multiplyExact(transformedFloats, Float.BYTES);
        byte[] shuffled = switch (payload.mode()) {
            case MODE_JINA_NO_ZSTD -> payload.payload();
            case MODE_JINA_ZSTD -> {
                Zstd zstd = NativeAccess.instance().getZstd();
                if (zstd == null) {
                    throw new IOException("zstd is not available for decompression");
                }
                yield zstdDecompress(payload.payload(), transformedBytes, zstd);
            }
            default -> throw new IOException("Unsupported jina payload mode [" + payload.mode() + "]");
        };

        byte[] unshuffled = byteUnshuffle(shuffled, transformedFloats);
        float[] transposed = bytesToFloatsLE(unshuffled);
        float[] spherical = new float[transposed.length];
        ESVectorUtil.jinaTranspose(transposed, angleDims, vectorCount, spherical);
        float[][] vectors = new float[vectorCount][dimension];
        for (int i = 0; i < vectorCount; i++) {
            ESVectorUtil.jinaSphericalToCartesian(spherical, i * angleDims, vectors[i], dimension);
        }
        return vectors;
    }

    private static float[][] decompressRaw(CompressedPayload payload, int vectorCount, int dimension) throws IOException {
        final int rawBytes = Math.multiplyExact(Math.multiplyExact(vectorCount, dimension), Float.BYTES);
        byte[] raw = switch (payload.mode()) {
            case MODE_RAW_NO_ZSTD -> payload.payload();
            case MODE_RAW_ZSTD -> {
                Zstd zstd = NativeAccess.instance().getZstd();
                if (zstd == null) {
                    throw new IOException("zstd is not available for decompression");
                }
                yield zstdDecompress(payload.payload(), rawBytes, zstd);
            }
            default -> throw new IOException("Unsupported raw payload mode [" + payload.mode() + "]");
        };
        float[] flattened = bytesToFloatsLE(raw);
        float[][] vectors = new float[vectorCount][dimension];
        int offset = 0;
        for (int i = 0; i < vectorCount; i++) {
            System.arraycopy(flattened, offset, vectors[i], 0, dimension);
            offset += dimension;
        }
        return vectors;
    }

    private static byte[] zstdCompress(byte[] input, Zstd zstd, int level) throws IOException {
        int bound = zstd.compressBound(input.length);
        try (
            CloseableByteBuffer src = NativeAccess.instance().newConfinedBuffer(input.length);
            CloseableByteBuffer dst = NativeAccess.instance().newConfinedBuffer(bound)
        ) {
            src.buffer().put(input);
            src.buffer().flip();
            int compressedLength = zstd.compress(dst, src, level);
            byte[] output = new byte[compressedLength];
            dst.buffer().get(output, 0, compressedLength);
            return output;
        }
    }

    private static byte[] zstdDecompress(byte[] input, int decompressedLength, Zstd zstd) throws IOException {
        try (
            CloseableByteBuffer src = NativeAccess.instance().newConfinedBuffer(input.length);
            CloseableByteBuffer dst = NativeAccess.instance().newConfinedBuffer(decompressedLength)
        ) {
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

    private static byte[] floatsToBytesLE(List<float[]> vectors, int dimension) {
        float[] flattened = new float[vectors.size() * dimension];
        int offset = 0;
        for (float[] vector : vectors) {
            System.arraycopy(vector, 0, flattened, offset, dimension);
            offset += dimension;
        }
        return floatsToBytesLE(flattened);
    }

    private static float[] bytesToFloatsLE(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        float[] values = new float[bytes.length / Float.BYTES];
        for (int i = 0; i < values.length; i++) {
            values[i] = buffer.getFloat();
        }
        return values;
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
}
