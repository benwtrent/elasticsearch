/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es94;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class ES94CompressedFlatVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
    }

    private final DenseVectorFieldMapper.ElementType elementType;
    private final boolean compressionEnabled;

    public ES94CompressedFlatVectorsFormatTests(DenseVectorFieldMapper.ElementType elementType, boolean compressionEnabled) {
        this.elementType = elementType;
        this.compressionEnabled = compressionEnabled;
    }

    @ParametersFactory
    public static Iterable<Object[]> elements() {
        return Stream.of(
            new Object[] { DenseVectorFieldMapper.ElementType.FLOAT, Boolean.TRUE },
            new Object[] { DenseVectorFieldMapper.ElementType.FLOAT, Boolean.FALSE }
        ).toList();
    }

    @Override
    protected VectorEncoding randomVectorEncoding() {
        return switch (elementType) {
            case FLOAT -> VectorEncoding.FLOAT32;
            case BYTE -> VectorEncoding.BYTE;
            default -> throw new IllegalArgumentException();
        };
    }

    @Override
    protected Codec getCodec() {
        return TestUtil.alwaysKnnVectorsFormat(new ES94CompressedFlatVectorsFormat(elementType, compressionEnabled));
    }

    public void testSearchWithVisitedLimit() {
        throw new AssumptionViolatedException("requires graph-based vector codec");
    }

    public void testSimpleOffHeapSizeIncludesCompressedPayloadWhenEnabled() throws IOException {
        int vectorLength = random().nextInt(32, 256);
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
            Document doc = new Document();
            if (elementType == DenseVectorFieldMapper.ElementType.FLOAT) {
                float[] vector = new float[vectorLength];
                for (int i = 0; i < vector.length; i++) {
                    vector[i] = (i % 8 == 0 ? 1f : 0f);
                }
                doc.add(new KnnFloatVectorField("f", vector, DOT_PRODUCT));
            } else {
                byte[] vector = new byte[vectorLength];
                for (int i = 0; i < vector.length; i++) {
                    vector[i] = (byte) (i % 7);
                }
                doc.add(new KnnByteVectorField("f", vector, DOT_PRODUCT));
            }
            w.addDocument(doc);
            w.commit();
            try (IndexReader reader = DirectoryReader.open(w)) {
                LeafReader r = getOnlyLeafReader(reader);
                if (r instanceof CodecReader codecReader) {
                    KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
                    if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                        knnVectorsReader = fieldsReader.getFieldReader("f");
                    }
                    var fieldInfo = r.getFieldInfos().fieldInfo("f");
                    var offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);
                    if (elementType == DenseVectorFieldMapper.ElementType.FLOAT && compressionEnabled) {
                        assertThat(offHeap.get(ES94CompressedFlatVectorsFormat.DATA_EXT), notNullValue());
                        assertThat(offHeap.get(ES94CompressedFlatVectorsFormat.DATA_EXT).intValue(), greaterThan(0));
                    } else {
                        assertThat(offHeap.keySet(), not(hasItem(ES94CompressedFlatVectorsFormat.DATA_EXT)));
                    }
                }
            }
        }
    }

    public void testJinaCompressionRoundTrip() throws IOException {
        int dimension = random().nextInt(8, 65);
        int vectorCount = random().nextInt(5, 31);
        List<float[]> vectors = randomUnitVectors(vectorCount, dimension);
        var payload = ES94JinaCompressionUtils.compress(vectors, dimension, true, ES94JinaCompressionUtils.CompressionMode.JINA);
        float[][] restored = ES94JinaCompressionUtils.decompress(payload, vectorCount, dimension);
        for (int i = 0; i < vectorCount; i++) {
            for (int j = 0; j < dimension; j++) {
                assertEquals(vectors.get(i)[j], restored[i][j], 1e-4f);
            }
        }
    }

    public void testZstdOnlyCompressionRoundTrip() throws IOException {
        int dimension = random().nextInt(8, 65);
        int vectorCount = random().nextInt(5, 31);
        List<float[]> vectors = randomUnitVectors(vectorCount, dimension);
        var payload = ES94JinaCompressionUtils.compress(vectors, dimension, true, ES94JinaCompressionUtils.CompressionMode.ZSTD_ONLY);
        float[][] restored = ES94JinaCompressionUtils.decompress(payload, vectorCount, dimension);
        for (int i = 0; i < vectorCount; i++) {
            for (int j = 0; j < dimension; j++) {
                assertEquals(vectors.get(i)[j], restored[i][j], 1e-6f);
            }
        }
    }

    private List<float[]> randomUnitVectors(int vectorCount, int dimension) {
        List<float[]> vectors = new ArrayList<>(vectorCount);
        for (int i = 0; i < vectorCount; i++) {
            float[] v = new float[dimension];
            double norm = 0d;
            for (int j = 0; j < dimension; j++) {
                v[j] = (random().nextFloat() * 2f) - 1f;
                norm += (double) v[j] * v[j];
            }
            norm = Math.sqrt(norm);
            if (norm == 0d) {
                v[0] = 1f;
            } else {
                for (int j = 0; j < dimension; j++) {
                    v[j] = (float) (v[j] / norm);
                }
            }
            vectors.add(v);
        }
        return vectors;
    }
}
