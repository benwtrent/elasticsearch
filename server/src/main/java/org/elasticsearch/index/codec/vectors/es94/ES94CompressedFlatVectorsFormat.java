/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es94;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.elasticsearch.index.codec.vectors.es93.DirectIOCapableLucene99FlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93FlatVectorScorer;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.codec.vectors.VectorScoringUtils.scoreAndCollectAll;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

public class ES94CompressedFlatVectorsFormat extends KnnVectorsFormat {
    public static final String NAME = "ES94CompressedFlatVectorsFormat";
    static final String META_EXT = "es94vcm";
    static final String DATA_EXT = "es94vcd";
    static final String META_CODEC = "ES94CompressedFlatVectorsMeta";
    static final String DATA_CODEC = "ES94CompressedFlatVectorsData";
    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;

    private final DirectIOCapableLucene99FlatVectorsFormat rawFormat;
    private final boolean compressionEnabled;

    public ES94CompressedFlatVectorsFormat() {
        this(DenseVectorFieldMapper.ElementType.FLOAT, true);
    }

    public ES94CompressedFlatVectorsFormat(DenseVectorFieldMapper.ElementType elementType, boolean compressionEnabled) {
        super(NAME);
        this.rawFormat = switch (elementType) {
            case FLOAT, BYTE, BFLOAT16, BIT -> new DirectIOCapableLucene99FlatVectorsFormat(ES93FlatVectorScorer.INSTANCE);
        };
        this.compressionEnabled = compressionEnabled;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES94CompressedFlatVectorsWriter(state, rawFormat.fieldsWriter(state), compressionEnabled);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Reader(new ES94CompressedFlatVectorsReader(state, rawFormat.fieldsReader(state)));
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMS_COUNT;
    }

    private static class Reader extends KnnVectorsReader {
        private final FlatVectorsReader delegate;

        Reader(FlatVectorsReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public void checkIntegrity() throws IOException {
            delegate.checkIntegrity();
        }

        @Override
        public FloatVectorValues getFloatVectorValues(String field) throws IOException {
            return delegate.getFloatVectorValues(field);
        }

        @Override
        public ByteVectorValues getByteVectorValues(String field) throws IOException {
            return delegate.getByteVectorValues(field);
        }

        @Override
        public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
            scoreAndCollectAll(knnCollector, acceptDocs, delegate.getRandomVectorScorer(field, target));
        }

        @Override
        public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
            scoreAndCollectAll(knnCollector, acceptDocs, delegate.getRandomVectorScorer(field, target));
        }

        @Override
        public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
            return delegate.getOffHeapByteSize(fieldInfo);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
