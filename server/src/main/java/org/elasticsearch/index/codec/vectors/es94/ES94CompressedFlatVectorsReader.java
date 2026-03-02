/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es94;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class ES94CompressedFlatVectorsReader extends FlatVectorsReader {
    private final FlatVectorsReader delegate;
    private final IndexInput dataIn;
    private final Map<String, FieldEntry> fields;

    @SuppressWarnings("this-escape")
    ES94CompressedFlatVectorsReader(SegmentReadState state, FlatVectorsReader delegate) throws IOException {
        super(delegate.getFlatVectorScorer());
        this.delegate = delegate;
        this.fields = new HashMap<>();

        String metaFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ES94CompressedFlatVectorsFormat.META_EXT);
        String dataFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ES94CompressedFlatVectorsFormat.DATA_EXT);
        IndexInput localDataIn = null;
        try (ChecksumIndexInput metaIn = state.directory.openChecksumInput(metaFile)) {
            Throwable prior = null;
            try {
                CodecUtil.checkIndexHeader(
                    metaIn,
                    ES94CompressedFlatVectorsFormat.META_CODEC,
                    ES94CompressedFlatVectorsFormat.VERSION_START,
                    ES94CompressedFlatVectorsFormat.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                readFields(metaIn, state.fieldInfos);
            } catch (Throwable t) {
                prior = t;
            } finally {
                CodecUtil.checkFooter(metaIn, prior);
            }
            localDataIn = state.directory.openInput(dataFile, state.context);
            CodecUtil.checkIndexHeader(
                localDataIn,
                ES94CompressedFlatVectorsFormat.DATA_CODEC,
                ES94CompressedFlatVectorsFormat.VERSION_START,
                ES94CompressedFlatVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            CodecUtil.retrieveChecksum(localDataIn);
            this.dataIn = localDataIn;
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(delegate, localDataIn);
            throw t;
        }
    }

    private ES94CompressedFlatVectorsReader(FlatVectorsReader delegate, IndexInput dataIn, Map<String, FieldEntry> fields) {
        super(delegate.getFlatVectorScorer());
        this.delegate = delegate;
        this.dataIn = dataIn;
        this.fields = fields;
    }

    private void readFields(ChecksumIndexInput meta, FieldInfos fieldInfos) throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            FieldInfo info = fieldInfos.fieldInfo(fieldNumber);
            if (info == null) {
                throw new CorruptIndexException("Invalid field number [" + fieldNumber + "]", meta);
            }
            int vectorCount = meta.readInt();
            int dimension = meta.readInt();
            byte mode = meta.readByte();
            int originalBytes = meta.readVInt();
            long payloadOffset = meta.readVLong();
            long payloadLength = meta.readVLong();
            fields.put(info.name, new FieldEntry(vectorCount, dimension, mode, originalBytes, payloadOffset, payloadLength));
        }
    }

    @Override
    public FlatVectorsReader getMergeInstance() throws IOException {
        return new ES94CompressedFlatVectorsReader(delegate.getMergeInstance(), dataIn.clone(), fields);
    }

    @Override
    public FlatVectorsScorer getFlatVectorScorer() {
        return delegate.getFlatVectorScorer();
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
    public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
        return delegate.getRandomVectorScorer(field, target);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
        return delegate.getRandomVectorScorer(field, target);
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        delegate.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        delegate.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
        CodecUtil.checksumEntireFile(dataIn);
    }

    @Override
    public long ramBytesUsed() {
        return delegate.ramBytesUsed();
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        Map<String, Long> base = delegate.getOffHeapByteSize(fieldInfo);
        FieldEntry entry = fields.get(fieldInfo.name);
        if (entry == null || entry.payloadLength == 0) {
            return base;
        }
        return org.apache.lucene.codecs.KnnVectorsReader.mergeOffHeapByteSizeMaps(base, Map.of(ES94CompressedFlatVectorsFormat.DATA_EXT, entry.payloadLength));
    }

    FieldEntry getFieldEntry(String field) {
        return fields.get(field);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(delegate, dataIn);
    }

    record FieldEntry(int vectorCount, int dimension, byte mode, int originalBytes, long payloadOffset, long payloadLength) {}
}
