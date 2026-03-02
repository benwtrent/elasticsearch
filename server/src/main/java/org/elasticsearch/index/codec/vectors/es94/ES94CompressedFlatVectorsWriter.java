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
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class ES94CompressedFlatVectorsWriter extends FlatVectorsWriter {
    private static final Logger logger = LogManager.getLogger(ES94CompressedFlatVectorsWriter.class);

    private final SegmentWriteState state;
    private final FlatVectorsWriter delegate;
    private final boolean compressionEnabled;
    private final IndexOutput metaOut;
    private final IndexOutput dataOut;
    private final List<FieldWriter> fields = new ArrayList<>();

    @SuppressWarnings("this-escape")
    ES94CompressedFlatVectorsWriter(SegmentWriteState state, FlatVectorsWriter delegate, boolean compressionEnabled) throws IOException {
        super(delegate.getFlatVectorScorer());
        this.state = state;
        this.delegate = delegate;
        this.compressionEnabled = compressionEnabled;
        final String metaFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ES94CompressedFlatVectorsFormat.META_EXT);
        final String dataFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ES94CompressedFlatVectorsFormat.DATA_EXT);
        try {
            this.metaOut = state.directory.createOutput(metaFile, state.context);
            this.dataOut = state.directory.createOutput(dataFile, state.context);
            CodecUtil.writeIndexHeader(
                metaOut,
                ES94CompressedFlatVectorsFormat.META_CODEC,
                ES94CompressedFlatVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            CodecUtil.writeIndexHeader(
                dataOut,
                ES94CompressedFlatVectorsFormat.DATA_CODEC,
                ES94CompressedFlatVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(this);
            throw t;
        }
    }

    @Override
    public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
        FlatFieldVectorsWriter<?> delegateField = delegate.addField(fieldInfo);
        if (fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT32) {
            @SuppressWarnings("unchecked")
            FieldWriter fieldWriter = new FieldWriter(fieldInfo, (FlatFieldVectorsWriter<float[]>) delegateField);
            fields.add(fieldWriter);
            return fieldWriter;
        }
        return delegateField;
    }

    @Override
    public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        delegate.mergeOneField(fieldInfo, mergeState);
        writeNoCompressionMeta(fieldInfo);
    }

    @Override
    public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        CloseableRandomVectorScorerSupplier supplier = delegate.mergeOneFieldToIndex(fieldInfo, mergeState);
        writeNoCompressionMeta(fieldInfo);
        return supplier;
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        delegate.flush(maxDoc, sortMap);
        for (FieldWriter field : fields) {
            writeCompressedField(field);
            field.finish();
        }
    }

    private void writeCompressedField(FieldWriter field) throws IOException {
        if (compressionEnabled == false) {
            writeNoCompressionMeta(field.fieldInfo);
            return;
        }
        ES94JinaCompressionUtils.CompressedPayload payload = ES94JinaCompressionUtils.compress(
            field.getVectors(),
            field.fieldInfo.getVectorDimension(),
            true
        );
        long offset = dataOut.getFilePointer();
        dataOut.writeBytes(payload.payload(), payload.payload().length);
        int vectorCount = field.getDocsWithFieldSet().cardinality();
        if (compressionEnabled && payload.mode() == ES94JinaCompressionUtils.MODE_NONE && vectorCount > 0) {
            logger.debug("ES94 compression requested but zstd path unavailable for field [{}]", field.fieldInfo.name);
        }
        writeMeta(field.fieldInfo.number, vectorCount, field.fieldInfo.getVectorDimension(), payload.mode(), payload.originalBytes(), offset, payload.payload().length);
    }

    private void writeNoCompressionMeta(FieldInfo fieldInfo) throws IOException {
        writeMeta(fieldInfo.number, 0, fieldInfo.getVectorDimension(), ES94JinaCompressionUtils.MODE_NONE, 0, 0L, 0);
    }

    private void writeMeta(int fieldNumber, int vectorCount, int dimension, byte mode, int originalBytes, long payloadOffset, long payloadLength)
        throws IOException {
        metaOut.writeInt(fieldNumber);
        metaOut.writeInt(vectorCount);
        metaOut.writeInt(dimension);
        metaOut.writeByte(mode);
        metaOut.writeVInt(originalBytes);
        metaOut.writeVLong(payloadOffset);
        metaOut.writeVLong(payloadLength);
    }

    @Override
    public void finish() throws IOException {
        delegate.finish();
        metaOut.writeInt(-1);
        CodecUtil.writeFooter(metaOut);
        CodecUtil.writeFooter(dataOut);
    }

    @Override
    public long ramBytesUsed() {
        return delegate.ramBytesUsed() + fields.stream().mapToLong(FieldWriter::ramBytesUsed).sum();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(metaOut, dataOut, delegate);
    }

    static class FieldWriter extends FlatFieldVectorsWriter<float[]> {
        private final FieldInfo fieldInfo;
        private final FlatFieldVectorsWriter<float[]> delegate;
        private final List<float[]> copiedVectors = new ArrayList<>();
        private boolean finished;

        FieldWriter(FieldInfo fieldInfo, FlatFieldVectorsWriter<float[]> delegate) {
            this.fieldInfo = fieldInfo;
            this.delegate = delegate;
        }

        @Override
        public List<float[]> getVectors() {
            return copiedVectors;
        }

        @Override
        public DocsWithFieldSet getDocsWithFieldSet() {
            return delegate.getDocsWithFieldSet();
        }

        @Override
        public void finish() throws IOException {
            if (finished) {
                return;
            }
            finished = true;
            delegate.finish();
        }

        @Override
        public boolean isFinished() {
            return finished && delegate.isFinished();
        }

        @Override
        public void addValue(int docID, float[] vectorValue) throws IOException {
            delegate.addValue(docID, vectorValue);
            copiedVectors.add(copyValue(vectorValue));
        }

        @Override
        public float[] copyValue(float[] vectorValue) {
            return vectorValue.clone();
        }

        @Override
        public long ramBytesUsed() {
            long vectorsBytes = 0L;
            for (float[] vector : copiedVectors) {
                vectorsBytes += (long) vector.length * Float.BYTES;
            }
            return delegate.ramBytesUsed() + vectorsBytes;
        }
    }
}
