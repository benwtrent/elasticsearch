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
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

class ES94CompressedFlatVectorsWriter extends FlatVectorsWriter {
    private static final int VECTORS_PER_BLOCK = 1024;

    private final SegmentWriteState state;
    private final boolean compressionEnabled;
    private final ES94JinaCompressionUtils.CompressionMode compressionMode;
    private final DenseVectorFieldMapper.ElementType elementType;
    private final IndexOutput metaOut;
    private final IndexOutput dataOut;
    private final List<FieldWriter> fields = new ArrayList<>();

    @SuppressWarnings("this-escape")
    ES94CompressedFlatVectorsWriter(
        SegmentWriteState state,
        boolean compressionEnabled,
        ES94JinaCompressionUtils.CompressionMode compressionMode,
        DenseVectorFieldMapper.ElementType elementType
    ) throws IOException {
        super(ES94CompressedFlatVectorScorer.INSTANCE);
        this.state = state;
        this.compressionEnabled = compressionEnabled;
        this.compressionMode = compressionMode;
        this.elementType = elementType;
        final String metaFile = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ES94CompressedFlatVectorsFormat.META_EXT
        );
        final String dataFile = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ES94CompressedFlatVectorsFormat.DATA_EXT
        );
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
    public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) {
        if (fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
            throw new IllegalArgumentException("ES94CompressedFlatVectorsFormat only supports FLOAT32 vectors");
        }
        if (elementType != DenseVectorFieldMapper.ElementType.FLOAT && elementType != DenseVectorFieldMapper.ElementType.BFLOAT16) {
            throw new IllegalArgumentException("ES94CompressedFlatVectorsFormat only supports FLOAT/BFLOAT16 element types");
        }
        FieldWriter writer = new FieldWriter(fieldInfo);
        fields.add(writer);
        return writer;
    }

    @Override
    public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        if (fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
            throw new IllegalArgumentException("ES94CompressedFlatVectorsFormat only supports FLOAT32 vectors");
        }
        var merged = KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
        var iterator = merged.iterator();
        List<float[]> vectors = new ArrayList<>(merged.size());
        List<Integer> docs = new ArrayList<>(merged.size());
        for (int doc = iterator.nextDoc(); doc != KnnVectorValues.DocIndexIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            vectors.add(merged.vectorValue(iterator.index()).clone());
            docs.add(doc);
        }
        writeField(fieldInfo, vectors, docs);
    }

    @Override
    public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        mergeOneField(fieldInfo, mergeState);
        return null;
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        for (FieldWriter field : fields) {
            if (sortMap != null) {
                field.applySort(sortMap);
            }
            writeField(field.fieldInfo, field.vectors, field.docIds);
            field.finish();
        }
    }

    private void writeField(FieldInfo fieldInfo, List<float[]> vectors, List<Integer> docIds) throws IOException {
        metaOut.writeInt(fieldInfo.number);
        metaOut.writeInt(fieldInfo.getVectorDimension());
        metaOut.writeInt(fieldInfo.getVectorSimilarityFunction().ordinal());
        metaOut.writeInt(vectors.size());
        final int blockCount = vectors.isEmpty() ? 0 : (vectors.size() + VECTORS_PER_BLOCK - 1) / VECTORS_PER_BLOCK;
        metaOut.writeInt(blockCount);
        for (int block = 0; block < blockCount; block++) {
            int start = block * VECTORS_PER_BLOCK;
            int end = Math.min(vectors.size(), start + VECTORS_PER_BLOCK);
            ES94JinaCompressionUtils.CompressedPayload payload = ES94JinaCompressionUtils.compress(
                vectors.subList(start, end),
                fieldInfo.getVectorDimension(),
                compressionEnabled,
                compressionMode
            );
            long payloadOffset = dataOut.getFilePointer();
            dataOut.writeBytes(payload.payload(), payload.payload().length);
            metaOut.writeInt(start);
            metaOut.writeInt(end - start);
            metaOut.writeByte(payload.mode());
            metaOut.writeVInt(payload.originalBytes());
            metaOut.writeVLong(payloadOffset);
            metaOut.writeVLong(payload.payload().length);
        }
        for (int docId : docIds) {
            metaOut.writeVInt(docId);
        }
    }

    @Override
    public void finish() throws IOException {
        metaOut.writeInt(-1);
        CodecUtil.writeFooter(metaOut);
        CodecUtil.writeFooter(dataOut);
    }

    @Override
    public long ramBytesUsed() {
        long bytes = 0L;
        for (FieldWriter field : fields) {
            bytes += field.ramBytesUsed();
        }
        return bytes;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(metaOut, dataOut);
    }

    static class FieldWriter extends FlatFieldVectorsWriter<float[]> {
        private final FieldInfo fieldInfo;
        private DocsWithFieldSet docsWithFieldSet = new DocsWithFieldSet();
        private final List<float[]> vectors = new ArrayList<>();
        private final List<Integer> docIds = new ArrayList<>();
        private boolean finished;

        FieldWriter(FieldInfo fieldInfo) {
            this.fieldInfo = fieldInfo;
        }

        @Override
        public List<float[]> getVectors() {
            return vectors;
        }

        @Override
        public DocsWithFieldSet getDocsWithFieldSet() {
            return docsWithFieldSet;
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public void addValue(int docID, float[] vectorValue) {
            docsWithFieldSet.add(docID);
            docIds.add(docID);
            vectors.add(copyValue(vectorValue));
        }

        @Override
        public float[] copyValue(float[] vectorValue) {
            return vectorValue.clone();
        }

        void applySort(Sorter.DocMap sortMap) {
            final int size = vectors.size();
            Integer[] order = new Integer[size];
            for (int i = 0; i < size; i++) {
                order[i] = i;
            }
            Arrays.sort(order, Comparator.comparingInt(i -> sortMap.oldToNew(docIds.get(i))));

            List<float[]> sortedVectors = new ArrayList<>(size);
            List<Integer> sortedDocIds = new ArrayList<>(size);
            docsWithFieldSet = new DocsWithFieldSet();
            for (Integer i : order) {
                int newDoc = sortMap.oldToNew(docIds.get(i));
                sortedVectors.add(vectors.get(i));
                sortedDocIds.add(newDoc);
                docsWithFieldSet.add(newDoc);
            }
            vectors.clear();
            vectors.addAll(sortedVectors);
            docIds.clear();
            docIds.addAll(sortedDocIds);
        }

        @Override
        public long ramBytesUsed() {
            long bytes = 0L;
            for (float[] vector : vectors) {
                bytes += (long) vector.length * Float.BYTES;
            }
            bytes += (long) docIds.size() * Integer.BYTES;
            return bytes;
        }
    }
}
