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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.vectors.es93.ES93FlatVectorScorer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.codec.vectors.VectorScoringUtils.scoreAndCollectAll;

class ES94CompressedFlatVectorsReader extends FlatVectorsReader {
    private final Map<String, FieldData> fields;
    private final IndexInput dataInput;

    ES94CompressedFlatVectorsReader(SegmentReadState state) throws IOException {
        super(ES94CompressedFlatVectorScorer.INSTANCE);
        this.fields = new HashMap<>();
        final String metaFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ES94CompressedFlatVectorsFormat.META_EXT);
        final String dataFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ES94CompressedFlatVectorsFormat.DATA_EXT);
        IndexInput localDataInput = null;
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
                localDataInput = state.directory.openInput(dataFile, state.context);
                CodecUtil.checkIndexHeader(
                    localDataInput,
                    ES94CompressedFlatVectorsFormat.DATA_CODEC,
                    ES94CompressedFlatVectorsFormat.VERSION_START,
                    ES94CompressedFlatVectorsFormat.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                CodecUtil.retrieveChecksum(localDataInput);
                readFields(metaIn, localDataInput, state.fieldInfos);
            } catch (Throwable t) {
                prior = t;
            } finally {
                CodecUtil.checkFooter(metaIn, prior);
            }
            this.dataInput = localDataInput;
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(localDataInput);
            throw t;
        }
    }

    private ES94CompressedFlatVectorsReader(Map<String, FieldData> fields, IndexInput dataInput) {
        super(ES94CompressedFlatVectorScorer.INSTANCE);
        this.fields = fields;
        this.dataInput = dataInput;
    }

    private void readFields(ChecksumIndexInput metaIn, IndexInput dataIn, FieldInfos fieldInfos) throws IOException {
        for (int fieldNumber = metaIn.readInt(); fieldNumber != -1; fieldNumber = metaIn.readInt()) {
            FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);
            if (fieldInfo == null) {
                throw new CorruptIndexException("Invalid field number [" + fieldNumber + "]", metaIn);
            }
            int dimension = metaIn.readInt();
            VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.values()[metaIn.readInt()];
            int vectorCount = metaIn.readInt();
            int blockCount = metaIn.readInt();
            BlockMeta[] blocks = new BlockMeta[blockCount];
            long totalCompressedBytes = 0L;
            for (int i = 0; i < blockCount; i++) {
                int startOrd = metaIn.readInt();
                int vectorsInBlock = metaIn.readInt();
                byte mode = metaIn.readByte();
                int originalBytes = metaIn.readVInt();
                long payloadOffset = metaIn.readVLong();
                int payloadLength = Math.toIntExact(metaIn.readVLong());
                blocks[i] = new BlockMeta(startOrd, vectorsInBlock, mode, originalBytes, payloadOffset, payloadLength);
                totalCompressedBytes += payloadLength;
            }
            int[] docIds = new int[vectorCount];
            for (int i = 0; i < vectorCount; i++) {
                docIds[i] = metaIn.readVInt();
            }
            fields.put(fieldInfo.name, new FieldData(dimension, similarityFunction, docIds, blocks, totalCompressedBytes));
        }
    }

    @Override
    public FlatVectorsScorer getFlatVectorScorer() {
        return ES94CompressedFlatVectorScorer.INSTANCE;
    }

    @Override
    public FlatVectorsReader getMergeInstance() {
        return new ES94CompressedFlatVectorsReader(fields, dataInput.clone());
    }

    @Override
    public void finishMerge() {}

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        FieldData fieldData = fields.get(field);
        if (fieldData == null) {
            return null;
        }
        return new LazyFloatVectorValues(fieldData, dataInput.clone());
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) {
        return null;
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
        FieldData fieldData = fields.get(field);
        if (fieldData == null || fieldData.docIds.length == 0) {
            return null;
        }
        return ES93FlatVectorScorer.INSTANCE.getRandomVectorScorer(
            fieldData.similarityFunction,
            new LazyFloatVectorValues(fieldData, dataInput.clone()),
            target
        );
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) {
        return null;
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        scoreAndCollectAll(knnCollector, acceptDocs, getRandomVectorScorer(field, target));
    }

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) {
        throw new IllegalArgumentException("ES94CompressedFlatVectorsFormat does not support byte vectors");
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(dataInput);
    }

    @Override
    public long ramBytesUsed() {
        long bytes = 0L;
        for (FieldData fieldData : fields.values()) {
            bytes += (long) fieldData.docIds.length * Integer.BYTES;
            bytes += (long) fieldData.blocks.length * (Integer.BYTES * 3L + Long.BYTES * 2L + Byte.BYTES);
        }
        return bytes;
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        FieldData fieldData = fields.get(fieldInfo.name);
        if (fieldData == null) {
            return Map.of();
        }
        return Map.of(ES94CompressedFlatVectorsFormat.DATA_EXT, fieldData.totalCompressedBytes);
    }

    @Override
    public void close() throws IOException {
        dataInput.close();
    }

    private record BlockMeta(int startOrd, int vectorsInBlock, byte mode, int originalBytes, long payloadOffset, int payloadLength) {}

    private record FieldData(
        int dimension,
        VectorSimilarityFunction similarityFunction,
        int[] docIds,
        BlockMeta[] blocks,
        long totalCompressedBytes
    ) {}

    private static class LazyFloatVectorValues extends FloatVectorValues {
        private final FieldData fieldData;
        private final IndexInput dataIn;

        private int cachedBlockIndex = -1;
        private int cachedBlockStartOrd = -1;
        private float[][] cachedVectors;

        LazyFloatVectorValues(FieldData fieldData, IndexInput dataIn) {
            this.fieldData = fieldData;
            this.dataIn = dataIn;
        }

        @Override
        public int dimension() {
            return fieldData.dimension;
        }

        @Override
        public int size() {
            return fieldData.docIds.length;
        }

        @Override
        public int ordToDoc(int ord) {
            return fieldData.docIds[ord];
        }

        @Override
        public float[] vectorValue(int ord) throws IOException {
            if (ord < 0 || ord >= fieldData.docIds.length) {
                throw new IllegalArgumentException("Invalid ord [" + ord + "]");
            }
            int blockIndex = blockForOrd(ord);
            if (blockIndex != cachedBlockIndex) {
                loadBlock(blockIndex);
            }
            return cachedVectors[ord - cachedBlockStartOrd];
        }

        private int blockForOrd(int ord) {
            int low = 0;
            int high = fieldData.blocks.length - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                BlockMeta block = fieldData.blocks[mid];
                if (ord < block.startOrd) {
                    high = mid - 1;
                } else if (ord >= block.startOrd + block.vectorsInBlock) {
                    low = mid + 1;
                } else {
                    return mid;
                }
            }
            throw new IllegalArgumentException("Invalid ord [" + ord + "]");
        }

        private void loadBlock(int blockIndex) throws IOException {
            BlockMeta block = fieldData.blocks[blockIndex];
            byte[] payload = new byte[block.payloadLength];
            dataIn.seek(block.payloadOffset);
            dataIn.readBytes(payload, 0, payload.length);
            ES94JinaCompressionUtils.CompressedPayload compressedPayload = new ES94JinaCompressionUtils.CompressedPayload(
                block.mode,
                block.originalBytes,
                payload
            );
            cachedVectors = ES94JinaCompressionUtils.decompress(compressedPayload, block.vectorsInBlock, fieldData.dimension);
            cachedBlockIndex = blockIndex;
            cachedBlockStartOrd = block.startOrd;
        }

        @Override
        public DocIndexIterator iterator() {
            return new DocIndexIterator() {
                private int ord = -1;

                @Override
                public int index() {
                    if (ord == DocIdSetIterator.NO_MORE_DOCS) {
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }
                    return ord;
                }

                @Override
                public int docID() {
                    if (ord == DocIdSetIterator.NO_MORE_DOCS) {
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }
                    return ord < 0 || ord >= fieldData.docIds.length ? -1 : fieldData.docIds[ord];
                }

                @Override
                public int nextDoc() {
                    if (ord == DocIdSetIterator.NO_MORE_DOCS) {
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }
                    ord++;
                    if (ord >= fieldData.docIds.length) {
                        ord = DocIdSetIterator.NO_MORE_DOCS;
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }
                    return fieldData.docIds[ord];
                }

                @Override
                public int advance(int target) {
                    int currentDoc = docID();
                    if (currentDoc >= target) {
                        return currentDoc;
                    }
                    int advanced;
                    do {
                        advanced = nextDoc();
                    } while (advanced < target);
                    return advanced;
                }

                @Override
                public long cost() {
                    return fieldData.docIds.length;
                }
            };
        }

        @Override
        public LazyFloatVectorValues copy() throws IOException {
            return new LazyFloatVectorValues(fieldData, dataIn.clone());
        }
    }
}
