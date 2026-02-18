/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.GroupVIntUtil;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.index.codec.vectors.GenericFlatVectorReaders;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.cluster.NeighborQueue;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIterator;
import org.elasticsearch.index.codec.vectors.diskbbq.DocIdsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.index.codec.vectors.diskbbq.PostingMetadata;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.index.codec.vectors.diskbbq.PrefetchingCentroidIterator;
import org.elasticsearch.index.codec.vectors.diskbbq.VectorPreconditioner;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;
import org.elasticsearch.simdvec.ESNextOSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer.DEFAULT_LAMBDA;
import static org.elasticsearch.index.codec.vectors.diskbbq.PostingMetadata.NO_ORDINAL;
import static org.elasticsearch.simdvec.ESNextOSQVectorsScorer.BULK_SIZE;

/**
 * Default implementation of {@link IVFVectorsReader}. It scores the posting lists centroids using
 * brute force and then scores the top ones using the posting list.
 */
public class ESNextDiskBBQVectorsReader extends IVFVectorsReader implements VectorPreconditioner {
    private static final Logger logger = LogManager.getLogger(ESNextDiskBBQVectorsReader.class);
    private static final float GRAPH_CENTROID_OVERSAMPLE_MULTIPLIER = 2.5f;
    private static final int CENTROID_BULK_SIZE = 16;

    public ESNextDiskBBQVectorsReader(SegmentReadState state, GenericFlatVectorReaders.LoadFlatVectorsReader getFormatReader)
        throws IOException {
        super(state, getFormatReader);
    }

    CentroidIterator getPostingListPrefetchIterator(CentroidIterator centroidIterator, IndexInput postingListSlice) throws IOException {
        // TODO we may want to prefetch more than one postings list, however, we will likely want to place a limit
        // so we don't bother prefetching many lists we won't end up scoring
        return new PrefetchingCentroidIterator(centroidIterator, postingListSlice);
    }

    static long directWriterSizeOnDisk(long numValues, int bitsPerValue) {
        // TODO: use method in https://github.com/apache/lucene/pull/15422 when/if merged.
        long bytes = (numValues * bitsPerValue + Byte.SIZE - 1) / 8;
        int paddingBitsNeeded;
        if (bitsPerValue > Integer.SIZE) {
            paddingBitsNeeded = Long.SIZE - bitsPerValue;
        } else if (bitsPerValue > Short.SIZE) {
            paddingBitsNeeded = Integer.SIZE - bitsPerValue;
        } else if (bitsPerValue > Byte.SIZE) {
            paddingBitsNeeded = Short.SIZE - bitsPerValue;
        } else {
            paddingBitsNeeded = 0;
        }
        final int paddingBytesNeeded = (paddingBitsNeeded + Byte.SIZE - 1) / Byte.SIZE;
        return bytes + paddingBytesNeeded;
    }

    @Override
    public CentroidIterator getCentroidIterator(
        FieldInfo fieldInfo,
        int numCentroids,
        IndexInput centroids,
        float[] targetQuery,
        IndexInput postingListSlice,
        AcceptDocs acceptDocs,
        float approximateCost,
        FloatVectorValues values,
        float visitRatio
    ) throws IOException {
        final FieldEntry fieldEntry = fields.get(fieldInfo.number);
        int bulkSize = CENTROID_BULK_SIZE;
        float approximateDocsPerCentroid = approximateCost / numCentroids;
        if (approximateDocsPerCentroid <= 1.25) {
            // TODO: we need to make this call to build the iterator, otherwise accept docs breaks all together
            approximateDocsPerCentroid = (float) acceptDocs.cost() / numCentroids;
        }
        final int bitsRequired = DirectWriter.bitsRequired(numCentroids);
        final long sizeLookup = directWriterSizeOnDisk(values.size(), bitsRequired);
        final long fp = centroids.getFilePointer();
        final FixedBitSet acceptCentroids;
        if (approximateDocsPerCentroid > 1.25 || numCentroids == 1) {
            // only apply centroid filtering when we expect some / many centroids will not have
            // any matching document.
            acceptCentroids = null;
        } else {
            acceptCentroids = new FixedBitSet(numCentroids);
            final KnnVectorValues.DocIndexIterator docIndexIterator = values.iterator();
            final DocIdSetIterator iterator = ConjunctionUtils.intersectIterators(List.of(acceptDocs.iterator(), docIndexIterator));
            final LongValues longValues = DirectReader.getInstance(centroids.randomAccessSlice(fp, sizeLookup), bitsRequired);
            int doc = iterator.nextDoc();
            for (; doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                acceptCentroids.set((int) longValues.get(docIndexIterator.index()));
            }
        }
        final OptimizedScalarQuantizer scalarQuantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        final int[] scratch = new int[targetQuery.length];
        final OptimizedScalarQuantizer.QuantizationResult queryParams = scalarQuantizer.scalarQuantize(
            targetQuery,
            new float[targetQuery.length],
            scratch,
            (byte) 7,
            fieldEntry.globalCentroid()
        );
        final byte[] quantized = new byte[targetQuery.length];
        for (int i = 0; i < quantized.length; i++) {
            quantized[i] = (byte) scratch[i];
        }
        final ES92Int7VectorsScorer scorer = ESVectorUtil.getES92Int7VectorsScorer(centroids, fieldInfo.getVectorDimension(), bulkSize);
        centroids.seek(fp + sizeLookup);
        int numParents = centroids.readVInt();
        final NextFieldEntry nextFieldEntry = (NextFieldEntry) fieldEntry;
        final long quantizedStart = centroids.getFilePointer() + (long) numParents * fieldInfo.getVectorDimension() * Float.BYTES;
        final CentroidIterator centroidIterator = nextFieldEntry.hasCentroidGraph()
            ? getCentroidIteratorGraph(
                fieldInfo,
                centroids,
                nextFieldEntry,
                quantizedStart,
                numParents,
                numCentroids,
                quantized,
                queryParams,
                fieldEntry.globalCentroidDp(),
                acceptCentroids,
                visitRatio
            )
            : getCentroidIteratorFlat(
                fieldInfo,
                centroids,
                numParents,
                numCentroids,
                scorer,
                quantized,
                queryParams,
                fieldEntry.globalCentroidDp(),
                acceptCentroids,
                bulkSize
            );
        return getPostingListPrefetchIterator(centroidIterator, postingListSlice);
    }

    @Override
    protected FieldEntry doReadField(
        IndexInput input,
        String rawVectorFormat,
        boolean useDirectIOReads,
        VectorSimilarityFunction similarityFunction,
        VectorEncoding vectorEncoding,
        int numCentroids,
        long centroidOffset,
        long centroidLength,
        long postingListOffset,
        long postingListLength,
        float[] globalCentroid,
        float globalCentroidDp
    ) throws IOException {
        int bulkSize = input.readInt();
        ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding = ESNextDiskBBQVectorsFormat.QuantEncoding.fromId(input.readInt());
        long preconditionerLength = input.readLong();
        long preconditionerOffset = -1;
        if (preconditionerLength > 0) {
            preconditionerOffset = input.readLong();
        }
        long centroidIndexLength = input.readLong();
        long centroidIndexOffset = -1;
        int centroidGraphNumLevels = 0;
        int centroidGraphMaxConn = HnswGraph.UNKNOWN_MAX_CONN;
        int[][] centroidGraphNodesByLevel = new int[0][];
        int centroidGraphOffsetsBitsPerValue = -1;
        long centroidGraphValueCount = 0L;
        long centroidGraphOffsetsDataOffset = -1L;
        long centroidGraphOffsetsDataLength = 0L;
        if (centroidIndexLength > 0) {
            centroidIndexOffset = input.readLong();
        }
        return new NextFieldEntry(
            rawVectorFormat,
            useDirectIOReads,
            similarityFunction,
            vectorEncoding,
            numCentroids,
            centroidOffset,
            centroidLength,
            postingListOffset,
            postingListLength,
            globalCentroid,
            globalCentroidDp,
            quantEncoding,
            bulkSize,
            preconditionerOffset,
            preconditionerLength,
            centroidIndexOffset,
            centroidIndexLength,
            centroidGraphNumLevels,
            centroidGraphMaxConn,
            centroidGraphNodesByLevel,
            centroidGraphOffsetsBitsPerValue,
            centroidGraphValueCount,
            centroidGraphOffsetsDataOffset,
            centroidGraphOffsetsDataLength
        );
    }

    @Override
    public Preconditioner getPreconditioner(FieldInfo fieldInfo) throws IOException {
        final FieldEntry fieldEntry = fields.get(fieldInfo.number);
        // only seems possible in tests
        if (fieldEntry == null) {
            return null;
        }
        long preconditionerOffset = ((NextFieldEntry) fieldEntry).preconditionerOffset();
        long preconditionerLength = ((NextFieldEntry) fieldEntry).preconditionerLength();
        if (preconditionerLength > 0) {
            IndexInput ivfPreconditionerSlice = ivfCentroids.slice("preconditioner", preconditionerOffset, preconditionerLength);
            if (ivfPreconditionerSlice != null) {
                ivfPreconditionerSlice.seek(0);
                return Preconditioner.read(ivfPreconditionerSlice);
            }
        }
        return null;
    }

    static class NextFieldEntry extends FieldEntry {
        private final ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding;
        protected final long preconditionerOffset;
        protected final long preconditionerLength;
        protected final long centroidIndexOffset;
        protected final long centroidIndexLength;
        protected final int centroidGraphNumLevels;
        protected final int centroidGraphMaxConn;
        protected final int[][] centroidGraphNodesByLevel;
        protected final int centroidGraphOffsetsBitsPerValue;
        protected final long centroidGraphValueCount;
        protected final long centroidGraphOffsetsDataOffset;
        protected final long centroidGraphOffsetsDataLength;

        NextFieldEntry(
            String rawVectorFormat,
            boolean doDirectIOReads,
            VectorSimilarityFunction similarityFunction,
            VectorEncoding vectorEncoding,
            int numCentroids,
            long centroidOffset,
            long centroidLength,
            long postingListOffset,
            long postingListLength,
            float[] globalCentroid,
            float globalCentroidDp,
            ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding,
            int bulkSize,
            long preconditionerOffset,
            long preconditionerLength,
            long centroidIndexOffset,
            long centroidIndexLength,
            int centroidGraphNumLevels,
            int centroidGraphMaxConn,
            int[][] centroidGraphNodesByLevel,
            int centroidGraphOffsetsBitsPerValue,
            long centroidGraphValueCount,
            long centroidGraphOffsetsDataOffset,
            long centroidGraphOffsetsDataLength
        ) {
            super(
                rawVectorFormat,
                doDirectIOReads,
                similarityFunction,
                vectorEncoding,
                numCentroids,
                centroidOffset,
                centroidLength,
                postingListOffset,
                postingListLength,
                globalCentroid,
                globalCentroidDp,
                bulkSize
            );
            this.quantEncoding = quantEncoding;
            this.preconditionerOffset = preconditionerOffset;
            this.preconditionerLength = preconditionerLength;
            this.centroidIndexOffset = centroidIndexOffset;
            this.centroidIndexLength = centroidIndexLength;
            this.centroidGraphNumLevels = centroidGraphNumLevels;
            this.centroidGraphMaxConn = centroidGraphMaxConn;
            this.centroidGraphNodesByLevel = centroidGraphNodesByLevel;
            this.centroidGraphOffsetsBitsPerValue = centroidGraphOffsetsBitsPerValue;
            this.centroidGraphValueCount = centroidGraphValueCount;
            this.centroidGraphOffsetsDataOffset = centroidGraphOffsetsDataOffset;
            this.centroidGraphOffsetsDataLength = centroidGraphOffsetsDataLength;
        }

        public ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding() {
            return quantEncoding;
        }

        public long preconditionerOffset() {
            return preconditionerOffset;
        }

        public long preconditionerLength() {
            return preconditionerLength;
        }

        public long centroidIndexOffset() {
            return centroidIndexOffset;
        }

        public long centroidIndexLength() {
            return centroidIndexLength;
        }

        public boolean hasCentroidGraph() {
            return centroidIndexLength > 0 && centroidIndexOffset >= 0;
        }
    }

    private CentroidIterator getCentroidIteratorGraph(
        FieldInfo fieldInfo,
        IndexInput centroids,
        NextFieldEntry fieldEntry,
        long quantizedStart,
        int numParents,
        int numCentroids,
        byte[] quantizeQuery,
        OptimizedScalarQuantizer.QuantizationResult queryParams,
        float globalCentroidDp,
        FixedBitSet acceptCentroids,
        float visitRatio
    ) throws IOException {
        final LayeredCentroidTree tree = new LayeredCentroidTree(
            fieldInfo.getVectorDimension(),
            ivfCentroids.slice("centroid-tree", fieldEntry.centroidIndexOffset(), fieldEntry.centroidIndexLength())
        );
        if (logger.isDebugEnabled()) {
            logger.debug(
                "loaded layered centroid tree [field={}, numCentroids={}, layer1Nodes={}, layer0Nodes={}]",
                fieldInfo.name,
                numCentroids,
                tree.layerNodeCount(1),
                tree.layerNodeCount(0)
            );
        }
        if (tree.isEmpty()) {
            final ES92Int7VectorsScorer scorer = ESVectorUtil.getES92Int7VectorsScorer(
                centroids,
                fieldInfo.getVectorDimension(),
                CENTROID_BULK_SIZE
            );
            return getCentroidIteratorFlat(
                fieldInfo,
                centroids,
                numParents,
                numCentroids,
                scorer,
                quantizeQuery,
                queryParams,
                globalCentroidDp,
                acceptCentroids,
                CENTROID_BULK_SIZE
            );
        }
        final int desiredCentroids = Math.max(1, Math.min(numCentroids, (int) Math.ceil(numCentroids * visitRatio)));
        final int gatheredCentroids = Math.max(
            desiredCentroids,
            Math.min(numCentroids, (int) Math.ceil(desiredCentroids * GRAPH_CENTROID_OVERSAMPLE_MULTIPLIER))
        );
        final CentroidSearchStats searchStats = logger.isDebugEnabled() ? new CentroidSearchStats() : null;
        final IndexInput treeScoringInput = tree.data.clone();
        final ES92Int7VectorsScorer treeScorer = ESVectorUtil.getES92Int7VectorsScorer(
            treeScoringInput,
            fieldInfo.getVectorDimension(),
            CENTROID_BULK_SIZE
        );
        final float[] scoreScratch = new float[CENTROID_BULK_SIZE];
        final NeighborQueue layer0Queue = new NeighborQueue(Math.max(1, tree.layerNodeCount(0)), true);
        tree.scoreLayer0Nodes(
            layer0Queue,
            treeScoringInput,
            treeScorer,
            quantizeQuery,
            queryParams,
            fieldInfo.getVectorSimilarityFunction(),
            globalCentroidDp,
            scoreScratch,
            searchStats
        );
        final TopKnnCollector collector = new TopKnnCollector(gatheredCentroids, Integer.MAX_VALUE);
        final TreeCentroidSearcher treeSearcher = new TreeCentroidSearcher(Math.max(1, tree.layerNodeCount(1)));
        final int initialLayer0ToExplore = tree.layerNodeCount(0);
        int expandedLayer0 = 0;
        int expandedLayer1 = 0;
        treeSearcher.search(
            tree,
            layer0Queue,
            collector,
            initialLayer0ToExplore,
            treeScoringInput,
            treeScorer,
            quantizeQuery,
            queryParams,
            fieldInfo.getVectorSimilarityFunction(),
            globalCentroidDp,
            scoreScratch,
            acceptCentroids,
            searchStats
        );
        expandedLayer0 = treeSearcher.expandedLayer0();
        expandedLayer1 = treeSearcher.expandedLayer1();
        final ScoreDoc[] scoreDocs = collector.topDocs().scoreDocs;
        if (searchStats != null) {
            logger.debug(
                "layered centroid tree stats [field={}, centroids={}, acceptedCentroids={}, desiredCentroids={}, gatheredCentroids={}, returnedCentroids={}, initialLayer0={}, expandedLayer0={}, expandedLayer1={}, queuedLayer1={}, nodesVisited={}, blocksVisited={}]",
                fieldInfo.name,
                numCentroids,
                acceptCentroids == null ? numCentroids : acceptCentroids.cardinality(),
                desiredCentroids,
                gatheredCentroids,
                scoreDocs.length,
                initialLayer0ToExplore,
                expandedLayer0,
                expandedLayer1,
                treeSearcher.layer1QueueSize(),
                searchStats.nodesVisited(),
                searchStats.blocksVisited()
            );
        }
        if (scoreDocs.length == 0) {
            final ES92Int7VectorsScorer scorer = ESVectorUtil.getES92Int7VectorsScorer(
                centroids,
                fieldInfo.getVectorDimension(),
                CENTROID_BULK_SIZE
            );
            return getCentroidIteratorFlat(
                fieldInfo,
                centroids,
                numParents,
                numCentroids,
                scorer,
                quantizeQuery,
                queryParams,
                globalCentroidDp,
                acceptCentroids,
                CENTROID_BULK_SIZE
            );
        }
        final long postingsOffset = quantizedStart + (long) numCentroids * (fieldInfo.getVectorDimension() + 3L * Float.BYTES
            + Integer.BYTES);
        return new CentroidIterator() {
            private int scoreDocIdx = 0;

            @Override
            public boolean hasNext() {
                return scoreDocIdx < scoreDocs.length;
            }

            @Override
            public PostingMetadata nextPosting() throws IOException {
                final ScoreDoc scoreDoc = scoreDocs[scoreDocIdx++];
                final int centroidOrd = scoreDoc.doc;
                final float score = scoreDoc.score;
                centroids.seek(postingsOffset + (Long.BYTES * 2L + Integer.BYTES) * centroidOrd);
                long postingListOffset = centroids.readLong();
                long postingListLength = centroids.readLong();
                int parentOrd = centroids.readInt();
                return new PostingMetadata(postingListOffset, postingListLength, parentOrd, score);
            }
        };
    }

    private static class LayeredCentroidTree {
        private final int quantizedRecordSize;
        private final IndexInput data;
        private final int[] layerNodeCounts;
        private final long layer0VectorsOffset;
        private final long[][] layerChildOffsets;
        private final int[][] layerChildCounts;

        LayeredCentroidTree(int dimension, IndexInput data) throws IOException {
            this.quantizedRecordSize = dimension + 3 * Float.BYTES + Integer.BYTES;
            this.data = data;
            this.layerNodeCounts = new int[] { data.readVInt(), data.readVInt(), data.readVInt() };
            this.layer0VectorsOffset = data.getFilePointer();
            data.skipBytes((long) layerNodeCounts[0] * quantizedRecordSize);
            this.layerChildOffsets = new long[3][];
            this.layerChildCounts = new int[3][];
            this.layerChildOffsets[0] = new long[layerNodeCounts[0]];
            this.layerChildCounts[0] = new int[layerNodeCounts[0]];
            for (int i = 0; i < layerNodeCounts[0]; i++) {
                final int childCount = data.readVInt();
                layerChildCounts[0][i] = childCount;
                layerChildOffsets[0][i] = data.getFilePointer();
                data.skipBytes((long) childCount * quantizedRecordSize);
                for (int c = 0; c < childCount; c++) {
                    data.readVInt();
                }
            }
            this.layerChildOffsets[1] = new long[layerNodeCounts[1]];
            this.layerChildCounts[1] = new int[layerNodeCounts[1]];
            for (int i = 0; i < layerNodeCounts[1]; i++) {
                final int childCount = data.readVInt();
                layerChildCounts[1][i] = childCount;
                layerChildOffsets[1][i] = data.getFilePointer();
                data.skipBytes((long) childCount * quantizedRecordSize);
                for (int c = 0; c < childCount; c++) {
                    data.readVInt();
                }
            }
            this.layerChildOffsets[2] = new long[0];
            this.layerChildCounts[2] = new int[0];
        }

        boolean isEmpty() {
            return layerNodeCounts[0] <= 0 || layerNodeCounts[1] <= 0;
        }

        int layerNodeCount(int layer) {
            return layerNodeCounts[layer];
        }

        void scoreLayer0Nodes(
            NeighborQueue resultQueue,
            IndexInput scoringInput,
            ES92Int7VectorsScorer scorer,
            byte[] quantizedQuery,
            OptimizedScalarQuantizer.QuantizationResult queryParams,
            VectorSimilarityFunction similarityFunction,
            float globalCentroidDp,
            float[] scoreScratch,
            CentroidSearchStats searchStats
        ) throws IOException {
            scoringInput.seek(layer0VectorsOffset);
            int limit = layerNodeCounts[0] - CENTROID_BULK_SIZE + 1;
            int i = 0;
            for (; i < limit; i += CENTROID_BULK_SIZE) {
                if (searchStats != null) {
                    searchStats.recordScoredBlock(CENTROID_BULK_SIZE);
                }
                scorer.scoreBulk(
                    quantizedQuery,
                    queryParams.lowerInterval(),
                    queryParams.upperInterval(),
                    queryParams.quantizedComponentSum(),
                    queryParams.additionalCorrection(),
                    similarityFunction,
                    globalCentroidDp,
                    scoreScratch,
                    CENTROID_BULK_SIZE
                );
                for (int j = 0; j < CENTROID_BULK_SIZE; j++) {
                    resultQueue.add(i + j, scoreScratch[j]);
                }
            }
            final int tail = layerNodeCounts[0] - i;
            if (tail > 0) {
                if (searchStats != null) {
                    searchStats.recordScoredBlock(tail);
                }
                scorer.scoreBulk(
                    quantizedQuery,
                    queryParams.lowerInterval(),
                    queryParams.upperInterval(),
                    queryParams.quantizedComponentSum(),
                    queryParams.additionalCorrection(),
                    similarityFunction,
                    globalCentroidDp,
                    scoreScratch,
                    tail
                );
                for (int j = 0; j < tail; j++) {
                    resultQueue.add(i + j, scoreScratch[j]);
                }
            }
        }

        void scoreLayerChildrenToQueue(
            int layer,
            int nodeOrd,
            NeighborQueue resultQueue,
            IndexInput scoringInput,
            ES92Int7VectorsScorer scorer,
            byte[] quantizedQuery,
            OptimizedScalarQuantizer.QuantizationResult queryParams,
            VectorSimilarityFunction similarityFunction,
            float globalCentroidDp,
            float[] scoreScratch,
            CentroidSearchStats searchStats
        ) throws IOException {
            scoreChildren(
                layerChildOffsets[layer][nodeOrd],
                layerChildCounts[layer][nodeOrd],
                scoringInput,
                scorer,
                quantizedQuery,
                queryParams,
                similarityFunction,
                globalCentroidDp,
                scoreScratch,
                null,
                resultQueue::add,
                searchStats
            );
        }

        void scoreLayerChildrenToCollector(
            int layer,
            int nodeOrd,
            IndexInput scoringInput,
            ES92Int7VectorsScorer scorer,
            byte[] quantizedQuery,
            OptimizedScalarQuantizer.QuantizationResult queryParams,
            VectorSimilarityFunction similarityFunction,
            float globalCentroidDp,
            float[] scoreScratch,
            FixedBitSet acceptCentroids,
            IntFloatConsumer scoreConsumer,
            CentroidSearchStats searchStats
        ) throws IOException {
            scoreChildren(
                layerChildOffsets[layer][nodeOrd],
                layerChildCounts[layer][nodeOrd],
                scoringInput,
                scorer,
                quantizedQuery,
                queryParams,
                similarityFunction,
                globalCentroidDp,
                scoreScratch,
                acceptCentroids,
                scoreConsumer,
                searchStats
            );
        }

        private void scoreChildren(
            long childrenOffset,
            int childrenCount,
            IndexInput scoringInput,
            ES92Int7VectorsScorer scorer,
            byte[] quantizedQuery,
            OptimizedScalarQuantizer.QuantizationResult queryParams,
            VectorSimilarityFunction similarityFunction,
            float globalCentroidDp,
            float[] scoreScratch,
            FixedBitSet acceptCentroids,
            IntFloatConsumer scoreConsumer,
            CentroidSearchStats searchStats
        ) throws IOException {
            if (childrenCount == 0) {
                return;
            }
            scoringInput.seek(childrenOffset);
            long ordinalsPointer = childrenOffset + (long) childrenCount * quantizedRecordSize;
            final int limit = childrenCount - CENTROID_BULK_SIZE + 1;
            int i = 0;
            for (; i < limit; i += CENTROID_BULK_SIZE) {
                if (searchStats != null) {
                    searchStats.recordScoredBlock(CENTROID_BULK_SIZE);
                }
                scorer.scoreBulk(
                    quantizedQuery,
                    queryParams.lowerInterval(),
                    queryParams.upperInterval(),
                    queryParams.quantizedComponentSum(),
                    queryParams.additionalCorrection(),
                    similarityFunction,
                    globalCentroidDp,
                    scoreScratch,
                    CENTROID_BULK_SIZE
                );
                final long nextVectorPointer = scoringInput.getFilePointer();
                scoringInput.seek(ordinalsPointer);
                for (int j = 0; j < CENTROID_BULK_SIZE; j++) {
                    final int childOrd = scoringInput.readVInt();
                    if (acceptCentroids == null || acceptCentroids.get(childOrd)) {
                        scoreConsumer.accept(childOrd, scoreScratch[j]);
                    }
                }
                ordinalsPointer = scoringInput.getFilePointer();
                scoringInput.seek(nextVectorPointer);
            }
            final int tail = childrenCount - i;
            if (tail > 0) {
                if (searchStats != null) {
                    searchStats.recordScoredBlock(tail);
                }
                scorer.scoreBulk(
                    quantizedQuery,
                    queryParams.lowerInterval(),
                    queryParams.upperInterval(),
                    queryParams.quantizedComponentSum(),
                    queryParams.additionalCorrection(),
                    similarityFunction,
                    globalCentroidDp,
                    scoreScratch,
                    tail
                );
                final long nextVectorPointer = scoringInput.getFilePointer();
                scoringInput.seek(ordinalsPointer);
                for (int j = 0; j < tail; j++) {
                    final int childOrd = scoringInput.readVInt();
                    if (acceptCentroids == null || acceptCentroids.get(childOrd)) {
                        scoreConsumer.accept(childOrd, scoreScratch[j]);
                    }
                }
                scoringInput.seek(nextVectorPointer);
            }
        }
    }

    @FunctionalInterface
    private interface IntFloatConsumer {
        void accept(int value, float score);
    }

    private static boolean shouldScoreMoreLayerNodes(NeighborQueue layerQueue, KnnCollector collector) {
        if (layerQueue.size() == 0) {
            return false;
        }
        return layerQueue.topScore() > collector.minCompetitiveSimilarity();
    }

    private static class TreeCentroidSearcher {
        private final NeighborQueue layer1Queue;
        private int expandedLayer0;
        private int expandedLayer1;

        private TreeCentroidSearcher(int layer1QueueSize) {
            this.layer1Queue = new NeighborQueue(Math.max(1, layer1QueueSize), true);
        }

        void search(
            LayeredCentroidTree tree,
            NeighborQueue layer0Queue,
            KnnCollector collector,
            int initialLayer0ToExplore,
            IndexInput scoringInput,
            ES92Int7VectorsScorer scorer,
            byte[] quantizedQuery,
            OptimizedScalarQuantizer.QuantizationResult queryParams,
            VectorSimilarityFunction similarityFunction,
            float globalCentroidDp,
            float[] scoreScratch,
            FixedBitSet acceptCentroids,
            CentroidSearchStats searchStats
        ) throws IOException {
            for (int i = 0; i < initialLayer0ToExplore && layer0Queue.size() > 0; i++) {
                final int layer0Ord = layer0Queue.pop();
                expandedLayer0++;
                tree.scoreLayerChildrenToQueue(
                    0,
                    layer0Ord,
                    layer1Queue,
                    scoringInput,
                    scorer,
                    quantizedQuery,
                    queryParams,
                    similarityFunction,
                    globalCentroidDp,
                    scoreScratch,
                    searchStats
                );
            }

            while (layer1Queue.size() > 0 || layer0Queue.size() > 0) {
                if (layer1Queue.size() == 0 && layer0Queue.size() > 0) {
                    final int layer0Ord = layer0Queue.pop();
                    expandedLayer0++;
                    tree.scoreLayerChildrenToQueue(
                        0,
                        layer0Ord,
                        layer1Queue,
                        scoringInput,
                        scorer,
                        quantizedQuery,
                        queryParams,
                        similarityFunction,
                        globalCentroidDp,
                        scoreScratch,
                        searchStats
                    );
                }
                if (layer1Queue.size() == 0) {
                    break;
                }
                if (shouldScoreMoreLayerNodes(layer1Queue, collector) == false) {
                    break;
                }
                expandedLayer1++;
                final int layer1Ord = layer1Queue.pop();
                tree.scoreLayerChildrenToCollector(
                    1,
                    layer1Ord,
                    scoringInput,
                    scorer,
                    quantizedQuery,
                    queryParams,
                    similarityFunction,
                    globalCentroidDp,
                    scoreScratch,
                    acceptCentroids,
                    collector::collect,
                    searchStats
                );
            }
        }

        int expandedLayer0() {
            return expandedLayer0;
        }

        int expandedLayer1() {
            return expandedLayer1;
        }

        int layer1QueueSize() {
            return layer1Queue.size();
        }

    }

    private static CentroidIterator getCentroidIteratorFlat(
        FieldInfo fieldInfo,
        IndexInput centroids,
        int numParents,
        int numCentroids,
        ES92Int7VectorsScorer scorer,
        byte[] quantizeQuery,
        OptimizedScalarQuantizer.QuantizationResult queryParams,
        float globalCentroidDp,
        FixedBitSet acceptCentroids,
        int bulkSize
    ) throws IOException {
        if (numParents > 0) {
            // Parent centroids are stored raw only for query quantization; centroid scoring is always flat.
            centroids.skipBytes((long) numParents * fieldInfo.getVectorDimension() * Float.BYTES);
        }
        final NeighborQueue neighborQueue = new NeighborQueue(numCentroids, true);
        final CentroidSearchStats searchStats = logger.isDebugEnabled() ? new CentroidSearchStats() : null;
        final long centroidQuantizeSize = fieldInfo.getVectorDimension() + 3 * Float.BYTES + Integer.BYTES;
        score(
            neighborQueue,
            numCentroids,
            0,
            scorer,
            centroids,
            centroidQuantizeSize,
            quantizeQuery,
            queryParams,
            globalCentroidDp,
            fieldInfo.getVectorSimilarityFunction(),
            new float[bulkSize],
            acceptCentroids,
            bulkSize,
            searchStats
        );
        if (searchStats != null) {
            logger.debug(
                "flat centroid search stats [field={}, centroids={}, acceptedCentroids={}, nodesVisited={}, blocksVisited={}]",
                fieldInfo.name,
                numCentroids,
                acceptCentroids == null ? numCentroids : acceptCentroids.cardinality(),
                searchStats.nodesVisited(),
                searchStats.blocksVisited()
            );
        }
        long offset = centroids.getFilePointer();
        return new CentroidIterator() {
            @Override
            public boolean hasNext() {
                return neighborQueue.size() > 0;
            }

            @Override
            public PostingMetadata nextPosting() throws IOException {
                long centroidOrdinalAndScore = neighborQueue.popRaw();
                int centroidOrd = neighborQueue.decodeNodeId(centroidOrdinalAndScore);
                float score = neighborQueue.decodeScore(centroidOrdinalAndScore);
                centroids.seek(offset + (Long.BYTES * 2L + Integer.BYTES) * centroidOrd);
                long postingListOffset = centroids.readLong();
                long postingListLength = centroids.readLong();
                int parentOrd = centroids.readInt();
                return new PostingMetadata(postingListOffset, postingListLength, parentOrd, score);
            }
        };
    }

    /**
     * Expands a node at one level of the cluster hierarchy into scored children.
     * For single-parent: expands a parent ordinal into scored child centroids.
     * For future multi-level hierarchies: one instance per non-leaf level.
     */
    @FunctionalInterface
    interface ClusterLevelExpander {
        /**
         * Expand the node at the given ordinal, scoring its children
         * and adding them to the provided queue.
         */
        void expandNode(int nodeOrd, NeighborQueue childrenQueue) throws IOException;
    }

    /**
     * Converts a leaf-level centroid ordinal and score into {@link PostingMetadata}.
     * Decouples the beam search result iteration from the on-disk posting metadata layout.
     */
    @FunctionalInterface
    interface LeafPostingReader {
        PostingMetadata readPostingMetadata(int centroidOrd, float centroidScore) throws IOException;
    }

    /**
     * A centroid iterator that uses beam-width search to find the best centroids.
     * Scores all parents, expands the top-B parents (the beam), scores all their children,
     * and ranks children from all expanded parents together (interleaved by score).
     * Supports continuation via {@link #continueSearch()} for exceptional cases where
     * the initial beam didn't produce enough centroids.
     */
    public static class BeamSearchCentroidIterator implements CentroidIterator {
        private final NeighborQueue resultQueue;
        private final NeighborQueue parentsQueue;
        private final NeighborQueue tempQueue;
        private final ClusterLevelExpander expander;
        private final LeafPostingReader leafReader;

        BeamSearchCentroidIterator(
            NeighborQueue parentsQueue,
            int beamWidth,
            int initialResultCapacity,
            int maxChildrenPerNode,
            ClusterLevelExpander expander,
            LeafPostingReader leafReader
        ) throws IOException {
            this.parentsQueue = parentsQueue;
            this.expander = expander;
            this.leafReader = leafReader;
            this.tempQueue = new NeighborQueue(maxChildrenPerNode, true);
            this.resultQueue = new NeighborQueue(initialResultCapacity, true);
            // Expand the beam: pop top beamWidth parents and score all their children
            int expanded = 0;
            while (parentsQueue.size() > 0 && expanded < beamWidth) {
                expandNode(parentsQueue.pop());
                expanded++;
            }
        }

        private void expandNode(int nodeOrd) throws IOException {
            tempQueue.clear();
            expander.expandNode(nodeOrd, tempQueue);
            // Move all scored children into the result queue (interleaved with existing results)
            while (tempQueue.size() > 0) {
                float childScore = tempQueue.topScore();
                int child = tempQueue.pop();
                resultQueue.add(child, childScore);
            }
        }

        @Override
        public boolean hasNext() {
            return resultQueue.size() > 0;
        }

        @Override
        public PostingMetadata nextPosting() throws IOException {
            long centroidOrdinalAndScore = resultQueue.popRaw();
            int centroidOrd = resultQueue.decodeNodeId(centroidOrdinalAndScore);
            float score = resultQueue.decodeScore(centroidOrdinalAndScore);
            return leafReader.readPostingMetadata(centroidOrd, score);
        }

        /**
         * Continue the beam search by expanding the next best unexplored parent.
         * This is intended for exceptional cases where the initial beam didn't produce enough centroids.
         * @return true if new centroids were added to the result queue, false if no more parents to explore
         */
        public boolean continueSearch() throws IOException {
            if (parentsQueue.size() == 0) {
                return false;
            }
            expandNode(parentsQueue.pop());
            return resultQueue.size() > 0;
        }
    }

    private static void score(
        NeighborQueue neighborQueue,
        int size,
        int scoresOffset,
        ES92Int7VectorsScorer scorer,
        IndexInput centroids,
        long centroidQuantizeSize,
        byte[] quantizeQuery,
        OptimizedScalarQuantizer.QuantizationResult queryCorrections,
        float centroidDp,
        VectorSimilarityFunction similarityFunction,
        float[] scores,
        FixedBitSet acceptCentroids,
        int bulkSize,
        CentroidSearchStats searchStats
    ) throws IOException {
        int limit = size - bulkSize + 1;
        int i = 0;
        for (; i < limit; i += bulkSize) {
            if (acceptCentroids == null || acceptCentroids.cardinality(scoresOffset + i, scoresOffset + i + bulkSize) > 0) {
                if (searchStats != null) {
                    searchStats.recordScoredBlock(bulkSize);
                }
                scorer.scoreBulk(
                    quantizeQuery,
                    queryCorrections.lowerInterval(),
                    queryCorrections.upperInterval(),
                    queryCorrections.quantizedComponentSum(),
                    queryCorrections.additionalCorrection(),
                    similarityFunction,
                    centroidDp,
                    scores,
                    bulkSize
                );
                for (int j = 0; j < bulkSize; j++) {
                    int centroidOrd = scoresOffset + i + j;
                    if (acceptCentroids == null || acceptCentroids.get(centroidOrd)) {
                        neighborQueue.add(centroidOrd, scores[j]);
                    }
                }
            } else {
                centroids.skipBytes(bulkSize * centroidQuantizeSize);
            }
        }

        int tailBulkSize = size - i;
        if (tailBulkSize > 0) {
            if (acceptCentroids == null || acceptCentroids.cardinality(scoresOffset + i, scoresOffset + i + tailBulkSize) > 0) {
                if (searchStats != null) {
                    searchStats.recordScoredBlock(tailBulkSize);
                }
                scorer.scoreBulk(
                    quantizeQuery,
                    queryCorrections.lowerInterval(),
                    queryCorrections.upperInterval(),
                    queryCorrections.quantizedComponentSum(),
                    queryCorrections.additionalCorrection(),
                    similarityFunction,
                    centroidDp,
                    scores,
                    tailBulkSize
                );
                for (int j = 0; j < tailBulkSize; j++) {
                    int centroidOrd = scoresOffset + i + j;
                    if (acceptCentroids == null || acceptCentroids.get(centroidOrd)) {
                        neighborQueue.add(centroidOrd, scores[j]);
                    }
                }
            } else {
                centroids.skipBytes(tailBulkSize * centroidQuantizeSize);
            }
        }

    }

    private static class CentroidSearchStats {
        private long nodesVisited;
        private long blocksVisited;

        void recordScoredBlock(int blockSize) {
            blocksVisited++;
            nodesVisited += blockSize;
        }

        long nodesVisited() {
            return nodesVisited;
        }

        long blocksVisited() {
            return blocksVisited;
        }
    }

    private static class OffHeapCentroidQueryScorer implements RandomVectorScorer {
        private final IndexInput quantizedCentroids;
        private final byte[] quantizedQuery;
        private final OptimizedScalarQuantizer.QuantizationResult queryParams;
        private final VectorSimilarityFunction similarityFunction;
        private final float globalCentroidDp;
        private final int size;
        private final int fullBlockCount;
        private final int tailBlockCount;
        private final long fullBlockByteSize;
        private final float[] blockScores = new float[CENTROID_BULK_SIZE];
        private final ES92Int7VectorsScorer scorer;
        private final CentroidSearchStats searchStats;
        private int cachedBlock = -1;
        private int cachedBlockSize = 0;

        OffHeapCentroidQueryScorer(
            int dimension,
            int size,
            IndexInput quantizedCentroids,
            byte[] quantizedQuery,
            OptimizedScalarQuantizer.QuantizationResult queryParams,
            VectorSimilarityFunction similarityFunction,
            float globalCentroidDp,
            CentroidSearchStats searchStats
        ) throws IOException {
            this.quantizedCentroids = quantizedCentroids;
            this.quantizedQuery = quantizedQuery;
            this.queryParams = queryParams;
            this.similarityFunction = similarityFunction;
            this.globalCentroidDp = globalCentroidDp;
            this.size = size;
            this.searchStats = searchStats;
            this.fullBlockCount = size / CENTROID_BULK_SIZE;
            this.tailBlockCount = size % CENTROID_BULK_SIZE;
            this.fullBlockByteSize = (long) CENTROID_BULK_SIZE * (dimension + 4L * Integer.BYTES);
            this.scorer = ESVectorUtil.getES92Int7VectorsScorer(quantizedCentroids, dimension, CENTROID_BULK_SIZE);
        }

        @Override
        public float score(int node) throws IOException {
            final int block = node / CENTROID_BULK_SIZE;
            if (block != cachedBlock) {
                cachedBlock = block;
                cachedBlockSize = blockVectorCount(block);
                if (searchStats != null) {
                    searchStats.recordScoredBlock(cachedBlockSize);
                }
                quantizedCentroids.seek(blockStartOffset(block));
                scorer.scoreBulk(
                    quantizedQuery,
                    queryParams.lowerInterval(),
                    queryParams.upperInterval(),
                    queryParams.quantizedComponentSum(),
                    queryParams.additionalCorrection(),
                    similarityFunction,
                    globalCentroidDp,
                    blockScores,
                    cachedBlockSize
                );
            }
            return blockScores[node % CENTROID_BULK_SIZE];
        }

        @Override
        public int maxOrd() {
            return size;
        }

        private int blockVectorCount(int block) {
            if (block < fullBlockCount) {
                return CENTROID_BULK_SIZE;
            }
            return tailBlockCount;
        }

        private long blockStartOffset(int block) {
            if (block < fullBlockCount) {
                return block * fullBlockByteSize;
            }
            return (long) fullBlockCount * fullBlockByteSize;
        }
    }

    private static class OffHeapHnswGraph extends HnswGraph {
        private final int numCentroids;
        private final int numLevels;
        private final int maxConn;
        private final int[][] nodesByLevel;
        private final long[] levelBaseOffsets;
        private final LongValues nodeOffsets;
        private final IndexInput graphData;
        private int[] currentNeighbors = new int[0];
        private int currentNeighborCount = 0;
        private int currentNeighborOrd = 0;

        OffHeapHnswGraph(NextFieldEntry fieldEntry, int numCentroids, IndexInput ivfCentroids) throws IOException {
            this.numCentroids = numCentroids;
            this.numLevels = fieldEntry.centroidGraphNumLevels;
            this.maxConn = fieldEntry.centroidGraphMaxConn;
            this.nodesByLevel = fieldEntry.centroidGraphNodesByLevel;
            this.graphData = ivfCentroids.slice("centroid-graph-data", fieldEntry.centroidIndexOffset, fieldEntry.centroidIndexLength);
            this.nodeOffsets = DirectReader.getInstance(
                ivfCentroids.randomAccessSlice(fieldEntry.centroidGraphOffsetsDataOffset, fieldEntry.centroidGraphOffsetsDataLength),
                fieldEntry.centroidGraphOffsetsBitsPerValue
            );
            this.levelBaseOffsets = new long[numLevels];
            long base = 0L;
            for (int level = 0; level < numLevels; level++) {
                levelBaseOffsets[level] = base;
                base += levelNodeCount(level);
            }
        }

        boolean isEmpty() {
            return numLevels == 0 || numCentroids == 0;
        }

        @Override
        public void seek(int level, int target) throws IOException {
            currentNeighbors = readNeighbors(level, target);
            currentNeighborCount = currentNeighbors.length;
            currentNeighborOrd = 0;
        }

        @Override
        public int size() {
            return numCentroids;
        }

        @Override
        public int nextNeighbor() {
            if (currentNeighborOrd >= currentNeighborCount) {
                return NO_MORE_DOCS;
            }
            return currentNeighbors[currentNeighborOrd++];
        }

        @Override
        public int numLevels() {
            return numLevels;
        }

        @Override
        public int maxConn() {
            return maxConn;
        }

        @Override
        public int entryNode() {
            if (numLevels <= 1) {
                return numCentroids == 0 ? -1 : 0;
            }
            int[] topLevelNodes = nodesByLevel[numLevels - 1];
            return topLevelNodes.length == 0 ? 0 : topLevelNodes[0];
        }

        @Override
        public NodesIterator getNodesOnLevel(int level) {
            if (level == 0) {
                return new ArrayNodesIterator(numCentroids);
            }
            return new ArrayNodesIterator(nodesByLevel[level], nodesByLevel[level].length);
        }

        @Override
        public int neighborCount() {
            return currentNeighborCount;
        }

        private int[] readNeighbors(int level, int node) throws IOException {
            long index = nodeIndex(level, node);
            if (index < 0) {
                return new int[0];
            }
            final long offset = nodeOffsets.get(index);
            graphData.seek(offset);
            int size = graphData.readVInt();
            if (size == 0) {
                return new int[0];
            }
            int[] deltas = new int[size];
            GroupVIntUtil.readGroupVInts(graphData, deltas, size);
            for (int i = 1; i < size; i++) {
                deltas[i] += deltas[i - 1];
            }
            return deltas;
        }

        private long nodeIndex(int level, int node) {
            if (level == 0) {
                return levelBaseOffsets[0] + node;
            }
            int position = Arrays.binarySearch(nodesByLevel[level], node);
            if (position < 0) {
                return -1L;
            }
            return levelBaseOffsets[level] + position;
        }

        private int levelNodeCount(int level) {
            if (level == 0) {
                return numCentroids;
            }
            return nodesByLevel[level].length;
        }
    }

    @Override
    public PostingVisitor getPostingVisitor(
        FieldInfo fieldInfo,
        IndexInput indexInput,
        float[] target,
        Bits acceptDocs,
        IndexInput centroidSlice
    ) throws IOException {
        FieldEntry entry = fields.get(fieldInfo.number);
        final int bitsRequired = DirectWriter.bitsRequired(entry.numCentroids());
        final long sizeLookup = directWriterSizeOnDisk(
            getReaderForField(fieldInfo.name).getFloatVectorValues(fieldInfo.name).size(),
            bitsRequired
        );
        centroidSlice.skipBytes(sizeLookup);
        ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding = ((NextFieldEntry) entry).quantEncoding();
        int numParents = centroidSlice.readVInt();
        final QueryQuantizer queryQuantizer;
        if (numParents > 0) {
            IndexInput parentsSlice = centroidSlice.slice(
                "parents-slice",
                centroidSlice.getFilePointer(),
                (long) numParents * fieldInfo.getVectorDimension() * Float.BYTES
            );
            queryQuantizer = new QueryQuantizer(quantEncoding, fieldInfo, target, parentsSlice, entry.globalCentroid());
        } else {
            queryQuantizer = new QueryQuantizer(quantEncoding, fieldInfo, target, null, entry.globalCentroid());
        }

        return new MemorySegmentPostingsVisitor(queryQuantizer, quantEncoding, indexInput, entry, fieldInfo, acceptDocs);
    }

    private record QueryQuantizerResult(OptimizedScalarQuantizer.QuantizationResult queryCorrections, byte[] quantizedTarget) {}

    private static class QueryQuantizer {
        private final Cache<Integer, QueryQuantizerResult> cache;
        private final ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding;
        private final float[] target;
        private final float[] scratch;
        private final int[] quantizationScratch;
        private final OptimizedScalarQuantizer quantizer;
        private final IndexInput parentsSlice;
        private final float[] globalCentroid;
        private final float[] centroidScratch;
        private int currentCentroidOrdinal = -2;
        private int nextCentroidOrdinal = -1;
        private byte[] evictedQuantizedQuery = null;
        private QueryQuantizerResult result = null;

        QueryQuantizer(
            ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding,
            FieldInfo fieldInfo,
            float[] target,
            IndexInput parentsSlice,
            float[] globalCentroid
        ) {
            this.quantEncoding = quantEncoding;
            this.target = target;
            this.scratch = new float[fieldInfo.getVectorDimension()];
            this.centroidScratch = new float[fieldInfo.getVectorDimension()];
            this.quantizationScratch = new int[quantEncoding.discretizedDimensions(fieldInfo.getVectorDimension())];
            this.quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction(), DEFAULT_LAMBDA, 1);
            this.parentsSlice = parentsSlice;
            this.globalCentroid = globalCentroid;
            this.cache = CacheBuilder.<Integer, QueryQuantizerResult>builder()
                .weigher((k, v) -> 1L)
                .setMaximumWeight(16)
                .removalListener(n -> {
                    evictedQuantizedQuery = n.getValue().quantizedTarget();
                })
                .build();
        }

        void reset(int centroidOrdinal) {
            this.nextCentroidOrdinal = centroidOrdinal;
        }

        void quantizeQueryIfNecessary() throws IOException {
            if (nextCentroidOrdinal != currentCentroidOrdinal) {
                var quantized = cache.get(nextCentroidOrdinal);
                if (quantized != null) {
                    result = quantized;
                    currentCentroidOrdinal = nextCentroidOrdinal;
                    return;
                }
                // reuse the evicted byte array to reduce allocations
                final byte[] quantizedQuery = Objects.requireNonNullElseGet(
                    evictedQuantizedQuery,
                    () -> new byte[quantEncoding.getQueryPackedLength(target.length)]
                );
                final float[] queryCentroid;
                if (parentsSlice != null) {
                    assert nextCentroidOrdinal >= 0;
                    parentsSlice.seek((long) nextCentroidOrdinal * centroidScratch.length * Float.BYTES);
                    parentsSlice.readFloats(centroidScratch, 0, centroidScratch.length);
                    queryCentroid = centroidScratch;
                } else {
                    assert nextCentroidOrdinal == NO_ORDINAL;
                    queryCentroid = globalCentroid;
                }
                OptimizedScalarQuantizer.QuantizationResult queryCorrections = quantizer.scalarQuantize(
                    target,
                    scratch,
                    quantizationScratch,
                    quantEncoding.queryBits(),
                    queryCentroid
                );
                quantEncoding.packQuery(quantizationScratch, quantizedQuery);
                currentCentroidOrdinal = nextCentroidOrdinal;
                result = new QueryQuantizerResult(queryCorrections, quantizedQuery);
                cache.put(nextCentroidOrdinal, result);
            }
        }

        OptimizedScalarQuantizer.QuantizationResult getQueryCorrections() {
            return result.queryCorrections();
        }

        byte[] getQuantizedTarget() {
            return result.quantizedTarget();
        }
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        // TODO: override if adding new files
        return super.getOffHeapByteSize(fieldInfo);
    }

    private static class MemorySegmentPostingsVisitor implements PostingVisitor {
        final long quantizedByteLength;
        final IndexInput indexInput;
        final FieldEntry entry;
        final FieldInfo fieldInfo;
        final Bits acceptDocs;
        private final ESNextOSQVectorsScorer osqVectorsScorer;
        final float[] scores = new float[BULK_SIZE];
        final float[] correctionsLower = new float[BULK_SIZE];
        final float[] correctionsUpper = new float[BULK_SIZE];
        final int[] correctionsSum = new int[BULK_SIZE];
        final float[] correctionsAdd = new float[BULK_SIZE];
        final int[] docIdsScratch = new int[BULK_SIZE];
        byte docEncoding;
        int docBase = 0;

        int vectors;
        float centroidToParentSqDist;
        float centroidDistance;
        long slicePos;

        private final QueryQuantizer queryQuantizer;
        final DocIdsWriter idsWriter = new DocIdsWriter();
        final VectorSimilarityFunction similarityFunction;
        final float[] correctiveValues = new float[3];
        final long quantizedVectorByteSize;

        MemorySegmentPostingsVisitor(
            QueryQuantizer queryQuantizer,
            ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding,
            IndexInput indexInput,
            FieldEntry entry,
            FieldInfo fieldInfo,
            Bits acceptDocs
        ) throws IOException {
            this.queryQuantizer = queryQuantizer;
            this.indexInput = indexInput;
            this.similarityFunction = fieldInfo.getVectorSimilarityFunction();
            this.entry = entry;
            this.fieldInfo = fieldInfo;
            this.acceptDocs = acceptDocs;
            quantizedVectorByteSize = quantEncoding.getDocPackedLength(fieldInfo.getVectorDimension());
            quantizedByteLength = quantizedVectorByteSize + (Float.BYTES * 3) + Integer.BYTES;
            osqVectorsScorer = ESVectorUtil.getESNextOSQVectorsScorer(
                indexInput,
                quantEncoding.queryBits(),
                quantEncoding.bits(),
                fieldInfo.getVectorDimension(),
                (int) quantizedVectorByteSize,
                BULK_SIZE
            );
        }

        @Override
        public int resetPostingsScorer(PostingMetadata metadata) throws IOException {
            float score = metadata.documentCentroidScore();
            indexInput.seek(metadata.offset());
            centroidToParentSqDist = Float.intBitsToFloat(indexInput.readInt());
            vectors = indexInput.readVInt();
            docEncoding = indexInput.readByte();
            docBase = 0;
            slicePos = indexInput.getFilePointer();
            // The score is the transformed score used when searching the centroids.
            // we need to convert it back to the raw similarity to be used as part of
            // final corrections
            centroidDistance = switch (similarityFunction) {
                case EUCLIDEAN -> ((1 / score) - 1) - centroidToParentSqDist;
                case COSINE, DOT_PRODUCT -> 2 * score - 1;
                case MAXIMUM_INNER_PRODUCT -> score - 1;
            };
            queryQuantizer.reset(metadata.queryCentroidOrdinal());
            return vectors;
        }

        private float scoreIndividually(int bulkSize) throws IOException {
            float maxScore = Float.NEGATIVE_INFINITY;
            // score individually, first the quantized byte chunk
            for (int j = 0; j < bulkSize; j++) {
                int doc = docIdsScratch[j];
                if (doc != -1) {
                    float qcDist = osqVectorsScorer.quantizeScore(queryQuantizer.getQuantizedTarget());
                    scores[j] = qcDist;
                } else {
                    indexInput.skipBytes(quantizedVectorByteSize);
                }
            }
            // read in all corrections
            indexInput.readFloats(correctionsLower, 0, bulkSize);
            indexInput.readFloats(correctionsUpper, 0, bulkSize);
            for (int j = 0; j < bulkSize; j++) {
                correctionsSum[j] = indexInput.readInt();
            }
            indexInput.readFloats(correctionsAdd, 0, bulkSize);
            // Now apply corrections
            for (int j = 0; j < bulkSize; j++) {
                int doc = docIdsScratch[j];
                if (doc != -1) {
                    scores[j] = osqVectorsScorer.score(
                        queryQuantizer.getQueryCorrections().lowerInterval(),
                        queryQuantizer.getQueryCorrections().upperInterval(),
                        queryQuantizer.getQueryCorrections().quantizedComponentSum(),
                        centroidDistance,
                        fieldInfo.getVectorSimilarityFunction(),
                        0,
                        correctionsLower[j],
                        correctionsUpper[j],
                        correctionsSum[j],
                        correctionsAdd[j],
                        scores[j]
                    );
                    if (scores[j] > maxScore) {
                        maxScore = scores[j];
                    }
                }
            }
            return maxScore;
        }

        private static int docToBulkScore(int[] docIds, Bits acceptDocs, int bulkSize) {
            assert acceptDocs != null : "acceptDocs must not be null";
            int docToScore = bulkSize;
            for (int i = 0; i < bulkSize; i++) {
                if (acceptDocs.get(docIds[i]) == false) {
                    docIds[i] = -1;
                    docToScore--;
                }
            }
            return docToScore;
        }

        private void collectBulk(KnnCollector knnCollector, float[] scores, int bulkSize) {
            for (int i = 0; i < bulkSize; i++) {
                final int doc = docIdsScratch[i];
                if (doc != -1) {
                    knnCollector.collect(doc, scores[i]);
                }
            }
        }

        private void readDocIds(int count) throws IOException {
            idsWriter.readInts(indexInput, count, docEncoding, docIdsScratch);
            // reconstitute from the deltas
            for (int j = 0; j < count; j++) {
                docBase += docIdsScratch[j];
                docIdsScratch[j] = docBase;
            }
        }

        @Override
        public int visit(KnnCollector knnCollector) throws IOException {
            indexInput.seek(slicePos);
            // block processing
            int scoredDocs = 0;
            int limit = vectors - BULK_SIZE + 1;
            int i = 0;
            // read Docs
            for (; i < limit; i += BULK_SIZE) {
                // read the doc ids
                readDocIds(BULK_SIZE);
                final int docsToBulkScore = acceptDocs == null ? BULK_SIZE : docToBulkScore(docIdsScratch, acceptDocs, BULK_SIZE);
                if (docsToBulkScore == 0) {
                    indexInput.skipBytes(quantizedByteLength * BULK_SIZE);
                    continue;
                }
                queryQuantizer.quantizeQueryIfNecessary();
                final float maxScore;
                if (docsToBulkScore < BULK_SIZE / 2) {
                    maxScore = scoreIndividually(BULK_SIZE);
                } else {
                    maxScore = osqVectorsScorer.scoreBulk(
                        queryQuantizer.getQuantizedTarget(),
                        queryQuantizer.getQueryCorrections().lowerInterval(),
                        queryQuantizer.getQueryCorrections().upperInterval(),
                        queryQuantizer.getQueryCorrections().quantizedComponentSum(),
                        centroidDistance,
                        fieldInfo.getVectorSimilarityFunction(),
                        0f,
                        scores
                    );
                }
                if (knnCollector.minCompetitiveSimilarity() < maxScore) {
                    collectBulk(knnCollector, scores, BULK_SIZE);
                }
                scoredDocs += docsToBulkScore;
            }
            // bulk process tail
            if (i < vectors) {
                int tailSize = vectors - i;
                readDocIds(tailSize);
                final int docsToBulkScore = acceptDocs == null ? tailSize : docToBulkScore(docIdsScratch, acceptDocs, tailSize);
                if (docsToBulkScore == 0) {
                    indexInput.skipBytes(quantizedByteLength * tailSize);
                } else {
                    queryQuantizer.quantizeQueryIfNecessary();
                    final float maxScore;
                    if (docsToBulkScore < tailSize / 2) {
                        maxScore = scoreIndividually(tailSize);
                    } else {
                        maxScore = osqVectorsScorer.scoreBulk(
                            queryQuantizer.getQuantizedTarget(),
                            queryQuantizer.getQueryCorrections().lowerInterval(),
                            queryQuantizer.getQueryCorrections().upperInterval(),
                            queryQuantizer.getQueryCorrections().quantizedComponentSum(),
                            centroidDistance,
                            fieldInfo.getVectorSimilarityFunction(),
                            0f,
                            scores,
                            tailSize
                        );
                    }
                    if (knnCollector.minCompetitiveSimilarity() < maxScore) {
                        collectBulk(knnCollector, scores, tailSize);
                    }
                    scoredDocs += docsToBulkScore;
                }
            }
            if (scoredDocs > 0) {
                knnCollector.incVisitedCount(scoredDocs);
            }
            return scoredDocs;
        }
    }

}
