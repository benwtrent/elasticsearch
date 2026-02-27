/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues;
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
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.GroupVIntUtil;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
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
import org.elasticsearch.simdvec.ESNextOSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.VectorScorerFactory;

import java.io.IOException;
import java.util.ArrayList;
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
    private static final float GRAPH_CENTROID_OVERSAMPLE_MULTIPLIER = 1.5f;

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
        float approximateDocsPerCentroid = approximateCost / numCentroids;
        if (approximateDocsPerCentroid <= 1.25) {
            // TODO: we need to make this call to build the iterator, otherwise accept docs breaks all together
            approximateDocsPerCentroid = (float) acceptDocs.cost() / numCentroids;
        }
        final int bitsRequired = DirectWriter.bitsRequired(numCentroids);
        final long sizeLookup = directWriterSizeOnDisk(values.size(), bitsRequired);
        final long fp = centroids.getFilePointer();
        final FixedBitSet acceptCentroids;
        int filteredCentroidCount = 0;
        if (approximateDocsPerCentroid > 1.25 || numCentroids == 1) {
            // only apply centroid filtering when we expect some / many centroids will not have
            // any matching document.
            acceptCentroids = null;
            filteredCentroidCount = numCentroids;
        } else {
            acceptCentroids = new FixedBitSet(numCentroids);
            final KnnVectorValues.DocIndexIterator docIndexIterator = values.iterator();
            final DocIdSetIterator iterator = ConjunctionUtils.intersectIterators(List.of(acceptDocs.iterator(), docIndexIterator));
            final LongValues longValues = DirectReader.getInstance(centroids.randomAccessSlice(fp, sizeLookup), bitsRequired);
            int doc = iterator.nextDoc();
            for (; doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                final int centroidOrd = (int) longValues.get(docIndexIterator.index());
                if (acceptCentroids.getAndSet(centroidOrd) == false) {
                    filteredCentroidCount++;
                }
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
                numCentroids,
                targetQuery,
                quantized,
                queryParams,
                fieldEntry.globalCentroidDp(),
                acceptCentroids,
                filteredCentroidCount,
                visitRatio,
                fieldEntry.globalCentroid()
            )
            : getCentroidIteratorFlat(
                fieldInfo,
                centroids,
                quantizedStart,
                numCentroids,
                acceptCentroids,
                filteredCentroidCount,
                createInt7uCentroidScorer(
                    fieldInfo,
                    centroids,
                    quantizedStart,
                    numCentroids,
                    targetQuery,
                    quantized,
                    queryParams,
                    fieldEntry.globalCentroid(),
                    fieldEntry.globalCentroidDp()
                )
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
            centroidGraphNumLevels = input.readVInt();
            centroidGraphMaxConn = input.readVInt();
            centroidGraphNodesByLevel = new int[centroidGraphNumLevels][];
            centroidGraphNodesByLevel[0] = new int[0];
            for (int level = 1; level < centroidGraphNumLevels; level++) {
                final int count = input.readVInt();
                final int[] nodes = new int[count];
                int previous = 0;
                for (int i = 0; i < count; i++) {
                    previous += input.readVInt();
                    nodes[i] = previous;
                }
                centroidGraphNodesByLevel[level] = nodes;
            }
            centroidGraphValueCount = input.readVLong();
            centroidGraphOffsetsBitsPerValue = input.readVInt();
            centroidGraphOffsetsDataOffset = input.readVLong();
            centroidGraphOffsetsDataLength = input.readVLong();
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
            return centroidIndexLength > 0 && centroidGraphValueCount > 0 && centroidGraphOffsetsBitsPerValue > 0;
        }
    }

    private CentroidIterator getCentroidIteratorGraph(
        FieldInfo fieldInfo,
        IndexInput centroids,
        NextFieldEntry fieldEntry,
        long quantizedStart,
        int numCentroids,
        float[] targetQuery,
        byte[] quantizeQuery,
        OptimizedScalarQuantizer.QuantizationResult queryParams,
        float globalCentroidDp,
        FixedBitSet acceptCentroids,
        int filteredCentroidCount,
        float visitRatio,
        float[] globalCentroid
    ) throws IOException {
        final OffHeapHnswGraph graph = new OffHeapHnswGraph(fieldEntry, numCentroids, ivfCentroids);
        final RandomVectorScorer centroidScorer = createInt7uCentroidScorer(
            fieldInfo,
            centroids,
            quantizedStart,
            numCentroids,
            targetQuery,
            quantizeQuery,
            queryParams,
            globalCentroid,
            globalCentroidDp
        );
        final int desiredCentroids = Math.max(1, Math.min(numCentroids, (int) Math.ceil(numCentroids * visitRatio)));
        final int gatheredCentroids = Math.max(
            desiredCentroids,
            Math.min(numCentroids, (int) Math.ceil(desiredCentroids * GRAPH_CENTROID_OVERSAMPLE_MULTIPLIER))
        );
        if (graph.isEmpty() || filteredCentroidCount <= gatheredCentroids) {
            return getCentroidIteratorFlat(
                fieldInfo,
                centroids,
                quantizedStart,
                numCentroids,
                acceptCentroids,
                filteredCentroidCount,
                centroidScorer
            );
        }
        final TopKnnCollector collector = new TopKnnCollector(gatheredCentroids, Integer.MAX_VALUE);
        HnswGraphSearcher.search(centroidScorer, collector, graph, acceptCentroids, filteredCentroidCount);
        final ScoreDoc[] scoreDocs = collector.topDocs().scoreDocs;
        logger.debug(
            "graph centroid search stats [field={}, centroids={}, acceptedCentroids={}, desiredCentroids={}, gatheredCentroids={}, returnedCentroids={}]",
            fieldInfo.name,
            numCentroids,
            filteredCentroidCount,
            desiredCentroids,
            gatheredCentroids,
            scoreDocs.length
        );
        if (scoreDocs.length == 0) {
            return getCentroidIteratorFlat(
                fieldInfo,
                centroids,
                quantizedStart,
                numCentroids,
                acceptCentroids,
                filteredCentroidCount,
                centroidScorer
            );
        }
        final long postingsOffset = quantizedStart + (long) numCentroids * (fieldInfo.getVectorDimension() + 3L * Float.BYTES
            + Integer.BYTES);
        return new GraphCentroidIterator(
            centroids,
            postingsOffset,
            numCentroids,
            gatheredCentroids,
            centroidScorer,
            graph,
            acceptCentroids,
            filteredCentroidCount,
            scoreDocs
        );
    }

    private static CentroidIterator getCentroidIteratorFlat(
        FieldInfo fieldInfo,
        IndexInput centroids,
        long quantizedStart,
        int numCentroids,
        FixedBitSet acceptCentroids,
        int filteredCentroidCount,
        RandomVectorScorer centroidScorer
    ) throws IOException {
        // Quantized centroid records start immediately after optional raw parent centroids.
        // Seek explicitly so fallback paths don't depend on caller-side cursor position invariants.
        centroids.seek(quantizedStart);
        final NeighborQueue neighborQueue = new NeighborQueue(numCentroids, true);
        for (int ord = 0; ord < numCentroids; ord++) {
            if (acceptCentroids == null || acceptCentroids.get(ord)) {
                neighborQueue.add(ord, centroidScorer.score(ord));
            }
        }
        logger.debug(
            "flat centroid search stats [field={}, centroids={}, acceptedCentroids={}]",
            fieldInfo.name,
            numCentroids,
            filteredCentroidCount
        );
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

    private static final class GraphCentroidIterator implements CentroidIterator {
        private final IndexInput centroids;
        private final long postingsOffset;
        private final int numCentroids;
        private final int gatheredCentroids;
        private final RandomVectorScorer centroidScorer;
        private final OffHeapHnswGraph graph;
        private final FixedBitSet acceptCentroids;
        private ScoreDoc[] currentScoreDocs;
        private int scoreDocIdx = 0;
        private int remainingAcceptedCentroids;
        private int lastReturnedCentroidOrd = -1;

        GraphCentroidIterator(
            IndexInput centroids,
            long postingsOffset,
            int numCentroids,
            int gatheredCentroids,
            RandomVectorScorer centroidScorer,
            OffHeapHnswGraph graph,
            FixedBitSet acceptCentroids,
            int filteredCentroidCount,
            ScoreDoc[] initialScoreDocs
        ) {
            this.centroids = centroids;
            this.postingsOffset = postingsOffset;
            this.numCentroids = numCentroids;
            this.gatheredCentroids = gatheredCentroids;
            this.centroidScorer = centroidScorer;
            this.graph = graph;
            this.acceptCentroids = acceptCentroids;
            this.remainingAcceptedCentroids = filteredCentroidCount;
            this.currentScoreDocs = initialScoreDocs;
        }

        @Override
        public boolean hasNext() {
            try {
                while (scoreDocIdx >= currentScoreDocs.length && remainingAcceptedCentroids > 0) {
                    if (acceptCentroids == null) {
                        return false;
                    }
                    if (remainingAcceptedCentroids <= graph.maxConn()) {
                        currentScoreDocs = scoreAcceptedCentroids(numCentroids, acceptCentroids, centroidScorer);
                        scoreDocIdx = 0;
                        if (currentScoreDocs.length == 0) {
                            remainingAcceptedCentroids = 0;
                        }
                        break;
                    }
                    final int continueK = Math.min(gatheredCentroids, remainingAcceptedCentroids);
                    final KnnSearchStrategy continueSearchStrategy = lastReturnedCentroidOrd >= 0
                        ? new KnnSearchStrategy.Seeded(
                            new SingleOrdDocIdSetIterator(lastReturnedCentroidOrd),
                            1,
                            KnnSearchStrategy.Hnsw.DEFAULT
                        )
                        : KnnSearchStrategy.Hnsw.DEFAULT;
                    final TopKnnCollector continueCollector = new TopKnnCollector(continueK, Integer.MAX_VALUE, continueSearchStrategy);
                    HnswGraphSearcher.search(centroidScorer, continueCollector, graph, acceptCentroids, remainingAcceptedCentroids);
                    currentScoreDocs = continueCollector.topDocs().scoreDocs;
                    scoreDocIdx = 0;
                    if (currentScoreDocs.length == 0) {
                        currentScoreDocs = scoreAcceptedCentroids(numCentroids, acceptCentroids, centroidScorer);
                        scoreDocIdx = 0;
                        if (currentScoreDocs.length == 0) {
                            remainingAcceptedCentroids = 0;
                        }
                        break;
                    }
                }
                return scoreDocIdx < currentScoreDocs.length;
            } catch (IOException e) {
                throw new RuntimeException("failed to continue centroid search", e);
            }
        }

        @Override
        public PostingMetadata nextPosting() throws IOException {
            ScoreDoc scoreDoc = currentScoreDocs[scoreDocIdx++];
            int centroidOrd = scoreDoc.doc;
            float score = scoreDoc.score;
            if (acceptCentroids != null && acceptCentroids.getAndClear(centroidOrd)) {
                remainingAcceptedCentroids--;
            }
            lastReturnedCentroidOrd = centroidOrd;
            centroids.seek(postingsOffset + (Long.BYTES * 2L + Integer.BYTES) * centroidOrd);
            long postingListOffset = centroids.readLong();
            long postingListLength = centroids.readLong();
            int parentOrd = centroids.readInt();
            return new PostingMetadata(postingListOffset, postingListLength, parentOrd, score);
        }
    }

    private static ScoreDoc[] scoreAcceptedCentroids(int numCentroids, FixedBitSet acceptCentroids, RandomVectorScorer centroidScorer)
        throws IOException {
        final NeighborQueue neighborQueue = new NeighborQueue(numCentroids, true);
        for (int ord = 0; ord < numCentroids; ord++) {
            if (acceptCentroids.get(ord)) {
                neighborQueue.add(ord, centroidScorer.score(ord));
            }
        }
        final List<ScoreDoc> scoreDocs = new ArrayList<>(neighborQueue.size());
        while (neighborQueue.size() > 0) {
            long centroidOrdinalAndScore = neighborQueue.popRaw();
            int centroidOrd = neighborQueue.decodeNodeId(centroidOrdinalAndScore);
            float score = neighborQueue.decodeScore(centroidOrdinalAndScore);
            scoreDocs.add(new ScoreDoc(centroidOrd, score));
        }
        return scoreDocs.toArray(ScoreDoc[]::new);
    }

    private static final class SingleOrdDocIdSetIterator extends DocIdSetIterator {
        private final int ord;
        private int doc = -1;

        private SingleOrdDocIdSetIterator(int ord) {
            this.ord = ord;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            if (doc == -1) {
                doc = ord;
                return doc;
            }
            doc = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
            if (doc == NO_MORE_DOCS) {
                return NO_MORE_DOCS;
            }
            if (target <= ord) {
                doc = ord;
                return ord;
            }
            doc = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return 1;
        }
    }

    private RandomVectorScorer createInt7uCentroidScorer(
        FieldInfo fieldInfo,
        IndexInput centroids,
        long quantizedStart,
        int numCentroids,
        float[] targetQuery,
        byte[] quantizedQuery,
        OptimizedScalarQuantizer.QuantizationResult queryParams,
        float[] globalCentroid,
        float globalCentroidDp
    ) throws IOException {
        final int dimension = fieldInfo.getVectorDimension();
        final long recordByteSize = (long) dimension + 3L * Float.BYTES + Integer.BYTES;
        final IndexInput quantizedSlice = centroids.slice("quantized-centroids", quantizedStart, recordByteSize * numCentroids);
        final DenseOffHeapCentroidQuantizedValues quantizedValues = new DenseOffHeapCentroidQuantizedValues(
            dimension,
            numCentroids,
            fieldInfo.getVectorSimilarityFunction(),
            quantizedSlice,
            globalCentroid,
            globalCentroidDp
        );
        final VectorScorerFactory factory = VectorScorerFactory.instance().orElse(null);
        if (factory != null) {
            final var scorer = factory.getInt7uOSQVectorScorer(
                fieldInfo.getVectorSimilarityFunction(),
                quantizedValues,
                quantizedQuery,
                queryParams.lowerInterval(),
                queryParams.upperInterval(),
                queryParams.additionalCorrection(),
                queryParams.quantizedComponentSum()
            );
            if (scorer.isPresent()) {
                return scorer.get();
            }
        }
        return new Lucene104ScalarQuantizedVectorScorer(new EmptyFlatVectorsScorer()).getRandomVectorScorer(
            fieldInfo.getVectorSimilarityFunction(),
            quantizedValues,
            targetQuery
        );
    }

    private static final class EmptyFlatVectorsScorer implements FlatVectorsScorer {
        @Override
        public RandomVectorScorerSupplier getRandomVectorScorerSupplier(VectorSimilarityFunction sim, KnnVectorValues values) {
            throw new IllegalStateException("Unexpected call for quantized centroid values");
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(VectorSimilarityFunction sim, KnnVectorValues values, float[] query) {
            throw new IllegalStateException("Unexpected call for quantized centroid values");
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(VectorSimilarityFunction sim, KnnVectorValues values, byte[] query) {
            throw new IllegalStateException("Unexpected call for quantized centroid values");
        }
    }

    private static class DenseOffHeapCentroidQuantizedValues extends QuantizedByteVectorValues {
        private final int dimension;
        private final int size;
        private final VectorSimilarityFunction similarityFunction;
        private final IndexInput slice;
        private final byte[] vectorValue;
        private final float[] correctiveValues = new float[3];
        private final float[] centroid;
        private final float centroidDp;
        private int quantizedComponentSum;
        private int lastOrd = -1;

        private DenseOffHeapCentroidQuantizedValues(
            int dimension,
            int size,
            VectorSimilarityFunction similarityFunction,
            IndexInput slice,
            float[] centroid,
            float centroidDp
        ) {
            this.dimension = dimension;
            this.size = size;
            this.similarityFunction = similarityFunction;
            this.slice = slice;
            this.centroid = centroid;
            this.centroidDp = centroidDp;
            this.vectorValue = new byte[dimension];
        }

        @Override
        public IndexInput getSlice() {
            return slice;
        }

        @Override
        public org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int vectorOrd)
            throws IOException {
            if (lastOrd != vectorOrd) {
                readOrd(vectorOrd);
            }
            return new org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult(
                correctiveValues[0],
                correctiveValues[1],
                correctiveValues[2],
                quantizedComponentSum
            );
        }

        @Override
        public org.apache.lucene.util.quantization.OptimizedScalarQuantizer getQuantizer() {
            return new org.apache.lucene.util.quantization.OptimizedScalarQuantizer(similarityFunction);
        }

        @Override
        public Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding getScalarEncoding() {
            return Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.SEVEN_BIT;
        }

        @Override
        public float[] getCentroid() {
            return centroid;
        }

        @Override
        public float getCentroidDP() {
            return centroidDp;
        }

        @Override
        public KnnVectorValues.DocIndexIterator iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public VectorScorer scorer(float[] query) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] vectorValue(int ord) throws IOException {
            if (lastOrd != ord) {
                readOrd(ord);
            }
            return vectorValue;
        }

        @Override
        public int dimension() {
            return dimension;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public QuantizedByteVectorValues copy() throws IOException {
            return new DenseOffHeapCentroidQuantizedValues(dimension, size, similarityFunction, slice.clone(), centroid, centroidDp);
        }

        private void readOrd(int ord) throws IOException {
            final long byteSize = (long) dimension + 3L * Float.BYTES + Integer.BYTES;
            slice.seek(ord * byteSize);
            slice.readBytes(vectorValue, 0, vectorValue.length);
            slice.readFloats(correctiveValues, 0, 3);
            quantizedComponentSum = slice.readInt();
            lastOrd = ord;
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
                return new DenseNodesIterator(numCentroids);
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
