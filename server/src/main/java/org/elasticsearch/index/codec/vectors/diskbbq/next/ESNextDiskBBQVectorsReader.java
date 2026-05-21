/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

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
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;
import org.elasticsearch.index.codec.vectors.GenericFlatVectorReaders;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.VectorScoringUtils;
import org.elasticsearch.index.codec.vectors.cluster.NeighborQueue;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIterator;
import org.elasticsearch.index.codec.vectors.diskbbq.DocIdsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.index.codec.vectors.diskbbq.PostingMetadata;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.index.codec.vectors.diskbbq.PrefetchingCentroidIterator;
import org.elasticsearch.index.codec.vectors.diskbbq.VectorPreconditioner;
import org.elasticsearch.index.codec.vectors.es94.ES94ScalarQuantizedVectorsFormat;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.vectors.BulkKnnCollector;
import org.elasticsearch.search.vectors.ESAcceptDocs;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;
import org.elasticsearch.simdvec.ES940OSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer.DEFAULT_LAMBDA;
import static org.elasticsearch.index.codec.vectors.diskbbq.PostingMetadata.NO_ORDINAL;
import static org.elasticsearch.simdvec.ES940OSQVectorsScorer.BULK_SIZE;

/**
 * Default implementation of {@link IVFVectorsReader}. It scores the posting lists centroids using
 * brute force and then scores the top ones using the posting list.
 */
public class ESNextDiskBBQVectorsReader extends IVFVectorsReader<ESNextDiskBBQVectorsReader.NextFieldEntry>
    implements
        VectorPreconditioner {
    private static final Logger logger = LogManager.getLogger(ESNextDiskBBQVectorsReader.class);
    private static final Lucene104ScalarQuantizedVectorScorer CENTROID_GRAPH_FLAT_SCORER = ES94ScalarQuantizedVectorsFormat
        .getFlatVectorScorer();
    public static final float DEFAULT_GRAPH_INITIAL_BEAM_MULTIPLIER = 5.0f;
    public static final int DEFAULT_GRAPH_MIN_BEAM_WIDTH = 32;
    public static final int DEFAULT_GRAPH_VISIT_LIMIT_MULTIPLIER = 10;
    public static final String DEFAULT_GRAPH_BEAM_SCALING = "linear";
    public static final String SYSTEM_PROPERTY_GRAPH_BEAM_SCALING = "es.diskbbq.ivf.centroid_hnsw.graph_beam_scaling";
    public static final String SYSTEM_PROPERTY_GRAPH_BEAM_MULTIPLIER = "es.diskbbq.ivf.centroid_hnsw.graph_beam_multiplier";
    public static final String SYSTEM_PROPERTY_GRAPH_MIN_BEAM_WIDTH = "es.diskbbq.ivf.centroid_hnsw.graph_min_beam_width";
    public static final String SYSTEM_PROPERTY_GRAPH_VISIT_LIMIT_MULTIPLIER = "es.diskbbq.ivf.centroid_hnsw.graph_visit_limit_multiplier";
    public static final String SYSTEM_PROPERTY_GRAPH_BRUTE_FORCE = "es.diskbbq.ivf.centroid_hnsw.graph_bruteforce";
    public static final int DEFAULT_GRAPH_SPANN_INTERNAL_RESULT_NUM = 64;
    public static final String SYSTEM_PROPERTY_GRAPH_SPANN_INTERNAL_RESULT_NUM = "es.diskbbq.ivf.centroid_hnsw.spann_internal_result_num";
    private final Map<Integer, GraphSectionData> preloadedGraphSections;
    private final ESVectorUtil.SliceAddressArenaPool sliceAddressArenaPool;

    public ESNextDiskBBQVectorsReader(SegmentReadState state, GenericFlatVectorReaders.LoadFlatVectorsReader getFormatReader)
        throws IOException {
        super(
            state,
            getFormatReader,
            ESNextDiskBBQVectorsFormat.NAME,
            ESNextDiskBBQVectorsFormat.CENTROID_EXTENSION,
            ESNextDiskBBQVectorsFormat.CLUSTER_EXTENSION,
            ESNextDiskBBQVectorsFormat.IVF_META_EXTENSION,
            ESNextDiskBBQVectorsFormat.VERSION_START,
            ESNextDiskBBQVectorsFormat.VERSION_CURRENT,
            ESNextDiskBBQVectorsFormat.VERSION_DIRECT_IO,
            ESNextDiskBBQVectorsFormat.DYNAMIC_VISIT_RATIO,
            ESNextDiskBBQVectorsFormat.CENTROID_GRAPH_EXTENSION,
            ESNextDiskBBQVectorsFormat.VERSION_CENTROID_HNSW_CEX
        );
        this.sliceAddressArenaPool = new ESVectorUtil.SliceAddressArenaPool();
        this.preloadedGraphSections = preloadGraphSections(state);
    }

    private Map<Integer, GraphSectionData> preloadGraphSections(SegmentReadState state) throws IOException {
        Map<Integer, GraphSectionData> graphSections = new LinkedHashMap<>();
        for (FieldInfo fieldInfo : state.fieldInfos) {
            NextFieldEntry fieldEntry = fields.get(fieldInfo.number);
            if (fieldEntry == null) {
                continue;
            }
            if (fieldEntry.centroidSearchMode() == ESNextDiskBBQVectorsFormat.CentroidSearchMode.HNSW_4BIT
                && fieldEntry.centroidGraphLength() > 0) {
                graphSections.put(fieldInfo.number, readGraphSectionData(fieldInfo, fieldEntry));
            }
        }
        return graphSections;
    }

    private GraphSectionData getGraphSectionDataForQuery(FieldInfo fieldInfo) {
        GraphSectionData preloadedGraphSection = preloadedGraphSections.get(fieldInfo.number);
        if (preloadedGraphSection == null) {
            throw new IllegalStateException("centroid graph section was not preloaded for field [" + fieldInfo.name + "]");
        }
        return preloadedGraphSection;
    }

    CentroidIterator getPostingListPrefetchIterator(CentroidIterator centroidIterator, IndexInput postingListSlice) throws IOException {
        // TODO we may want to prefetch more than one postings list, however, we will likely want to place a limit
        // so we don't bother prefetching many lists we won't end up scoring
        return new PrefetchingCentroidIterator(centroidIterator, postingListSlice);
    }

    @Override
    protected int getNumberOfVectors(NextFieldEntry entry, FloatVectorValues values, IndexInput centroidSlice, ESAcceptDocs esAcceptDocs)
        throws IOException {
        int size = values.size();
        assert esAcceptDocs == null
            || entry.numSlices >= 0 && esAcceptDocs.sliceOrd() >= 0
            || entry.numSlices == -1 && esAcceptDocs.sliceOrd() == -1;
        if (entry.numSlices > 0) {
            long fp = centroidSlice.getFilePointer();
            final int bitsRequired = DirectWriter.bitsRequired(entry.maxSliceSize);
            final long sizeLookup = DirectWriter.bytesRequired(entry.numSlices, bitsRequired);
            if (esAcceptDocs != null) {
                int sliceOrd = esAcceptDocs.sliceOrd();
                assert sliceOrd < entry.numSlices : "sliceOrd out of range for centroid slices";
                final LongValues longValues = DirectReader.getInstance(centroidSlice.randomAccessSlice(fp, sizeLookup), bitsRequired);
                size = (int) longValues.get(sliceOrd);
            }
            centroidSlice.seek(fp + sizeLookup);
        }
        return size;
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
        final NextFieldEntry fieldEntry = fields.get(fieldInfo.number);
        // build optmization filters if possible
        final FixedBitSet acceptCentroids = getCentroidFilter(centroids, numCentroids, values, acceptDocs, approximateCost);
        final int numParents = centroids.readVInt();
        final FixedBitSet acceptParents = getParentCentroidFilter(centroids, numParents, numCentroids, acceptDocs, fieldEntry.numSlices);
        if (fieldEntry.centroidSearchMode() == ESNextDiskBBQVectorsFormat.CentroidSearchMode.HNSW_4BIT
            && fieldEntry.centroidGraphLength() > 0) {
            CentroidIterator centroidIterator = getCentroidIteratorGraph(
                fieldInfo,
                fieldEntry,
                targetQuery,
                visitRatio,
                numCentroids,
                acceptCentroids,
                acceptParents
            );
            return getPostingListPrefetchIterator(centroidIterator, postingListSlice);
        }
        // build centroid search helpers
        final int bulkSize = fieldEntry.getBulkSize();
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
        // build iterator
        CentroidIterator centroidIterator;
        if (numParents > 0) {
            // equivalent to (float) centroidsPerParentCluster / 2
            float centroidOversampling = (float) fieldEntry.numCentroids() / (2 * numParents);
            centroidIterator = getCentroidIteratorWithParents(
                fieldInfo,
                centroids,
                numParents,
                numCentroids,
                scorer,
                quantized,
                queryParams,
                fieldEntry.globalCentroidDp(),
                visitRatio * centroidOversampling,
                acceptParents,
                acceptCentroids,
                bulkSize
            );
        } else {
            if (acceptCentroids != null && acceptParents != null) {
                acceptCentroids.and(acceptParents);
            }
            centroidIterator = getCentroidIteratorNoParent(
                fieldInfo,
                centroids,
                numCentroids,
                scorer,
                quantized,
                queryParams,
                fieldEntry.globalCentroidDp(),
                acceptCentroids != null ? acceptCentroids : acceptParents,
                bulkSize
            );
        }
        return getPostingListPrefetchIterator(centroidIterator, postingListSlice);
    }

    private FixedBitSet getCentroidFilter(
        IndexInput centroids,
        int numCentroids,
        FloatVectorValues values,
        AcceptDocs acceptDocs,
        float approximateCost
    ) throws IOException {
        float approximateDocsPerCentroid = approximateCost / numCentroids;
        if (approximateDocsPerCentroid <= 1.25) {
            // TODO: we need to make this call to build the iterator, otherwise accept docs breaks all together
            approximateDocsPerCentroid = (float) acceptDocs.cost() / numCentroids;
        }
        final int bitsRequired = DirectWriter.bitsRequired(numCentroids);
        final long sizeLookup = DirectWriter.bytesRequired(values.size(), bitsRequired);
        long fp = centroids.getFilePointer();
        final FixedBitSet acceptCentroids;
        if (approximateDocsPerCentroid > 1.25 || numCentroids == 1 || acceptDocs instanceof ESAcceptDocs.ESAcceptDocsAll) {
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
        centroids.seek(fp + sizeLookup);
        return acceptCentroids;
    }

    private FixedBitSet getParentCentroidFilter(
        IndexInput centroids,
        int numParents,
        int numCentroids,
        AcceptDocs acceptDocs,
        int numSlices
    ) throws IOException {
        if (numSlices <= 0) {
            return null;
        }
        long fp = centroids.getFilePointer();
        FixedBitSet acceptParents = null;
        if (acceptDocs instanceof ESAcceptDocs esAcceptDocs) {
            // build a parent centroids filter
            int slice = esAcceptDocs.sliceOrd();
            // a slice must be provided
            assert slice >= 0 && slice < numSlices : "sliceOrd out of range for centroid slices";
            final int startOffset;
            final int endOffset;
            if (slice == 0) {
                startOffset = 0;
                endOffset = centroids.readInt();
            } else {
                centroids.skipBytes((long) (slice - 1) * Integer.BYTES);
                startOffset = centroids.readInt();
                endOffset = centroids.readInt();
            }
            if (numParents > 0) {
                acceptParents = new FixedBitSet(numParents);
                assert startOffset >= 0 && endOffset <= numParents;
            } else {
                acceptParents = new FixedBitSet(numCentroids);
                assert startOffset >= 0 && endOffset <= numCentroids;
            }
            acceptParents.set(startOffset, endOffset);
        }
        centroids.seek(fp + (long) numSlices * Integer.BYTES);
        return acceptParents;
    }

    private CentroidIterator getCentroidIteratorGraph(
        FieldInfo fieldInfo,
        NextFieldEntry fieldEntry,
        float[] targetQuery,
        float visitRatio,
        int numCentroids,
        FixedBitSet acceptCentroids,
        FixedBitSet acceptParents
    ) throws IOException {
        GraphSectionData graphSectionData = getGraphSectionDataForQuery(fieldInfo);
        if (graphSectionData.numCentroids() == 0) {
            return new EmptyCentroidIterator();
        }
        GraphAcceptState graphAcceptState = buildGraphAcceptState(
            acceptCentroids,
            acceptParents,
            graphSectionData.parentOrds(),
            graphSectionData.numCentroids()
        );
        int filtered = graphAcceptState.acceptedCount();
        if (filtered == 0) {
            return new EmptyCentroidIterator();
        }
        long totalAcceptedVectorsApprox = Math.max(1L, Math.round(graphSectionData.avgPostingLength() * filtered));
        long targetVisitedVectors = Math.max(1L, (long) Math.ceil(totalAcceptedVectorsApprox * visitRatio));
        GraphSearchTuning tuning = GraphSearchTuning.fromSystemProperties();
        int spannInternalResultNum = Math.max(
            1,
            parseIntProperty(SYSTEM_PROPERTY_GRAPH_SPANN_INTERNAL_RESULT_NUM, DEFAULT_GRAPH_SPANN_INTERNAL_RESULT_NUM)
        );
        int initialCollectorK = (int) (1.5 * (visitRatio * graphSectionData.numCentroids)) + 1;
        if (initialCollectorK > spannInternalResultNum && logger.isDebugEnabled()) {
            logger.debug(
                "SPANN-style query budget exceeded before continuation: initialCollectorK [{}], spannInternalResultNum [{}], visitRatio [{}], acceptedCentroids [{}]",
                initialCollectorK,
                spannInternalResultNum,
                visitRatio,
                filtered
            );
        }
        TopKnnCollector collector = new TopKnnCollector(initialCollectorK, Integer.MAX_VALUE);
        HnswGraph graph = graphSectionData.graphTemplate().newGraph();
        GraphCentroidQuantizedValues quantizedValues = graphSectionData.quantizedValues().copy();
        RandomVectorScorer scorer = CENTROID_GRAPH_FLAT_SCORER.getRandomVectorScorer(
            fieldInfo.getVectorSimilarityFunction(),
            quantizedValues,
            targetQuery
        );
        ScoreDoc[] results;
        if (parseBooleanProperty(SYSTEM_PROPERTY_GRAPH_BRUTE_FORCE, false)) {
            try (Closeable ignored = ESVectorUtil.activateSliceAddressArenaPool(sliceAddressArenaPool)) {
                results = scoreAcceptedCentroids(graphAcceptState.acceptOrds(), graphSectionData.numCentroids(), scorer);
            }
        } else {
            try (Closeable ignored = ESVectorUtil.activateSliceAddressArenaPool(sliceAddressArenaPool)) {
                HnswGraphSearcher.search(scorer, collector, graph, graphAcceptState.acceptOrds(), filtered);
            }
            var topDocs = collector.topDocs();
            results = topDocs == null ? new ScoreDoc[0] : topDocs.scoreDocs;
        }
        return new GraphCentroidIterator(
            results,
            graphSectionData.postingOffsets(),
            graphSectionData.postingLengths(),
            graphSectionData.parentOrds(),
            scorer,
            graph,
            graphAcceptState.acceptOrds(),
            graphSectionData.numCentroids(),
            filtered,
            tuning.visitLimitMultiplier(),
            targetVisitedVectors,
            spannInternalResultNum,
            sliceAddressArenaPool
        );
    }

    private static int computeGraphVisitLimit(int maxAllowedVisits, int beamWidth, int visitLimitMultiplier) {
        return Math.min(maxAllowedVisits, Math.max(beamWidth, beamWidth * visitLimitMultiplier));
    }

    private static GraphAcceptState buildGraphAcceptState(
        FixedBitSet acceptCentroids,
        FixedBitSet acceptParents,
        int[] parentOrds,
        int numCentroids
    ) {
        if (acceptCentroids == null && acceptParents == null) {
            return new GraphAcceptState(null, numCentroids);
        }
        if (acceptParents == null) {
            return new GraphAcceptState(acceptCentroids, acceptCentroids.cardinality());
        }
        if (acceptCentroids == null && parentOrds.length == numCentroids && acceptParents.length() == numCentroids) {
            return new GraphAcceptState(acceptParents, acceptParents.cardinality());
        }
        FixedBitSet acceptOrds = new FixedBitSet(numCentroids);
        for (int index = 0; index < numCentroids; index++) {
            boolean accepted = true;
            if (acceptCentroids != null && acceptCentroids.get(index) == false) {
                accepted = false;
            }
            if (accepted && acceptParents != null) {
                int parentOrd = parentOrds[index];
                if (parentOrd == NO_ORDINAL) {
                    accepted = acceptParents.get(index);
                } else {
                    accepted = parentOrd >= 0 && parentOrd < acceptParents.length() && acceptParents.get(parentOrd);
                }
            }
            if (accepted) {
                acceptOrds.set(index);
            }
        }
        return new GraphAcceptState(acceptOrds, acceptOrds.cardinality());
    }

    private static ScoreDoc[] scoreAcceptedCentroids(Bits acceptCentroids, int size, RandomVectorScorer scorer) throws IOException {
        NeighborQueue neighborQueue = new NeighborQueue(size, true);
        for (int ord = 0; ord < size; ord++) {
            if (acceptCentroids == null || acceptCentroids.get(ord)) {
                neighborQueue.add(ord, scorer.score(ord));
            }
        }
        ArrayList<ScoreDoc> scoreDocs = new ArrayList<>(neighborQueue.size());
        while (neighborQueue.size() > 0) {
            long raw = neighborQueue.popRaw();
            scoreDocs.add(new ScoreDoc(neighborQueue.decodeNodeId(raw), neighborQueue.decodeScore(raw)));
        }
        return scoreDocs.toArray(ScoreDoc[]::new);
    }

    private record GraphAcceptState(Bits acceptOrds, int acceptedCount) {}

    private GraphSectionData readGraphSectionData(FieldInfo fieldInfo, NextFieldEntry fieldEntry) throws IOException {
        if (fieldEntry.centroidGraphOffset() < 0 || fieldEntry.centroidGraphLength() <= 0) {
            throw new IllegalStateException("centroid graph mode enabled without graph section");
        }
        IndexInput graphSourceInput = versionMeta >= ESNextDiskBBQVectorsFormat.VERSION_CENTROID_HNSW_CEX
            ? getAuxiliaryInput()
            : ivfCentroids;
        if (graphSourceInput == null) {
            throw new IllegalStateException("centroid graph mode enabled without centroid graph data input");
        }
        IndexInput graphInput = graphSourceInput.slice(
            "centroid-graph",
            fieldEntry.centroidGraphOffset(),
            fieldEntry.centroidGraphLength()
        );
        int numCentroids = graphInput.readVInt();
        int vectorByteLength = graphInput.readVInt();
        if (versionMeta == ESNextDiskBBQVectorsFormat.VERSION_CENTROID_HNSW_CEX) {
            int numQuantizationCentroids = graphInput.readVInt();
            long quantizationCentroidStride = (long) fieldInfo.getVectorDimension() * Float.BYTES + Float.BYTES;
            graphInput.skipBytes(quantizationCentroidStride * numQuantizationCentroids);
        }
        long vectorPitch = vectorByteLength + 3L * Float.BYTES + Integer.BYTES;
        long vectorDataOffset = graphInput.getFilePointer();
        long vectorDataLength = vectorPitch * numCentroids;
        IndexInput vectorData = graphInput.slice("centroid-graph-vectors", vectorDataOffset, vectorDataLength);
        graphInput.seek(vectorDataOffset + vectorDataLength);
        long[] postingOffsets = new long[numCentroids];
        long[] postingLengths = new long[numCentroids];
        int[] parentOrds = new int[numCentroids];
        long totalPostingLength = 0L;
        for (int i = 0; i < numCentroids; i++) {
            postingOffsets[i] = graphInput.readLong();
            postingLengths[i] = graphInput.readLong();
            totalPostingLength += postingLengths[i];
            parentOrds[i] = graphInput.readInt();
            if (versionMeta == ESNextDiskBBQVectorsFormat.VERSION_CENTROID_HNSW_CEX) {
                graphInput.readInt(); // skip legacy quantization ord
            }
        }
        GraphTemplate graphTemplate = versionMeta >= ESNextDiskBBQVectorsFormat.VERSION_CENTROID_HNSW_CEX_OFFHEAP_GRAPH
            ? readOffHeapGraphTemplate(graphInput, numCentroids)
            : readLegacyGraphTemplate(graphInput, numCentroids);
        GraphCentroidQuantizedValues quantizedValues = new GraphCentroidQuantizedValues(
            fieldInfo.getVectorDimension(),
            numCentroids,
            fieldEntry.similarityFunction(),
            fieldEntry.globalCentroid(),
            fieldEntry.globalCentroidDp(),
            vectorByteLength,
            vectorData
        );
        double avgPostingLength = numCentroids == 0 ? 0.0d : (double) totalPostingLength / (double) numCentroids;
        return new GraphSectionData(
            numCentroids,
            quantizedValues,
            postingOffsets,
            postingLengths,
            parentOrds,
            graphTemplate,
            avgPostingLength
        );
    }

    private static GraphTemplate readLegacyGraphTemplate(IndexInput input, int numCentroids) throws IOException {
        long graphDataStart = input.getFilePointer();
        int numLevels = input.readVInt();
        int entryNode = input.readVInt();
        int maxConn = input.readVInt();
        int[][] levelNodes = new int[numLevels][];
        long[][] neighborOffsetsByLevel = new long[numLevels][numCentroids];
        int[][] neighborCountsByLevel = new int[numLevels][numCentroids];
        for (long[] levelOffsets : neighborOffsetsByLevel) {
            Arrays.fill(levelOffsets, -1L);
        }
        for (int level = 0; level < numLevels; level++) {
            int nodeCount = input.readVInt();
            int[] nodes = new int[nodeCount];
            for (int i = 0; i < nodeCount; i++) {
                int node = input.readVInt();
                nodes[i] = node;
                int neighborCount = input.readVInt();
                neighborOffsetsByLevel[level][node] = input.getFilePointer() - graphDataStart;
                neighborCountsByLevel[level][node] = neighborCount;
                for (int j = 0; j < neighborCount; j++) {
                    input.readVInt();
                }
            }
            levelNodes[level] = nodes;
        }
        long graphDataLength = input.getFilePointer() - graphDataStart;
        IndexInput graphData = input.slice("centroid-graph-hnsw", graphDataStart, graphDataLength);
        return new LegacyGraphTemplate(
            numCentroids,
            maxConn,
            entryNode,
            levelNodes,
            neighborOffsetsByLevel,
            neighborCountsByLevel,
            graphData
        );
    }

    private static GraphTemplate readOffHeapGraphTemplate(IndexInput input, int numCentroids) throws IOException {
        int numLevels = input.readVInt();
        int entryNode = input.readVInt();
        int maxConn = input.readVInt();
        int[][] nodesByLevel = new int[numLevels][];
        long[] levelNodeIndexOffsets = new long[numLevels];
        long totalOrdinals = numCentroids;
        levelNodeIndexOffsets[0] = 0L;
        for (int level = 1; level < numLevels; level++) {
            int nodeCount = input.readVInt();
            int[] nodes = new int[nodeCount];
            int cumulative = 0;
            for (int i = 0; i < nodeCount; i++) {
                cumulative += input.readVInt();
                nodes[i] = cumulative;
            }
            nodesByLevel[level] = nodes;
            levelNodeIndexOffsets[level] = totalOrdinals;
            totalOrdinals += nodeCount;
        }
        int offsetCount = input.readVInt();
        if (offsetCount != totalOrdinals) {
            throw new IllegalStateException(
                "centroid graph offsets count mismatch, expected [" + totalOrdinals + "] but got [" + offsetCount + "]"
            );
        }
        long offsetsStart = input.getFilePointer();
        IndexInput offsetsInput = input.slice("centroid-graph-offsets", offsetsStart, (long) offsetCount * Long.BYTES);
        input.seek(offsetsStart + (long) offsetCount * Long.BYTES);
        long graphDataStart = input.getFilePointer();
        IndexInput graphData = input.slice("centroid-graph-hnsw", graphDataStart, input.length() - graphDataStart);
        return new OffHeapGraphTemplate(numCentroids, maxConn, entryNode, nodesByLevel, levelNodeIndexOffsets, offsetsInput, graphData);
    }

    private record GraphSectionData(
        int numCentroids,
        GraphCentroidQuantizedValues quantizedValues,
        long[] postingOffsets,
        long[] postingLengths,
        int[] parentOrds,
        GraphTemplate graphTemplate,
        double avgPostingLength
    ) {}

    private interface GraphTemplate {
        HnswGraph newGraph() throws IOException;
    }

    private static class LegacyGraphTemplate implements GraphTemplate {
        private final int size;
        private final int maxConn;
        private final int entryNode;
        private final int[][] levelNodes;
        private final long[][] neighborOffsetsByLevel;
        private final int[][] neighborCountsByLevel;
        private final IndexInput graphData;

        private LegacyGraphTemplate(
            int size,
            int maxConn,
            int entryNode,
            int[][] levelNodes,
            long[][] neighborOffsetsByLevel,
            int[][] neighborCountsByLevel,
            IndexInput graphData
        ) {
            this.size = size;
            this.maxConn = maxConn;
            this.entryNode = entryNode;
            this.levelNodes = levelNodes;
            this.neighborOffsetsByLevel = neighborOffsetsByLevel;
            this.neighborCountsByLevel = neighborCountsByLevel;
            this.graphData = graphData;
        }

        @Override
        public HnswGraph newGraph() throws IOException {
            return new LegacySerializedHnswGraph(
                size,
                maxConn,
                entryNode,
                levelNodes,
                neighborOffsetsByLevel,
                neighborCountsByLevel,
                graphData.clone()
            );
        }
    }

    private static class OffHeapGraphTemplate implements GraphTemplate {
        private final int size;
        private final int maxConn;
        private final int entryNode;
        private final int[][] nodesByLevel;
        private final long[] levelNodeIndexOffsets;
        private final IndexInput offsetsInput;
        private final IndexInput graphData;

        private OffHeapGraphTemplate(
            int size,
            int maxConn,
            int entryNode,
            int[][] nodesByLevel,
            long[] levelNodeIndexOffsets,
            IndexInput offsetsInput,
            IndexInput graphData
        ) {
            this.size = size;
            this.maxConn = maxConn;
            this.entryNode = entryNode;
            this.nodesByLevel = nodesByLevel;
            this.levelNodeIndexOffsets = levelNodeIndexOffsets;
            this.offsetsInput = offsetsInput;
            this.graphData = graphData;
        }

        @Override
        public HnswGraph newGraph() throws IOException {
            IndexInput clonedOffsets = offsetsInput.clone();
            RandomAccessInput offsetsRandomAccess = clonedOffsets.randomAccessSlice(0L, clonedOffsets.length());
            return new OffHeapSerializedHnswGraph(
                size,
                maxConn,
                entryNode,
                nodesByLevel,
                levelNodeIndexOffsets,
                offsetsRandomAccess,
                graphData.clone()
            );
        }
    }

    private static class EmptyCentroidIterator implements CentroidIterator {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public PostingMetadata nextPosting() {
            return null;
        }
    }

    private static class GraphCentroidIterator implements CentroidIterator {
        private final long[] postingOffsets;
        private final long[] postingLengths;
        private final int[] parentOrds;
        private final RandomVectorScorer scorer;
        private final HnswGraph graph;
        private final Bits acceptOrds;
        private final Bits remainingAcceptView;
        private final int numCentroids;
        private final FixedBitSet consumedCentroidOrds;
        private final FixedBitSet queuedCentroidOrds;
        private final NeighborQueue candidateQueue;
        private final int visitLimitMultiplier;
        private final long targetVisitedVectors;
        private final int spannInternalResultNum;
        private final ESVectorUtil.SliceAddressArenaPool sliceAddressArenaPool;
        private int remainingAcceptedCentroids;
        private long visitedVectors;
        private int consumedCentroids;
        private boolean spannOverexplorationLogged;

        private GraphCentroidIterator(
            ScoreDoc[] scoreDocs,
            long[] postingOffsets,
            long[] postingLengths,
            int[] parentOrds,
            RandomVectorScorer scorer,
            HnswGraph graph,
            Bits acceptOrds,
            int numCentroids,
            int remainingAcceptedCentroids,
            int visitLimitMultiplier,
            long targetVisitedVectors,
            int spannInternalResultNum,
            ESVectorUtil.SliceAddressArenaPool sliceAddressArenaPool
        ) {
            this.postingOffsets = postingOffsets;
            this.postingLengths = postingLengths;
            this.parentOrds = parentOrds;
            this.scorer = scorer;
            this.graph = graph;
            this.acceptOrds = acceptOrds;
            this.numCentroids = numCentroids;
            this.consumedCentroidOrds = new FixedBitSet(numCentroids);
            this.remainingAcceptView = new Bits() {
                @Override
                public boolean get(int index) {
                    return isAcceptedAndRemaining(index);
                }

                @Override
                public int length() {
                    return numCentroids;
                }
            };
            this.queuedCentroidOrds = new FixedBitSet(numCentroids);
            this.candidateQueue = new NeighborQueue(Math.max(1, remainingAcceptedCentroids), true);
            this.remainingAcceptedCentroids = remainingAcceptedCentroids;
            this.visitLimitMultiplier = visitLimitMultiplier;
            this.targetVisitedVectors = targetVisitedVectors;
            this.spannInternalResultNum = spannInternalResultNum;
            this.sliceAddressArenaPool = sliceAddressArenaPool;
            this.consumedCentroids = 0;
            this.spannOverexplorationLogged = false;
            enqueueScoreDocs(scoreDocs);
        }

        @Override
        public boolean hasNext() {
            try {
                if (visitedVectors >= targetVisitedVectors) {
                    return false;
                }
                while (true) {
                    if (candidateQueue.size() > 0) {
                        return true;
                    }
                    if (remainingAcceptedCentroids <= 0 || visitedVectors >= targetVisitedVectors) {
                        return false;
                    }
                    int continueK = Math.max(1, Math.min(10, remainingAcceptedCentroids));
                    int continueVisitLimit = computeGraphVisitLimit(remainingAcceptedCentroids, continueK, visitLimitMultiplier);
                    TopKnnCollector continueCollector = new TopKnnCollector(continueK, continueVisitLimit, KnnSearchStrategy.Hnsw.DEFAULT);
                    try (Closeable ignored = ESVectorUtil.activateSliceAddressArenaPool(sliceAddressArenaPool)) {
                        HnswGraphSearcher.search(scorer, continueCollector, graph, remainingAcceptView, remainingAcceptedCentroids);
                    }
                    var continueTopDocs = continueCollector.topDocs();
                    ScoreDoc[] moreScoreDocs = continueTopDocs == null ? new ScoreDoc[0] : continueTopDocs.scoreDocs;
                    int added = enqueueScoreDocs(moreScoreDocs);
                    if (added == 0) {
                        try (Closeable ignored = ESVectorUtil.activateSliceAddressArenaPool(sliceAddressArenaPool)) {
                            added = enqueueScoreDocs(scoreAcceptedCentroids(remainingAcceptView, numCentroids, scorer));
                        }
                        if (added == 0) {
                            return false;
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("failed to continue centroid graph search", e);
            }
        }

        @Override
        public PostingMetadata nextPosting() {
            while (hasNext()) {
                long centroidOrdinalAndScore = candidateQueue.popRaw();
                int centroidOrd = candidateQueue.decodeNodeId(centroidOrdinalAndScore);
                float score = candidateQueue.decodeScore(centroidOrdinalAndScore);
                queuedCentroidOrds.clear(centroidOrd);
                if (isAcceptedAndRemaining(centroidOrd) == false) {
                    continue;
                }
                consumedCentroidOrds.set(centroidOrd);
                remainingAcceptedCentroids--;
                consumedCentroids++;
                visitedVectors += postingLengths[centroidOrd];
                if (spannOverexplorationLogged == false && consumedCentroids > spannInternalResultNum && logger.isDebugEnabled()) {
                    logger.debug(
                        "SPANN-style query exploration exceeded internal_result_num [{}], consumedCentroids [{}], remainingAcceptedCentroids [{}], targetVisitedVectors [{}]",
                        spannInternalResultNum,
                        consumedCentroids,
                        remainingAcceptedCentroids,
                        targetVisitedVectors
                    );
                    spannOverexplorationLogged = true;
                }
                return new PostingMetadata(postingOffsets[centroidOrd], postingLengths[centroidOrd], parentOrds[centroidOrd], score);
            }
            return null;
        }

        private int enqueueScoreDocs(ScoreDoc[] docs) {
            int added = 0;
            for (ScoreDoc scoreDoc : docs) {
                int centroidOrd = scoreDoc.doc;
                if (isAcceptedAndRemaining(centroidOrd) && queuedCentroidOrds.get(centroidOrd) == false) {
                    candidateQueue.add(centroidOrd, scoreDoc.score);
                    queuedCentroidOrds.set(centroidOrd);
                    added++;
                }
            }
            return added;
        }

        private boolean isAcceptedAndRemaining(int centroidOrd) {
            if (consumedCentroidOrds.get(centroidOrd)) {
                return false;
            }
            return acceptOrds == null || acceptOrds.get(centroidOrd);
        }
    }

    private enum GraphBeamScaling {
        LINEAR,
        LOG2;

        int scaledBeamWidth(int desiredCentroids, float beamMultiplier) {
            return switch (this) {
                case LINEAR -> Math.max(1, (int) Math.ceil(desiredCentroids * beamMultiplier));
                case LOG2 -> {
                    double log2 = Math.log(Math.max(2, desiredCentroids + 1)) / Math.log(2.0d);
                    yield Math.max(1, (int) Math.ceil(log2 * beamMultiplier));
                }
            };
        }

        static GraphBeamScaling parse(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "log", "log2" -> LOG2;
                default -> LINEAR;
            };
        }
    }

    private record GraphSearchTuning(GraphBeamScaling beamScaling, float beamMultiplier, int minBeamWidth, int visitLimitMultiplier) {
        static GraphSearchTuning fromSystemProperties() {
            GraphBeamScaling beamScaling = GraphBeamScaling.parse(
                System.getProperty(SYSTEM_PROPERTY_GRAPH_BEAM_SCALING, DEFAULT_GRAPH_BEAM_SCALING)
            );
            float beamMultiplier = Math.max(
                0.1f,
                parseFloatProperty(SYSTEM_PROPERTY_GRAPH_BEAM_MULTIPLIER, DEFAULT_GRAPH_INITIAL_BEAM_MULTIPLIER)
            );
            int minBeamWidth = Math.max(1, parseIntProperty(SYSTEM_PROPERTY_GRAPH_MIN_BEAM_WIDTH, DEFAULT_GRAPH_MIN_BEAM_WIDTH));
            int visitLimitMultiplier = Math.max(
                1,
                parseIntProperty(SYSTEM_PROPERTY_GRAPH_VISIT_LIMIT_MULTIPLIER, DEFAULT_GRAPH_VISIT_LIMIT_MULTIPLIER)
            );
            return new GraphSearchTuning(beamScaling, beamMultiplier, minBeamWidth, visitLimitMultiplier);
        }
    }

    private static int parseIntProperty(String propertyName, int defaultValue) {
        String value = System.getProperty(propertyName);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static float parseFloatProperty(String propertyName, float defaultValue) {
        String value = System.getProperty(propertyName);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static boolean parseBooleanProperty(String propertyName, boolean defaultValue) {
        String value = System.getProperty(propertyName);
        if (value == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    private static class LegacySerializedHnswGraph extends HnswGraph {
        private final int size;
        private final int maxConn;
        private final int entryNode;
        private final int[][] levelNodes;
        private final long[][] neighborOffsetsByLevel;
        private final int[][] neighborCountsByLevel;
        private final IndexInput graphData;
        private int currentNeighborCount = 0;
        private int currentNeighborIndex = 0;

        private LegacySerializedHnswGraph(
            int size,
            int maxConn,
            int entryNode,
            int[][] levelNodes,
            long[][] neighborOffsetsByLevel,
            int[][] neighborCountsByLevel,
            IndexInput graphData
        ) {
            this.size = size;
            this.maxConn = maxConn;
            this.entryNode = entryNode;
            this.levelNodes = levelNodes;
            this.neighborOffsetsByLevel = neighborOffsetsByLevel;
            this.neighborCountsByLevel = neighborCountsByLevel;
            this.graphData = graphData;
        }

        @Override
        public void seek(int level, int target) throws IOException {
            long offset = neighborOffsetsByLevel[level][target];
            currentNeighborCount = neighborCountsByLevel[level][target];
            currentNeighborIndex = 0;
            if (offset >= 0 && currentNeighborCount > 0) {
                graphData.seek(offset);
            }
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int nextNeighbor() throws IOException {
            if (currentNeighborIndex < currentNeighborCount) {
                currentNeighborIndex++;
                return graphData.readVInt();
            }
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public int numLevels() {
            return levelNodes.length;
        }

        @Override
        public int maxConn() {
            return maxConn;
        }

        @Override
        public int entryNode() {
            return entryNode;
        }

        @Override
        public NodesIterator getNodesOnLevel(int level) {
            return new IntArrayNodesIterator(levelNodes[level]);
        }

        @Override
        public int neighborCount() {
            return currentNeighborCount;
        }
    }

    private static class OffHeapSerializedHnswGraph extends HnswGraph {
        private final int size;
        private final int maxConn;
        private final int entryNode;
        private final int[][] levelNodes;
        private final long[] levelNodeIndexOffsets;
        private final RandomAccessInput offsetsData;
        private final IndexInput graphData;
        private int[] currentNeighbors;
        private int currentNeighborCount = 0;
        private int currentNeighborIndex = 0;

        private OffHeapSerializedHnswGraph(
            int size,
            int maxConn,
            int entryNode,
            int[][] levelNodes,
            long[] levelNodeIndexOffsets,
            RandomAccessInput offsetsData,
            IndexInput graphData
        ) {
            this.size = size;
            this.maxConn = maxConn;
            this.entryNode = entryNode;
            this.levelNodes = levelNodes;
            this.levelNodeIndexOffsets = levelNodeIndexOffsets;
            this.offsetsData = offsetsData;
            this.graphData = graphData;
            this.currentNeighbors = new int[maxConn * 2];
        }

        @Override
        public void seek(int level, int target) throws IOException {
            int targetIndex = level == 0 ? target : Arrays.binarySearch(levelNodes[level], target);
            if (targetIndex < 0) {
                currentNeighborCount = 0;
                currentNeighborIndex = 0;
                return;
            }
            long ordinalIndex = levelNodeIndexOffsets[level] + targetIndex;
            long offset = offsetsData.readLong(ordinalIndex * Long.BYTES);
            graphData.seek(offset);
            currentNeighborCount = graphData.readVInt();
            currentNeighborIndex = 0;
            if (currentNeighborCount > currentNeighbors.length) {
                currentNeighbors = Arrays.copyOf(currentNeighbors, currentNeighborCount);
            }
            int sum = 0;
            for (int i = 0; i < currentNeighborCount; i++) {
                sum += graphData.readVInt();
                currentNeighbors[i] = sum;
            }
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int nextNeighbor() {
            if (currentNeighborIndex < currentNeighborCount) {
                return currentNeighbors[currentNeighborIndex++];
            }
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public int numLevels() {
            return levelNodes.length;
        }

        @Override
        public int maxConn() {
            return maxConn;
        }

        @Override
        public int entryNode() {
            return entryNode;
        }

        @Override
        public NodesIterator getNodesOnLevel(int level) {
            if (level == 0) {
                return new DenseNodesIterator(size);
            }
            return new IntArrayNodesIterator(levelNodes[level]);
        }

        @Override
        public int neighborCount() {
            return currentNeighborCount;
        }
    }

    private static class IntArrayNodesIterator extends HnswGraph.NodesIterator {
        private final int[] nodes;
        private int index = 0;

        private IntArrayNodesIterator(int[] nodes) {
            super(nodes.length);
            this.nodes = nodes;
        }

        @Override
        public boolean hasNext() {
            return index < nodes.length;
        }

        @Override
        public int nextInt() {
            return nodes[index++];
        }

        @Override
        public int consume(int[] target) {
            int copied = Math.min(target.length, nodes.length - index);
            System.arraycopy(nodes, index, target, 0, copied);
            index += copied;
            return copied;
        }
    }

    private static class GraphCentroidQuantizedValues extends QuantizedByteVectorValues {
        private final int dimension;
        private final int size;
        private final VectorSimilarityFunction similarityFunction;
        private final float[] centroid;
        private final float centroidDp;
        private final int vectorByteLength;
        private final long vectorPitch;
        private final IndexInput vectorData;
        private final org.apache.lucene.util.quantization.OptimizedScalarQuantizer quantizer;
        private final byte[] scratch;
        private int lastCorrectiveOrd = -1;
        private org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult lastCorrectiveTerms;

        private GraphCentroidQuantizedValues(
            int dimension,
            int size,
            VectorSimilarityFunction similarityFunction,
            float[] centroid,
            float centroidDp,
            int vectorByteLength,
            IndexInput vectorData
        ) {
            this.dimension = dimension;
            this.size = size;
            this.similarityFunction = similarityFunction;
            this.centroid = centroid;
            this.centroidDp = centroidDp;
            this.vectorByteLength = vectorByteLength;
            this.vectorPitch = vectorByteLength + 3L * Float.BYTES + Integer.BYTES;
            this.vectorData = vectorData;
            this.quantizer = new org.apache.lucene.util.quantization.OptimizedScalarQuantizer(similarityFunction);
            this.scratch = new byte[vectorByteLength];
        }

        @Override
        public org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int ord) {
            checkOrd(ord);
            if (lastCorrectiveOrd == ord && lastCorrectiveTerms != null) {
                return lastCorrectiveTerms;
            }
            try {
                long correctionsOffset = (long) ord * vectorPitch + vectorByteLength;
                vectorData.seek(correctionsOffset);
                float lowerInterval = Float.intBitsToFloat(vectorData.readInt());
                float upperInterval = Float.intBitsToFloat(vectorData.readInt());
                float additionalCorrection = Float.intBitsToFloat(vectorData.readInt());
                int quantizedComponentSum = vectorData.readInt();
                lastCorrectiveOrd = ord;
                lastCorrectiveTerms = new org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult(
                    lowerInterval,
                    upperInterval,
                    additionalCorrection,
                    quantizedComponentSum
                );
                return lastCorrectiveTerms;
            } catch (IOException e) {
                throw new RuntimeException("failed to read centroid corrective terms", e);
            }
        }

        @Override
        public org.apache.lucene.util.quantization.OptimizedScalarQuantizer getQuantizer() {
            return quantizer;
        }

        @Override
        public Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding getScalarEncoding() {
            return Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.PACKED_NIBBLE;
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
        public VectorScorer scorer(float[] query) throws IOException {
            GraphCentroidQuantizedValues copy = copy();
            RandomVectorScorer randomVectorScorer = CENTROID_GRAPH_FLAT_SCORER.getRandomVectorScorer(similarityFunction, copy, query);
            return VectorScoringUtils.denseVectorScorer(randomVectorScorer, copy.iterator());
        }

        @Override
        public GraphCentroidQuantizedValues copy() throws IOException {
            return new GraphCentroidQuantizedValues(
                dimension,
                size,
                similarityFunction,
                centroid,
                centroidDp,
                vectorByteLength,
                vectorData.clone()
            );
        }

        @Override
        public IndexInput getSlice() {
            return vectorData;
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
        public byte[] vectorValue(int ord) throws IOException {
            checkOrd(ord);
            vectorData.seek((long) ord * vectorPitch);
            vectorData.readBytes(scratch, 0, vectorByteLength);
            return scratch;
        }

        @Override
        public int ordToDoc(int ord) {
            checkOrd(ord);
            return ord;
        }

        @Override
        public DocIndexIterator iterator() {
            return createDenseIterator();
        }

        private void checkOrd(int ord) {
            if (ord < 0 || ord >= size) {
                throw new IllegalArgumentException("illegal ordinal: " + ord);
            }
        }
    }

    @Override
    protected NextFieldEntry doReadField(
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
        int numSlices = input.readInt();
        int maxSliceSize = 0;
        if (numSlices > 0) {
            maxSliceSize = input.readVInt();
        }
        ESNextDiskBBQVectorsFormat.CentroidSearchMode centroidSearchMode = ESNextDiskBBQVectorsFormat.CentroidSearchMode.BRUTE_FORCE;
        long centroidGraphOffset = -1L;
        long centroidGraphLength = 0L;
        if (versionMeta >= ESNextDiskBBQVectorsFormat.VERSION_CENTROID_HNSW) {
            centroidSearchMode = ESNextDiskBBQVectorsFormat.CentroidSearchMode.fromId(input.readInt());
            centroidGraphOffset = input.readLong();
            centroidGraphLength = input.readLong();
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
            numSlices,
            maxSliceSize,
            centroidSearchMode,
            centroidGraphOffset,
            centroidGraphLength
        );
    }

    @Override
    public Preconditioner getPreconditioner(FieldInfo fieldInfo) throws IOException {
        final NextFieldEntry fieldEntry = fields.get(fieldInfo.number);
        // only seems possible in tests
        if (fieldEntry == null) {
            return null;
        }
        long preconditionerOffset = fieldEntry.preconditionerOffset();
        long preconditionerLength = fieldEntry.preconditionerLength();
        if (preconditionerLength > 0) {
            IndexInput ivfPreconditionerSlice = ivfCentroids.slice("preconditioner", preconditionerOffset, preconditionerLength);
            if (ivfPreconditionerSlice != null) {
                ivfPreconditionerSlice.seek(0);
                return Preconditioner.read(ivfPreconditionerSlice);
            }
        }
        return null;
    }

    protected static class NextFieldEntry extends FieldEntry {
        private final ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding;
        protected final long preconditionerOffset;
        protected final long preconditionerLength;
        // -1 "not sliced".
        // 0 "sliced but on flush".
        // > 0 "sliced but on merge, is the number of slices".
        final int numSlices;
        final int maxSliceSize;
        final ESNextDiskBBQVectorsFormat.CentroidSearchMode centroidSearchMode;
        final long centroidGraphOffset;
        final long centroidGraphLength;

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
            int numSlices,
            int maxSliceSize,
            ESNextDiskBBQVectorsFormat.CentroidSearchMode centroidSearchMode,
            long centroidGraphOffset,
            long centroidGraphLength
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
            this.numSlices = numSlices;
            this.maxSliceSize = maxSliceSize;
            this.centroidSearchMode = centroidSearchMode;
            this.centroidGraphOffset = centroidGraphOffset;
            this.centroidGraphLength = centroidGraphLength;
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

        public ESNextDiskBBQVectorsFormat.CentroidSearchMode centroidSearchMode() {
            return centroidSearchMode;
        }

        public long centroidGraphOffset() {
            return centroidGraphOffset;
        }

        public long centroidGraphLength() {
            return centroidGraphLength;
        }
    }

    private static CentroidIterator getCentroidIteratorNoParent(
        FieldInfo fieldInfo,
        IndexInput centroids,
        int numCentroids,
        ES92Int7VectorsScorer scorer,
        byte[] quantizeQuery,
        OptimizedScalarQuantizer.QuantizationResult queryParams,
        float globalCentroidDp,
        FixedBitSet acceptCentroids,
        int bulkSize
    ) throws IOException {
        final NeighborQueue neighborQueue = new NeighborQueue(numCentroids, true);
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
            bulkSize
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
                centroids.seek(offset + (long) Long.BYTES * 2 * centroidOrd);
                long postingListOffset = centroids.readLong();
                long postingListLength = centroids.readLong();
                // NO_ORDINAL indicates that the global centroid should be used for query quantization
                return new PostingMetadata(postingListOffset, postingListLength, NO_ORDINAL, score);
            }
        };
    }

    private static CentroidIterator getCentroidIteratorWithParents(
        FieldInfo fieldInfo,
        IndexInput centroids,
        int numParents,
        int numCentroids,
        ES92Int7VectorsScorer scorer,
        byte[] quantizeQuery,
        OptimizedScalarQuantizer.QuantizationResult queryParams,
        float globalCentroidDp,
        float centroidRatio,
        FixedBitSet acceptParents,
        FixedBitSet acceptCentroids,
        int bulkSize
    ) throws IOException {
        // build the three queues we are going to use
        final long rawParentSize = (long) fieldInfo.getVectorDimension() * Float.BYTES;
        final long centroidQuantizeSize = fieldInfo.getVectorDimension() + 3 * Float.BYTES + Integer.BYTES;
        final NeighborQueue parentsQueue = new NeighborQueue(numParents, true);
        final int maxChildrenSize = centroids.readVInt();
        final NeighborQueue currentParentQueue = new NeighborQueue(maxChildrenSize, true);
        final int bufferSize = (int) Math.min(Math.max(centroidRatio * numCentroids, 1), numCentroids);
        final int numCentroidsFiltered = acceptCentroids == null ? numCentroids : acceptCentroids.cardinality();
        if (numCentroidsFiltered == 0) {
            // TODO maybe this makes CentroidIterator polymorphic?
            return new CentroidIterator() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public PostingMetadata nextPosting() {
                    return null;
                }
            };
        }
        final float[] scores = new float[bulkSize];
        final NeighborQueue neighborQueue;
        if (acceptCentroids != null && numCentroidsFiltered <= bufferSize) {
            // we are collecting every non-filter centroid, therefore we do not need to score the
            // parents. We give each of them the same score.
            neighborQueue = new NeighborQueue(numCentroidsFiltered, true);
            for (int i = 0; i < numParents; i++) {
                if (acceptParents == null || acceptParents.get(i)) {
                    parentsQueue.add(i, 0.5f);
                }
            }
            centroids.skipBytes((centroidQuantizeSize + rawParentSize) * numParents);
        } else {
            neighborQueue = new NeighborQueue(bufferSize, true);
            // score the parents
            centroids.skipBytes(rawParentSize * numParents);
            score(
                parentsQueue,
                numParents,
                0,
                scorer,
                centroids,
                centroidQuantizeSize,
                quantizeQuery,
                queryParams,
                globalCentroidDp,
                fieldInfo.getVectorSimilarityFunction(),
                scores,
                acceptParents,
                bulkSize
            );
        }

        final long offset = centroids.getFilePointer();
        final long childrenOffset = offset + (long) Long.BYTES * numParents;
        // populate the children's queue by reading parents one by one
        while (parentsQueue.size() > 0 && neighborQueue.size() < bufferSize) {
            final int pop = parentsQueue.pop();
            populateOneChildrenGroup(
                currentParentQueue,
                centroids,
                offset + 2L * Integer.BYTES * pop,
                childrenOffset,
                centroidQuantizeSize,
                fieldInfo,
                scorer,
                quantizeQuery,
                queryParams,
                globalCentroidDp,
                scores,
                acceptCentroids,
                bulkSize
            );
            while (currentParentQueue.size() > 0 && neighborQueue.size() < bufferSize) {
                final float score = currentParentQueue.topScore();
                final int children = currentParentQueue.pop();
                neighborQueue.add(children, score);
            }
        }
        final long childrenFileOffsets = childrenOffset + centroidQuantizeSize * numCentroids;
        return new CentroidIterator() {

            @Override
            public boolean hasNext() {
                return neighborQueue.size() > 0;
            }

            @Override
            public PostingMetadata nextPosting() throws IOException {
                long centroidOrdinalAndScore = nextCentroid();
                int centroidOrdinal = neighborQueue.decodeNodeId(centroidOrdinalAndScore);
                float score = neighborQueue.decodeScore(centroidOrdinalAndScore);
                centroids.seek(childrenFileOffsets + (long) (Long.BYTES * 2 + Integer.BYTES) * centroidOrdinal);
                long postingListOffset = centroids.readLong();
                long postingListLength = centroids.readLong();
                int parentOrd = centroids.readInt();
                return new PostingMetadata(postingListOffset, postingListLength, parentOrd, score);
            }

            private long nextCentroid() throws IOException {
                if (currentParentQueue.size() > 0) {
                    // return next centroid and maybe add a children from the current parent queue
                    return neighborQueue.popRawAndAddRaw(currentParentQueue.popRaw());
                } else if (parentsQueue.size() > 0) {
                    // current parent queue is empty, populate it again with the next parent
                    int pop = parentsQueue.pop();
                    populateOneChildrenGroup(
                        currentParentQueue,
                        centroids,
                        offset + 2L * Integer.BYTES * pop,
                        childrenOffset,
                        centroidQuantizeSize,
                        fieldInfo,
                        scorer,
                        quantizeQuery,
                        queryParams,
                        globalCentroidDp,
                        scores,
                        acceptCentroids,
                        bulkSize
                    );
                    return nextCentroid();
                } else {
                    return neighborQueue.popRaw();
                }
            }
        };
    }

    private static void populateOneChildrenGroup(
        NeighborQueue neighborQueue,
        IndexInput centroids,
        long parentOffset,
        long childrenOffset,
        long centroidQuantizeSize,
        FieldInfo fieldInfo,
        ES92Int7VectorsScorer scorer,
        byte[] quantizeQuery,
        OptimizedScalarQuantizer.QuantizationResult queryParams,
        float globalCentroidDp,
        float[] scores,
        FixedBitSet acceptCentroids,
        int bulkSize
    ) throws IOException {
        centroids.seek(parentOffset);
        int childrenOrdinal = centroids.readInt();
        int numChildren = centroids.readInt();
        centroids.seek(childrenOffset + centroidQuantizeSize * childrenOrdinal);
        score(
            neighborQueue,
            numChildren,
            childrenOrdinal,
            scorer,
            centroids,
            centroidQuantizeSize,
            quantizeQuery,
            queryParams,
            globalCentroidDp,
            fieldInfo.getVectorSimilarityFunction(),
            scores,
            acceptCentroids,
            bulkSize
        );
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
        int bulkSize
    ) throws IOException {
        int limit = size - bulkSize + 1;
        int i = 0;
        for (; i < limit; i += bulkSize) {
            if (acceptCentroids == null || acceptCentroids.cardinality(scoresOffset + i, scoresOffset + i + bulkSize) > 0) {
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

    @Override
    public PostingVisitor getPostingVisitor(
        FieldInfo fieldInfo,
        FloatVectorValues values,
        IndexInput indexInput,
        float[] target,
        Bits needsScoring,
        IndexInput centroidSlice,
        ESAcceptDocs acceptDocs
    ) throws IOException {
        NextFieldEntry entry = fields.get(fieldInfo.number);
        if (entry.numSlices > 0) {
            final int bitsRequired = DirectWriter.bitsRequired(entry.maxSliceSize);
            final long sizeLookup = DirectWriter.bytesRequired(entry.numSlices, bitsRequired);
            centroidSlice.skipBytes(sizeLookup);
        }
        final int bitsRequired = DirectWriter.bitsRequired(entry.numCentroids());
        final long sizeLookup = DirectWriter.bytesRequired(values.size(), bitsRequired);
        centroidSlice.skipBytes(sizeLookup);
        ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding = entry.quantEncoding();
        int numParents = centroidSlice.readVInt();
        if (entry.numSlices > 0) {
            // skip slice offsets
            centroidSlice.skipBytes((long) entry.numSlices * Integer.BYTES);
        }
        final QueryQuantizer queryQuantizer;
        if (numParents > 0) {
            // unused
            int longestPostingList = centroidSlice.readVInt();
            IndexInput parentsSlice = centroidSlice.slice(
                "parents-slice",
                centroidSlice.getFilePointer(),
                (long) numParents * fieldInfo.getVectorDimension() * Float.BYTES
            );
            queryQuantizer = new QueryQuantizer(quantEncoding, fieldInfo, target, parentsSlice, entry.globalCentroid());
        } else {
            queryQuantizer = new QueryQuantizer(quantEncoding, fieldInfo, target, null, entry.globalCentroid());
        }
        if (entry.numSlices == 0) {
            // should only happen in sliced flushed segments
            assert entry.numCentroids() == 1;
            int startDoc;
            int endDoc;
            if (acceptDocs == null) {
                startDoc = 0;
                endDoc = values.ordToDoc(values.size() - 1);
            } else {
                ESAcceptDocs.SliceAcceptDocs sliceAcceptDocs = acceptDocs.sliceAcceptDocs();
                startDoc = sliceAcceptDocs.startDoc();
                endDoc = sliceAcceptDocs.endDoc();
            }
            return new SlicedMemorySegmentPostingsVisitor(
                queryQuantizer,
                quantEncoding,
                indexInput,
                entry,
                fieldInfo,
                needsScoring,
                values,
                startDoc,
                endDoc
            );

        } else {
            return new MemorySegmentPostingsVisitor(queryQuantizer, quantEncoding, indexInput, entry, fieldInfo, needsScoring);
        }
    }

    private record QueryQuantizerResult(OptimizedScalarQuantizer.QuantizationResult queryCorrections, byte[] quantizedTarget) {}

    private static final int QUERY_CACHE_SIZE = 16;

    private static class QueryQuantizer {
        private final LinkedHashMap<Integer, QueryQuantizerResult> cache;
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
            this.cache = new LinkedHashMap<>(QUERY_CACHE_SIZE, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Integer, QueryQuantizerResult> eldest) {
                    if (size() > QUERY_CACHE_SIZE) {
                        evictedQuantizedQuery = eldest.getValue().quantizedTarget();
                        return true;
                    }
                    return false;
                }
            };
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
        Map<String, Long> base = super.getOffHeapByteSize(fieldInfo);
        NextFieldEntry entry = fields.get(fieldInfo.number);
        if (entry == null || entry.centroidGraphLength() <= 0 || versionMeta < ESNextDiskBBQVectorsFormat.VERSION_CENTROID_HNSW_CEX) {
            return base;
        }
        return org.apache.lucene.codecs.KnnVectorsReader.mergeOffHeapByteSizeMaps(
            base,
            Map.of(ESNextDiskBBQVectorsFormat.CENTROID_GRAPH_EXTENSION, entry.centroidGraphLength())
        );
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        try {
            super.close();
        } catch (IOException e) {
            failure = e;
        }
        try {
            sliceAddressArenaPool.close();
        } catch (RuntimeException e) {
            if (failure == null) {
                failure = new IOException("failed to close slice address arena pool", e);
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static class SlicedMemorySegmentPostingsVisitor extends MemorySegmentPostingsVisitor {
        final int startDocId;
        final int endDocId;
        final FloatVectorValues floatVectorValues;

        SlicedMemorySegmentPostingsVisitor(
            QueryQuantizer queryQuantizer,
            ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding,
            IndexInput indexInput,
            FieldEntry entry,
            FieldInfo fieldInfo,
            Bits acceptDocs,
            FloatVectorValues values,
            int startDocId,
            int endDocId
        ) throws IOException {
            super(queryQuantizer, quantEncoding, indexInput, entry, fieldInfo, acceptDocs);
            this.startDocId = startDocId;
            this.endDocId = endDocId;
            this.floatVectorValues = values;
        }

        @Override
        public int resetPostingsScorer(PostingMetadata metadata) throws IOException {
            int totalVectors = super.resetPostingsScorer(metadata);
            int totalBlocks = totalVectors / BULK_SIZE;
            KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
            if (iterator.advance(startDocId) > endDocId) {
                this.vectors = 0;
                return 0;
            }
            int minOrd = iterator.index();
            int docId = iterator.advance(endDocId);
            int maxOrd;
            if (docId == DocIdSetIterator.NO_MORE_DOCS) {
                maxOrd = floatVectorValues.size() - 1;
            } else {
                maxOrd = iterator.index();
            }
            assert maxOrd - minOrd + 1 <= totalVectors;
            int startBlock = minOrd / BULK_SIZE;
            int endBlock = maxOrd / BULK_SIZE;
            if (endBlock == totalBlocks) {
                this.vectors = totalVectors - startBlock * BULK_SIZE;
            } else {
                this.vectors = (1 + endBlock - startBlock) * BULK_SIZE;
            }
            docBase = startBlock * BULK_SIZE;
            slicePos += startBlock * BULK_SIZE * quantizedByteLength;
            return this.vectors;
        }

        @Override
        protected void readDocIds(int count) {
            for (int j = 0; j < count; j++) {
                int docId = floatVectorValues.ordToDoc(docBase++);
                if (docId >= startDocId && docId <= endDocId) {
                    docIdsScratch[j] = docId;
                } else {
                    docIdsScratch[j] = -1;
                }
            }
        }
    }

    private static class MemorySegmentPostingsVisitor implements PostingVisitor {
        final long quantizedByteLength;
        final IndexInput indexInput;
        final FieldEntry entry;
        final FieldInfo fieldInfo;
        final Bits acceptDocs;
        private final ES940OSQVectorsScorer osqVectorsScorer;
        final float[] scores = new float[BULK_SIZE];
        final float[] correctionsLower = new float[BULK_SIZE];
        final float[] correctionsUpper = new float[BULK_SIZE];
        final int[] correctionsSum = new int[BULK_SIZE];
        final float[] correctionsAdd = new float[BULK_SIZE];
        final int[] docIdsScratch = new int[BULK_SIZE];
        final int[] offsetsScratch = new int[BULK_SIZE];
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
            osqVectorsScorer = ESVectorUtil.getES940OSQVectorsScorer(
                indexInput,
                quantEncoding.queryBits(),
                quantEncoding.bits(),
                fieldInfo.getVectorDimension(),
                (int) quantizedVectorByteSize,
                BULK_SIZE,
                quantEncoding.bits() == 4
                    ? ES940OSQVectorsScorer.SymmetricInt4Encoding.PACKED_NIBBLE
                    : ES940OSQVectorsScorer.SymmetricInt4Encoding.STRIPED
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
                    scores[j] = osqVectorsScorer.applyCorrectionsIndividually(
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

        private static int docToBulkScore(int[] docIds, int[] offsets, Bits acceptDocs, int bulkSize) {
            assert acceptDocs != null : "acceptDocs must not be null";
            int docToScore = 0;
            for (int i = 0; i < bulkSize; i++) {
                if (docIds[i] == -1 || acceptDocs.get(docIds[i]) == false) {
                    docIds[i] = -1;
                } else {
                    offsets[docToScore] = i;
                    docToScore++;
                }
            }
            return docToScore;
        }

        protected void collectBulk(KnnCollector knnCollector, float[] scores, int bulkSize, int docsToBulkScore, float maxScore) {
            if (knnCollector instanceof BulkKnnCollector bulkCollector) {
                if (docsToBulkScore == bulkSize) {
                    bulkCollector.bulkCollect(docIdsScratch, scores, bulkSize, maxScore);
                    return;
                }
                for (int i = 0; i < docsToBulkScore; i++) {
                    int offset = offsetsScratch[i];
                    docIdsScratch[i] = docIdsScratch[offset];
                    scores[i] = scores[offset];
                }
                bulkCollector.bulkCollect(docIdsScratch, scores, docsToBulkScore, maxScore);
                return;
            }
            for (int i = 0; i < bulkSize; i++) {
                final int doc = docIdsScratch[i];
                if (doc != -1) {
                    knnCollector.collect(doc, scores[i]);
                }
            }
        }

        protected void readDocIds(int count) throws IOException {
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
                final int docsToBulkScore = acceptDocs == null
                    ? BULK_SIZE
                    : docToBulkScore(docIdsScratch, offsetsScratch, acceptDocs, BULK_SIZE);
                if (docsToBulkScore == 0) {
                    indexInput.skipBytes(quantizedByteLength * BULK_SIZE);
                    continue;
                }
                queryQuantizer.quantizeQueryIfNecessary();
                final float maxScore;
                if (docsToBulkScore == 1) {
                    maxScore = scoreIndividually(BULK_SIZE);
                } else if (docsToBulkScore < BULK_SIZE) {
                    maxScore = osqVectorsScorer.scoreBulkOffsets(
                        queryQuantizer.getQuantizedTarget(),
                        queryQuantizer.getQueryCorrections().lowerInterval(),
                        queryQuantizer.getQueryCorrections().upperInterval(),
                        queryQuantizer.getQueryCorrections().quantizedComponentSum(),
                        centroidDistance,
                        fieldInfo.getVectorSimilarityFunction(),
                        0f,
                        offsetsScratch,
                        docsToBulkScore,
                        scores,
                        BULK_SIZE
                    );
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
                    collectBulk(knnCollector, scores, BULK_SIZE, docsToBulkScore, maxScore);
                }
                scoredDocs += docsToBulkScore;
            }
            // bulk process tail
            if (i < vectors) {
                int tailSize = vectors - i;
                readDocIds(tailSize);
                final int docsToBulkScore = acceptDocs == null
                    ? tailSize
                    : docToBulkScore(docIdsScratch, offsetsScratch, acceptDocs, tailSize);
                if (docsToBulkScore == 0) {
                    indexInput.skipBytes(quantizedByteLength * tailSize);
                } else {
                    queryQuantizer.quantizeQueryIfNecessary();
                    final float maxScore;
                    if (docsToBulkScore == 1) {
                        maxScore = scoreIndividually(tailSize);
                    } else if (docsToBulkScore < tailSize) {
                        maxScore = osqVectorsScorer.scoreBulkOffsets(
                            queryQuantizer.getQuantizedTarget(),
                            queryQuantizer.getQueryCorrections().lowerInterval(),
                            queryQuantizer.getQueryCorrections().upperInterval(),
                            queryQuantizer.getQueryCorrections().quantizedComponentSum(),
                            centroidDistance,
                            fieldInfo.getVectorSimilarityFunction(),
                            0f,
                            offsetsScratch,
                            docsToBulkScore,
                            scores,
                            tailSize
                        );
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
                        collectBulk(knnCollector, scores, tailSize, docsToBulkScore, maxScore);
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
