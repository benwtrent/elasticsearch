/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.apache.lucene.util.hnsw.NeighborArray;
import org.apache.lucene.util.hnsw.OnHeapHnswGraph;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.cluster.ClusteringFloatVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.ClusteringFloatVectorValuesSlice;
import org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.KMeansResult;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidAssignments;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidSlices;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidSupplier;
import org.elasticsearch.index.codec.vectors.diskbbq.DiskBBQBulkWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.DocIdsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.IntSorter;
import org.elasticsearch.index.codec.vectors.diskbbq.IntToBooleanFunction;
import org.elasticsearch.index.codec.vectors.diskbbq.PostingMetadata;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantizedVectorValues;
import org.elasticsearch.index.codec.vectors.diskbbq.VectorPreconditioner;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ES940OSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;

import static org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans.NO_SOAR_ASSIGNMENT;
import static org.elasticsearch.simdvec.ES940OSQVectorsScorer.BULK_SIZE;

/**
 * Default implementation of {@link IVFVectorsWriter}. It uses {@link HierarchicalKMeans} algorithm to
 * partition the vector space, and then stores the centroids and posting list in a sequential
 * fashion.
 */
public class ESNextDiskBBQVectorsWriter extends IVFVectorsWriter {
    private static final Logger logger = LogManager.getLogger(ESNextDiskBBQVectorsWriter.class);
    private static final int CENTROID_GRAPH_HNSW_M = 16;
    private static final int CENTROID_GRAPH_HNSW_BEAM_WIDTH = 250;
    private static final String SYSTEM_PROPERTY_IVF_REPLICA_LIMIT = "es.diskbbq.ivf.replica_limit";
    private static final String SYSTEM_PROPERTY_IVF_REPLICA_INTERNAL_RESULT_NUM = "es.diskbbq.ivf.replica_internal_result_num";
    private static final String SYSTEM_PROPERTY_IVF_REPLICA_RNG_FACTOR = "es.diskbbq.ivf.replica_rng_factor";
    private static final String SYSTEM_PROPERTY_IVF_REPLICA_POSTING_LIMIT_MULTIPLIER = "es.diskbbq.ivf.replica_posting_limit_multiplier";
    private static final int HNSW_REPLICA_LIMIT = 8;
    private static final int REGULAR_IVF_REPLICA_LIMIT_ABLATION = 8;
    private static final int SPANN_INTERNAL_RESULT_NUM_DEFAULT = 64;
    private static final float SPANN_RNG_FACTOR_DEFAULT = 1.0f;
    private static final float SPANN_POSTING_LIMIT_MULTIPLIER_DEFAULT = 4.0f;

    private final int vectorPerCluster;
    private final int centroidsPerParentCluster;
    private final ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding;
    private final ESNextDiskBBQVectorsFormat.CentroidSearchMode centroidSearchMode;
    private final TaskExecutor mergeExec;
    private final int numMergeWorkers;
    private final int blockDimension;
    private final boolean doPrecondition;
    private final Map<Integer, GraphSection> graphSections = new HashMap<>();
    // field for slicing, null for no slicing
    private final String sliceField;

    public ESNextDiskBBQVectorsWriter(
        SegmentWriteState state,
        String rawVectorFormatName,
        boolean useDirectIOReads,
        FlatVectorsWriter rawVectorDelegate,
        ESNextDiskBBQVectorsFormat.QuantEncoding encoding,
        int vectorPerCluster,
        int centroidsPerParentCluster,
        TaskExecutor mergeExec,
        int numMergeWorkers,
        int blockDimension,
        boolean doPrecondition,
        int flatVectorThreshold,
        String sliceField,
        ESNextDiskBBQVectorsFormat.CentroidSearchMode centroidSearchMode
    ) throws IOException {
        super(
            state,
            rawVectorFormatName,
            useDirectIOReads,
            rawVectorDelegate,
            ESNextDiskBBQVectorsFormat.VERSION_CURRENT,
            ESNextDiskBBQVectorsFormat.NAME,
            ESNextDiskBBQVectorsFormat.IVF_META_EXTENSION,
            ESNextDiskBBQVectorsFormat.CENTROID_EXTENSION,
            ESNextDiskBBQVectorsFormat.CLUSTER_EXTENSION,
            ESNextDiskBBQVectorsFormat.CENTROID_GRAPH_EXTENSION,
            true,
            flatVectorThreshold
        );
        this.vectorPerCluster = vectorPerCluster;
        this.centroidsPerParentCluster = centroidsPerParentCluster;
        this.quantEncoding = encoding;
        this.centroidSearchMode = centroidSearchMode;
        this.mergeExec = mergeExec;
        this.numMergeWorkers = numMergeWorkers;
        this.blockDimension = blockDimension;
        this.doPrecondition = doPrecondition;
        this.sliceField = sliceField;
        if (sliceField != null) {
            Sort sort = state.segmentInfo.getIndexSort();
            if (sort == null || sort.getSort().length == 0) {
                throw new IllegalStateException("sliceField requires index sort");
            }
            SortField primary = sort.getSort()[0];
            if (sliceField.equals(primary.getField()) == false) {
                throw new IllegalStateException("sliceField must be primary index sort");
            }
            if (primary.getType() != SortField.Type.STRING) {
                throw new IllegalStateException("sliceField requires primary index sort");
            }
        }
    }

    @Override
    protected Preconditioner inheritPreconditioner(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        if (doPrecondition) {
            for (KnnVectorsReader reader : mergeState.knnVectorsReaders) {
                if (reader instanceof VectorPreconditioner) {
                    Preconditioner preconditioner = ((VectorPreconditioner) reader).getPreconditioner(fieldInfo);
                    if (preconditioner != null) {
                        return preconditioner;
                    }
                }
            }
            // else
            return createPreconditioner(fieldInfo.getVectorDimension());
        }
        return null;
    }

    @Override
    protected Preconditioner createPreconditioner(int dimension) {
        if (doPrecondition) {
            return Preconditioner.createPreconditioner(dimension, blockDimension);
        } else {
            return null;
        }
    }

    @Override
    protected void writePreconditioner(Preconditioner preconditioner, IndexOutput out) throws IOException {
        if (preconditioner != null) {
            preconditioner.write(out);
        }
    }

    @Override
    protected Consumer<List<float[]>> preconditionVectors(Preconditioner preconditioner) {
        return (vectors) -> {
            if (doPrecondition == false || vectors.isEmpty()) {
                return;
            }
            if (preconditioner == null) {
                throw new IllegalStateException("preconditioner was not created but should be first");
            }
            float[] out = new float[vectors.getFirst().length];
            for (int i = 0; i < vectors.size(); i++) {
                float[] vector = vectors.get(i);
                preconditioner.applyTransform(vector, out);
                System.arraycopy(out, 0, vector, 0, vector.length);
            }
        };
    }

    @Override
    protected FloatVectorValues preconditionVectors(Preconditioner preconditioner, FloatVectorValues vectors) {
        if (doPrecondition == false) {
            return vectors;
        }
        if (preconditioner == null) {
            throw new IllegalStateException("preconditioner was not created but should be first");
        }

        // TODO: batch apply preconditioner for better performance and keep a batch on heap at a time
        return new FloatVectorValues() {
            final float[] preconditionedVectorValue = new float[vectors.dimension()];
            int cachedOrd = -1;

            @Override
            public int getVectorByteLength() {
                return vectors.getVectorByteLength();
            }

            @Override
            public float[] vectorValue(int ord) throws IOException {
                assert ord != -1;
                if (ord != cachedOrd) {
                    float[] vectorValue = vectors.vectorValue(ord);
                    preconditioner.applyTransform(vectorValue, this.preconditionedVectorValue);
                    cachedOrd = ord;
                }
                return this.preconditionedVectorValue;
            }

            @Override
            public FloatVectorValues copy() throws IOException {
                return vectors.copy();
            }

            @Override
            public int dimension() {
                return vectors.dimension();
            }

            @Override
            public int size() {
                return vectors.size();
            }

            @Override
            public DocIndexIterator iterator() {
                return vectors.iterator();
            }
        };
    }

    @Override
    public CentroidOffsetAndLength buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        int[] assignments,
        int[] overspillAssignments
    ) throws IOException {
        int replicaLimit = resolveReplicaLimit();
        if (replicaLimit > 1) {
            return buildAndWritePostingsListsWithNeighborhoodReplicas(
                fieldInfo,
                centroidSupplier,
                floatVectorValues,
                postingsOutput,
                fileOffset,
                assignments,
                replicaLimit
            );
        }
        KMeansResult centroidClusters = centroidSupplier.secondLevelClusters();
        int[] centroidVectorCount = new int[centroidSupplier.size()];
        for (int i = 0; i < assignments.length; i++) {
            centroidVectorCount[assignments[i]]++;
            // if soar assignments are present, count them as well
            if (overspillAssignments.length > i && overspillAssignments[i] != NO_SOAR_ASSIGNMENT) {
                centroidVectorCount[overspillAssignments[i]]++;
            }
        }

        int maxPostingListSize = 0;
        int[][] assignmentsByCluster = new int[centroidSupplier.size()][];
        for (int c = 0; c < centroidSupplier.size(); c++) {
            int size = centroidVectorCount[c];
            maxPostingListSize = Math.max(maxPostingListSize, size);
            assignmentsByCluster[c] = new int[size];
        }
        Arrays.fill(centroidVectorCount, 0);

        for (int i = 0; i < assignments.length; i++) {
            int c = assignments[i];
            assignmentsByCluster[c][centroidVectorCount[c]++] = i;
            // if soar assignments are present, add them to the cluster as well
            if (overspillAssignments.length > i) {
                int s = overspillAssignments[i];
                if (s != NO_SOAR_ASSIGNMENT) {
                    assignmentsByCluster[s][centroidVectorCount[s]++] = i;
                }
            }
        }
        // write the posting lists
        final PackedLongValues.Builder offsets = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        final PackedLongValues.Builder lengths = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(quantEncoding.bits(), BULK_SIZE, postingsOutput, true, true);
        OnHeapQuantizedVectors onHeapQuantizedVectors = new OnHeapQuantizedVectors(
            floatVectorValues,
            fieldInfo.getVectorSimilarityFunction(),
            quantEncoding,
            fieldInfo.getVectorDimension(),
            new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction())
        );
        final int[] docIds = new int[maxPostingListSize];
        final int[] docDeltas = new int[maxPostingListSize];
        final int[] clusterOrds = new int[maxPostingListSize];
        DocIdsWriter idsWriter = new DocIdsWriter();
        for (int c = 0; c < centroidSupplier.size(); c++) {
            float[] centroid = centroidSupplier.centroid(c);
            int[] cluster = assignmentsByCluster[c];
            long offset = postingsOutput.alignFilePointer(Float.BYTES) - fileOffset;
            offsets.add(offset);
            postingsOutput.writeInt(Float.floatToIntBits(ESVectorUtil.squareDistance(centroid, centroidClusters.getCentroid(c))));
            int size = cluster.length;
            // write docIds
            postingsOutput.writeVInt(size);
            for (int j = 0; j < size; j++) {
                docIds[j] = floatVectorValues.ordToDoc(cluster[j]);
                clusterOrds[j] = j;
            }
            // sort cluster.buffer by docIds values, this way cluster ordinals are sorted by docIds
            new IntSorter(clusterOrds, i -> docIds[i]).sort(0, size);
            // encode doc deltas
            for (int j = 0; j < size; j++) {
                docDeltas[j] = j == 0 ? docIds[clusterOrds[j]] : docIds[clusterOrds[j]] - docIds[clusterOrds[j - 1]];
            }
            onHeapQuantizedVectors.reset(centroid, centroidClusters.getCentroid(c), size, ord -> cluster[clusterOrds[ord]]);
            byte encoding = idsWriter.calculateBlockEncoding(i -> docDeltas[i], size, BULK_SIZE);
            postingsOutput.writeByte(encoding);
            if (sliceField != null) {
                // We are not writing the docIds as we know they are writing in vector ord order.
                // we will ise the delegated FloatVectorValue instance on read to do the translation for us.
                assert centroidSupplier.size() == 1;
                bulkWriter.writeVectors(onHeapQuantizedVectors, null);
            } else {
                bulkWriter.writeVectors(onHeapQuantizedVectors, i -> {
                    // for vector i we write `bulk` size docs or the remaining docs
                    idsWriter.writeDocIds(d -> docDeltas[i + d], Math.min(BULK_SIZE, size - i), encoding, postingsOutput);
                });
            }
            lengths.add(postingsOutput.getFilePointer() - fileOffset - offset);
        }

        if (logger.isDebugEnabled()) {
            printClusterQualityStatistics(assignmentsByCluster);
        }

        return new CentroidOffsetAndLength(offsets.build(), lengths.build());
    }

    private CentroidOffsetAndLength buildAndWritePostingsListsWithNeighborhoodReplicas(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        int[] assignments,
        int replicaLimit
    ) throws IOException {
        KMeansResult centroidClusters = centroidSupplier.secondLevelClusters();
        float[][] centroids = materializeCentroids(centroidSupplier);
        ReplicaAssignmentSettings replicaSettings = resolveReplicaAssignmentSettings(replicaLimit);
        ReplicaAssignments replicasByVector = buildReplicaAssignmentsByNeighborhood(
            floatVectorValues,
            fieldInfo.getVectorSimilarityFunction(),
            assignments,
            centroids,
            replicaLimit,
            replicaSettings
        );
        applyReplicaPostingCut(assignments, replicasByVector, centroids.length, replicaSettings.postingLimitMultiplier());
        int[] centroidVectorCount = new int[centroidSupplier.size()];
        for (int i = 0; i < assignments.length; i++) {
            centroidVectorCount[assignments[i]]++;
            for (int replica : replicasByVector.replicaOrds()[i]) {
                if (replica != NO_SOAR_ASSIGNMENT) {
                    centroidVectorCount[replica]++;
                }
            }
        }
        int maxPostingListSize = 0;
        int[][] assignmentsByCluster = new int[centroidSupplier.size()][];
        for (int c = 0; c < centroidSupplier.size(); c++) {
            int size = centroidVectorCount[c];
            maxPostingListSize = Math.max(maxPostingListSize, size);
            assignmentsByCluster[c] = new int[size];
        }
        Arrays.fill(centroidVectorCount, 0);
        for (int i = 0; i < assignments.length; i++) {
            int primary = assignments[i];
            assignmentsByCluster[primary][centroidVectorCount[primary]++] = i;
            for (int replica : replicasByVector.replicaOrds()[i]) {
                if (replica != NO_SOAR_ASSIGNMENT) {
                    assignmentsByCluster[replica][centroidVectorCount[replica]++] = i;
                }
            }
        }
        final PackedLongValues.Builder offsets = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        final PackedLongValues.Builder lengths = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(quantEncoding.bits(), BULK_SIZE, postingsOutput, true, true);
        OnHeapQuantizedVectors onHeapQuantizedVectors = new OnHeapQuantizedVectors(
            floatVectorValues,
            fieldInfo.getVectorSimilarityFunction(),
            quantEncoding,
            fieldInfo.getVectorDimension(),
            new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction())
        );
        final int[] docIds = new int[maxPostingListSize];
        final int[] docDeltas = new int[maxPostingListSize];
        final int[] clusterOrds = new int[maxPostingListSize];
        DocIdsWriter idsWriter = new DocIdsWriter();
        for (int c = 0; c < centroidSupplier.size(); c++) {
            float[] centroid = centroidSupplier.centroid(c);
            int[] cluster = assignmentsByCluster[c];
            long offset = postingsOutput.alignFilePointer(Float.BYTES) - fileOffset;
            offsets.add(offset);
            postingsOutput.writeInt(Float.floatToIntBits(ESVectorUtil.squareDistance(centroid, centroidClusters.getCentroid(c))));
            int size = cluster.length;
            postingsOutput.writeVInt(size);
            for (int j = 0; j < size; j++) {
                docIds[j] = floatVectorValues.ordToDoc(cluster[j]);
                clusterOrds[j] = j;
            }
            new IntSorter(clusterOrds, i -> docIds[i]).sort(0, size);
            for (int j = 0; j < size; j++) {
                docDeltas[j] = j == 0 ? docIds[clusterOrds[j]] : docIds[clusterOrds[j]] - docIds[clusterOrds[j - 1]];
            }
            onHeapQuantizedVectors.reset(centroid, centroidClusters.getCentroid(c), size, ord -> cluster[clusterOrds[ord]]);
            byte encoding = idsWriter.calculateBlockEncoding(i -> docDeltas[i], size, BULK_SIZE);
            postingsOutput.writeByte(encoding);
            if (sliceField != null) {
                assert centroidSupplier.size() == 1;
                bulkWriter.writeVectors(onHeapQuantizedVectors, null);
            } else {
                bulkWriter.writeVectors(onHeapQuantizedVectors, i -> {
                    idsWriter.writeDocIds(d -> docDeltas[i + d], Math.min(BULK_SIZE, size - i), encoding, postingsOutput);
                });
            }
            lengths.add(postingsOutput.getFilePointer() - fileOffset - offset);
        }
        if (logger.isDebugEnabled()) {
            printClusterQualityStatistics(assignmentsByCluster);
        }
        return new CentroidOffsetAndLength(offsets.build(), lengths.build());
    }

    private void applyReplicaPostingCut(
        int[] primaryAssignments,
        ReplicaAssignments replicaAssignments,
        int numCentroids,
        float postingLimitMultiplier
    ) {
        if (postingLimitMultiplier <= 0f || numCentroids <= 0) {
            return;
        }
        int postingSizeLimit = Math.max(
            1,
            (int) Math.ceil(((double) primaryAssignments.length / (double) numCentroids) * postingLimitMultiplier)
        );
        int[] postingCounts = new int[numCentroids];
        int[][] replicaOrds = replicaAssignments.replicaOrds();
        float[][] replicaScores = replicaAssignments.replicaScores();
        for (int i = 0; i < primaryAssignments.length; i++) {
            postingCounts[primaryAssignments[i]]++;
            for (int replica : replicaOrds[i]) {
                if (replica != NO_SOAR_ASSIGNMENT) {
                    postingCounts[replica]++;
                }
            }
        }
        int totalDropped = 0;
        for (int centroidOrd = 0; centroidOrd < numCentroids; centroidOrd++) {
            int overflow = postingCounts[centroidOrd] - postingSizeLimit;
            if (overflow <= 0) {
                continue;
            }
            ArrayList<ReplicaEdge> edges = new ArrayList<>();
            for (int vectorOrd = 0; vectorOrd < replicaOrds.length; vectorOrd++) {
                int[] replicas = replicaOrds[vectorOrd];
                for (int slot = 0; slot < replicas.length; slot++) {
                    if (replicas[slot] == centroidOrd) {
                        edges.add(new ReplicaEdge(vectorOrd, slot, replicaScores[vectorOrd][slot]));
                    }
                }
            }
            edges.sort((a, b) -> Float.compare(a.score(), b.score()));
            int droppedForCentroid = 0;
            for (ReplicaEdge edge : edges) {
                if (overflow <= 0) {
                    break;
                }
                if (replicaOrds[edge.vectorOrd()][edge.slot()] != centroidOrd) {
                    continue;
                }
                replicaOrds[edge.vectorOrd()][edge.slot()] = NO_SOAR_ASSIGNMENT;
                replicaScores[edge.vectorOrd()][edge.slot()] = Float.NEGATIVE_INFINITY;
                postingCounts[centroidOrd]--;
                overflow--;
                droppedForCentroid++;
                totalDropped++;
            }
            if (overflow > 0) {
                logger.debug(
                    "SPANN-style posting cut could not fully trim centroid [{}], remaining overflow [{}], limit [{}]",
                    centroidOrd,
                    overflow,
                    postingSizeLimit
                );
            }
            if (droppedForCentroid > 0 && logger.isDebugEnabled()) {
                logger.debug(
                    "SPANN-style posting cut trimmed [{}] replicas from centroid [{}] to limit [{}]",
                    droppedForCentroid,
                    centroidOrd,
                    postingSizeLimit
                );
            }
        }
        if (totalDropped > 0 && logger.isInfoEnabled()) {
            logger.info("SPANN-style posting cut removed [{}] replica assignments (postingSizeLimit={})", totalDropped, postingSizeLimit);
        }
    }

    @Override
    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#deleteFilesIgnoringExceptions(...)")
    public CentroidOffsetAndLength buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        MergeState mergeState,
        int[] assignments,
        int[] overspillAssignments
    ) throws IOException {
        int replicaLimit = resolveReplicaLimit();
        if (replicaLimit > 1) {
            return buildAndWritePostingsListsWithNeighborhoodReplicas(
                fieldInfo,
                centroidSupplier,
                floatVectorValues,
                postingsOutput,
                fileOffset,
                assignments,
                replicaLimit
            );
        }
        // first, quantize all the vectors into a temporary file
        var vectorSimilarityFunction = fieldInfo.getVectorSimilarityFunction();
        KMeansResult centroidClusters = centroidSupplier.secondLevelClusters();
        String quantizedVectorsTempName = null;
        try (
            IndexOutput quantizedVectorsTemp = mergeState.segmentInfo.dir.createTempOutput(
                mergeState.segmentInfo.name,
                "qvec_",
                IOContext.DEFAULT
            )
        ) {
            quantizedVectorsTempName = quantizedVectorsTemp.getName();
            OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(vectorSimilarityFunction);
            int[] quantized = new int[quantEncoding.discretizedDimensions(fieldInfo.getVectorDimension())];
            byte[] binary = new byte[quantEncoding.getDocPackedLength(fieldInfo.getVectorDimension())];
            float[] scratch = new float[fieldInfo.getVectorDimension()];
            for (int i = 0; i < assignments.length; i++) {
                int c = assignments[i];
                float[] centroid = centroidSupplier.centroid(c);
                float[] parentCentroid = centroidClusters.getCentroid(c);
                float[] vector = floatVectorValues.vectorValue(i);
                boolean overspill = overspillAssignments.length > i && overspillAssignments[i] != NO_SOAR_ASSIGNMENT;
                OptimizedScalarQuantizer.QuantizationResult result = quantizer.scalarQuantize(
                    vector,
                    scratch,
                    quantized,
                    quantEncoding.bits(),
                    centroid
                );
                if (parentCentroid != null) {
                    float additionalCorrection = vectorSimilarityFunction == VectorSimilarityFunction.EUCLIDEAN
                        ? ESVectorUtil.squareDistance(vector, parentCentroid)
                        : ESVectorUtil.dotProduct(scratch, parentCentroid);
                    result = new OptimizedScalarQuantizer.QuantizationResult(
                        result.lowerInterval(),
                        result.upperInterval(),
                        additionalCorrection,
                        result.quantizedComponentSum()
                    );
                }
                quantEncoding.pack(quantized, binary);
                writeQuantizedValue(quantizedVectorsTemp, binary, result);
                if (overspill) {
                    int s = overspillAssignments[i];
                    float[] overspillCentroid = centroidSupplier.centroid(s);
                    float[] overspillParentCentroid = centroidClusters.getCentroid(s);
                    // write the overspill vector as well
                    result = quantizer.scalarQuantize(vector, scratch, quantized, quantEncoding.bits(), overspillCentroid);
                    if (overspillParentCentroid != null) {
                        float additionalCorrection = vectorSimilarityFunction == VectorSimilarityFunction.EUCLIDEAN
                            ? ESVectorUtil.squareDistance(vector, overspillParentCentroid)
                            : ESVectorUtil.dotProduct(scratch, overspillParentCentroid);
                        result = new OptimizedScalarQuantizer.QuantizationResult(
                            result.lowerInterval(),
                            result.upperInterval(),
                            additionalCorrection,
                            result.quantizedComponentSum()
                        );
                    }
                    quantEncoding.pack(quantized, binary);
                    writeQuantizedValue(quantizedVectorsTemp, binary, result);
                } else {
                    // write a zero vector for the overspill
                    Arrays.fill(binary, (byte) 0);
                    OptimizedScalarQuantizer.QuantizationResult zeroResult = new OptimizedScalarQuantizer.QuantizationResult(0f, 0f, 0f, 0);
                    writeQuantizedValue(quantizedVectorsTemp, binary, zeroResult);
                }
            }
        } catch (Throwable t) {
            if (quantizedVectorsTempName != null) {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, quantizedVectorsTempName);
            }
            throw t;
        }
        int[] centroidVectorCount = new int[centroidSupplier.size()];
        for (int i = 0; i < assignments.length; i++) {
            centroidVectorCount[assignments[i]]++;
            // if soar assignments are present, count them as well
            if (overspillAssignments.length > i && overspillAssignments[i] != NO_SOAR_ASSIGNMENT) {
                centroidVectorCount[overspillAssignments[i]]++;
            }
        }

        int maxPostingListSize = 0;
        int[][] assignmentsByCluster = new int[centroidSupplier.size()][];
        boolean[][] isOverspillByCluster = new boolean[centroidSupplier.size()][];
        for (int c = 0; c < centroidSupplier.size(); c++) {
            int size = centroidVectorCount[c];
            maxPostingListSize = Math.max(maxPostingListSize, size);
            assignmentsByCluster[c] = new int[size];
            isOverspillByCluster[c] = new boolean[size];
        }
        Arrays.fill(centroidVectorCount, 0);

        for (int i = 0; i < assignments.length; i++) {
            int c = assignments[i];
            assignmentsByCluster[c][centroidVectorCount[c]++] = i;
            // if soar assignments are present, add them to the cluster as well
            if (overspillAssignments.length > i) {
                int s = overspillAssignments[i];
                if (s != NO_SOAR_ASSIGNMENT) {
                    assignmentsByCluster[s][centroidVectorCount[s]] = i;
                    isOverspillByCluster[s][centroidVectorCount[s]++] = true;
                }
            }
        }
        // now we can read the quantized vectors from the temporary file
        try (IndexInput quantizedVectorsInput = mergeState.segmentInfo.dir.openInput(quantizedVectorsTempName, IOContext.DEFAULT)) {
            final PackedLongValues.Builder offsets = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
            final PackedLongValues.Builder lengths = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
            OffHeapQuantizedVectors offHeapQuantizedVectors = new OffHeapQuantizedVectors(
                quantizedVectorsInput,
                quantEncoding,
                fieldInfo.getVectorDimension()
            );
            DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(quantEncoding.bits(), BULK_SIZE, postingsOutput, true, true);
            // write the posting lists
            final int[] docIds = new int[maxPostingListSize];
            final int[] docDeltas = new int[maxPostingListSize];
            final int[] clusterOrds = new int[maxPostingListSize];
            DocIdsWriter idsWriter = new DocIdsWriter();
            for (int c = 0; c < centroidSupplier.size(); c++) {
                float[] centroid = centroidSupplier.centroid(c);
                int[] cluster = assignmentsByCluster[c];
                boolean[] isOverspill = isOverspillByCluster[c];
                long offset = postingsOutput.alignFilePointer(Float.BYTES) - fileOffset;
                offsets.add(offset);
                postingsOutput.writeInt(Float.floatToIntBits(ESVectorUtil.squareDistance(centroid, centroidClusters.getCentroid(c))));
                // write docIds
                int size = cluster.length;
                postingsOutput.writeVInt(size);
                for (int j = 0; j < size; j++) {
                    docIds[j] = floatVectorValues.ordToDoc(cluster[j]);
                    clusterOrds[j] = j;
                }
                // sort cluster.buffer by docIds values, this way cluster ordinals are sorted by docIds
                new IntSorter(clusterOrds, i -> docIds[i]).sort(0, size);
                // encode doc deltas
                for (int j = 0; j < size; j++) {
                    docDeltas[j] = j == 0 ? docIds[clusterOrds[j]] : docIds[clusterOrds[j]] - docIds[clusterOrds[j - 1]];
                }
                byte encoding = idsWriter.calculateBlockEncoding(i -> docDeltas[i], size, BULK_SIZE);
                postingsOutput.writeByte(encoding);
                offHeapQuantizedVectors.reset(size, ord -> isOverspill[clusterOrds[ord]], ord -> cluster[clusterOrds[ord]]);
                // write vectors
                bulkWriter.writeVectors(offHeapQuantizedVectors, i -> {
                    // for vector i we write `bulk` size docs or the remaining docs
                    idsWriter.writeDocIds(d -> docDeltas[d + i], Math.min(BULK_SIZE, size - i), encoding, postingsOutput);
                });
                lengths.add(postingsOutput.getFilePointer() - fileOffset - offset);
            }

            if (logger.isDebugEnabled()) {
                printClusterQualityStatistics(assignmentsByCluster);
            }
            return new CentroidOffsetAndLength(offsets.build(), lengths.build());
        } finally {
            org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, quantizedVectorsTempName);
        }
    }

    private static void printClusterQualityStatistics(int[][] clusters) {
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        float mean = 0;
        float m2 = 0;
        // iteratively compute the variance & mean
        int count = 0;
        for (int[] cluster : clusters) {
            count += 1;
            if (cluster == null) {
                continue;
            }
            float delta = cluster.length - mean;
            mean += delta / count;
            m2 += delta * (cluster.length - mean);
            min = Math.min(min, cluster.length);
            max = Math.max(max, cluster.length);
        }
        float variance = m2 / (clusters.length - 1);
        logger.debug(
            "Centroid count: {} min: {} max: {} mean: {} stdDev: {} variance: {}",
            clusters.length,
            min,
            max,
            mean,
            Math.sqrt(variance),
            variance
        );
    }

    @Override
    public CentroidSupplier createCentroidSupplier(
        IndexInput centroidsInput,
        CentroidSlices centroidSlices,
        int numCentroids,
        FieldInfo fieldInfo,
        float[] globalCentroid
    ) throws IOException {
        CentroidSupplier centroidSupplier = new OffHeapCentroidSupplier(
            centroidsInput,
            numCentroids,
            fieldInfo,
            KMeansResult.singleCluster(globalCentroid, numCentroids),
            centroidSlices
        );
        if (centroidSupplier.size() > centroidsPerParentCluster * centroidsPerParentCluster) {
            ClusteringFloatVectorValues floatVectorValues = centroidSupplier.asKmeansFloatVectorValues();
            if (centroidSlices == null) {
                KMeansResult centroidClusters = buildSecondLevelClusters(fieldInfo, floatVectorValues, true);
                return new OffHeapCentroidSupplier(centroidsInput, numCentroids, fieldInfo, centroidClusters, null);
            } else {
                List<KMeansResult> centroidClusters = new ArrayList<>(centroidSlices.sliceOffsets().length);
                int start = 0;
                for (int i = 0; i < centroidSlices.sliceOffsets().length; i++) {
                    final int offset = start;
                    start = centroidSlices.sliceOffsets()[i];
                    int count = start - offset;
                    ClusteringFloatVectorValues slice = new ClusteringFloatVectorValuesSlice(floatVectorValues, j -> offset + j, count);
                    KMeansResult result = buildSecondLevelClusters(fieldInfo, slice, true);
                    centroidClusters.add(result);
                    if (i == 0) {
                        centroidSlices.sliceOffsets()[i] = result.centroids().length;
                    } else {
                        centroidSlices.sliceOffsets()[i] = centroidSlices.sliceOffsets()[i - 1] + result.centroids().length;
                    }
                }
                KMeansResult result = KMeansResult.merge(centroidClusters);
                assert CentroidSlices.assertSliceOffsets(centroidSlices.sliceOffsets(), result.centroids().length);
                return new OffHeapCentroidSupplier(centroidsInput, numCentroids, fieldInfo, result, centroidSlices);
            }
        }
        return centroidSupplier;
    }

    @Override
    public CentroidSupplier createCentroidSupplier(FieldInfo info, float[][] centroids, float[] globalCentroid) throws IOException {
        CentroidSupplier centroidSupplier = CentroidSupplier.fromArray(
            centroids,
            KMeansResult.singleCluster(globalCentroid, centroids.length),
            info.getVectorDimension()
        );
        if (centroidSupplier.size() > centroidsPerParentCluster * centroidsPerParentCluster) {
            KMeansResult centroidClusters = buildSecondLevelClusters(info, centroidSupplier.asKmeansFloatVectorValues(), false);
            return CentroidSupplier.fromArray(centroids, centroidClusters, info.getVectorDimension());
        }
        return centroidSupplier;
    }

    @Override
    protected void doWriteMeta(
        IndexOutput metaOutput,
        FieldInfo field,
        int numCentroids,
        long preconditionerOffset,
        long preconditionerLength,
        int numberOfSlices,
        int maxSliceSize
    ) throws IOException {
        metaOutput.writeInt(ES940OSQVectorsScorer.BULK_SIZE);
        metaOutput.writeInt(quantEncoding.id());
        metaOutput.writeLong(preconditionerLength);
        if (preconditionerLength > 0) {
            metaOutput.writeLong(preconditionerOffset);
        }
        if (sliceField == null) {
            assert numberOfSlices == 0;
            metaOutput.writeInt(-1);
        } else {
            metaOutput.writeInt(numberOfSlices);
            if (numberOfSlices > 0) {
                metaOutput.writeVInt(maxSliceSize);
            }
        }
        GraphSection graphSection = graphSections.remove(field.number);
        metaOutput.writeInt(centroidSearchMode.id());
        if (graphSection == null) {
            metaOutput.writeLong(-1L);
            metaOutput.writeLong(0L);
        } else {
            metaOutput.writeLong(graphSection.offset());
            metaOutput.writeLong(graphSection.length());
        }
    }

    @Override
    public void writeCentroids(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        int[] centroidAssignments,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        IndexOutput centroidOutput
    ) throws IOException {
        doWriteCentroids(fieldInfo, centroidSupplier, centroidAssignments, globalCentroid, centroidOffsetAndLength, centroidOutput);
    }

    @Override
    public void writeCentroids(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        int[] centroidAssignments,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        IndexOutput centroidOutput,
        MergeState mergeState
    ) throws IOException {
        doWriteCentroids(fieldInfo, centroidSupplier, centroidAssignments, globalCentroid, centroidOffsetAndLength, centroidOutput);
    }

    private record CentroidGroups(float[][] centroids, int[][] vectors, int maxVectorsPerCentroidLength) {}

    private record OrderedCentroidData(int[] centroidOrdinals, int[] parentOrdinals) {}

    private record GraphSection(long offset, long length) {}

    private void doWriteCentroids(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        int[] centroidAssignments,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        IndexOutput centroidOutput
    ) throws IOException {
        CentroidSlices centroidSlices = centroidSupplier.slices();
        if (centroidSlices != null) {
            int numSlices = centroidSlices.sliceNumVectors().length;
            int maxSlice = centroidSlices.maxSliceSize();
            int bits = DirectWriter.bitsRequired(maxSlice);
            DirectWriter writer = DirectWriter.getInstance(centroidOutput, numSlices, bits);
            for (int i = 0; i < centroidSlices.sliceNumVectors().length; i++) {
                writer.add(centroidSlices.sliceNumVectors()[i]);
            }
            writer.finish();
        }
        final OrderedCentroidData orderedCentroidData;
        if (centroidSupplier.secondLevelClusters().centroidsSupplier().size() > 1) {
            final CentroidGroups centroidGroups = buildCentroidGroups(centroidSupplier.secondLevelClusters());
            {
                // write vector ord -> centroid lookup table. We need to remap current centroid ordinals
                // to the ordinals on the parent / child structure.
                final int[] centroidOrdinalMap = new int[centroidSupplier.size()];
                int idx = 0;
                for (int[] centroidVectors : centroidGroups.vectors()) {
                    for (int assignment : centroidVectors) {
                        centroidOrdinalMap[assignment] = idx++;
                    }
                }
                assert idx == centroidSupplier.size() : "Expected [" + centroidSupplier.size() + "], got [" + idx + "]";
                writeCentroidLookup(centroidOutput, centroidAssignments, i -> centroidOrdinalMap[i], centroidSupplier.size());
            }
            orderedCentroidData = writeCentroidsWithParents(
                fieldInfo,
                centroidSupplier,
                globalCentroid,
                centroidOffsetAndLength,
                centroidOutput,
                centroidGroups
            );
        } else {
            writeCentroidLookup(centroidOutput, centroidAssignments, IntUnaryOperator.identity(), centroidSupplier.size());
            orderedCentroidData = writeCentroidsWithoutParents(
                fieldInfo,
                centroidSupplier,
                globalCentroid,
                centroidOffsetAndLength,
                centroidOutput
            );
        }
        if (centroidSearchMode == ESNextDiskBBQVectorsFormat.CentroidSearchMode.HNSW_4BIT) {
            IndexOutput graphOutput = getAuxiliaryOutput();
            if (graphOutput == null) {
                throw new IllegalStateException("centroid graph mode enabled without auxiliary graph output");
            }
            long graphOffset = graphOutput.getFilePointer();
            writeCentroidGraphSection(
                fieldInfo,
                centroidSupplier,
                globalCentroid,
                centroidOffsetAndLength,
                orderedCentroidData,
                graphOutput
            );
            long graphLength = graphOutput.getFilePointer() - graphOffset;
            graphSections.put(fieldInfo.number, new GraphSection(graphOffset, graphLength));
        }
    }

    private void writeCentroidLookup(IndexOutput out, int[] centroidAssignments, IntUnaryOperator OrdinalMap, int numberCentroids)
        throws IOException {
        final int bitsRequired = DirectWriter.bitsRequired(numberCentroids);
        final long bytesRequired = DirectWriter.bytesRequired(centroidAssignments.length, bitsRequired);
        final ByteBuffersDataOutput memory = new ByteBuffersDataOutput(bytesRequired);
        final DirectWriter writer = DirectWriter.getInstance(memory, centroidAssignments.length, bitsRequired);
        for (int centroidAssignment : centroidAssignments) {
            writer.add(OrdinalMap.applyAsInt(centroidAssignment));
        }
        writer.finish();
        out.copyBytes(memory.toDataInput(), memory.size());
    }

    private void writeSlicesOffsets(IndexOutput out, CentroidSlices centroidSlices) throws IOException {
        if (centroidSlices == null) {
            return;
        }
        // TODO: should we compress slice offsets?
        for (int offset : centroidSlices.sliceOffsets()) {
            out.writeInt(offset);
        }
    }

    private OrderedCentroidData writeCentroidsWithParents(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        IndexOutput centroidOutput,
        CentroidGroups centroidGroups
    ) throws IOException {
        DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(7, BULK_SIZE, centroidOutput, true, true);
        final OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        centroidOutput.writeVInt(centroidGroups.centroids().length);
        writeSlicesOffsets(centroidOutput, centroidSupplier.slices());
        centroidOutput.writeVInt(centroidGroups.maxVectorsPerCentroidLength());
        // let's also write the raw parent centroids
        final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < centroidGroups.centroids().length; i++) {
            float[] centroid = centroidGroups.centroids()[i];
            buffer.asFloatBuffer().put(centroid);
            centroidOutput.writeBytes(buffer.array(), buffer.array().length);
        }
        QuantizedCentroids parentQuantizeCentroid = new QuantizedCentroids(
            CentroidSupplier.fromArray(centroidGroups.centroids, KMeansResult.EMPTY, fieldInfo.getVectorDimension()),
            fieldInfo.getVectorDimension(),
            osq,
            globalCentroid
        );
        bulkWriter.writeVectors(parentQuantizeCentroid, null);
        int offset = 0;
        for (int[] centroidVectors : centroidGroups.vectors()) {
            centroidOutput.writeInt(offset);
            centroidOutput.writeInt(centroidVectors.length);
            offset += centroidVectors.length;
        }

        QuantizedCentroids childrenQuantizeCentroid = new QuantizedCentroids(
            centroidSupplier,
            fieldInfo.getVectorDimension(),
            osq,
            globalCentroid
        );
        for (int[] centroidVectors : centroidGroups.vectors()) {
            childrenQuantizeCentroid.reset(idx -> centroidVectors[idx], centroidVectors.length);
            bulkWriter.writeVectors(childrenQuantizeCentroid, null);
        }
        // write the centroid offsets at the end of the file
        int parentOrd = 0;
        int[] orderedCentroidOrds = new int[centroidSupplier.size()];
        int[] parentOrds = new int[centroidSupplier.size()];
        int orderedOrdinal = 0;
        for (int[] centroidVectors : centroidGroups.vectors()) {
            for (int assignment : centroidVectors) {
                centroidOutput.writeLong(centroidOffsetAndLength.offsets().get(assignment));
                centroidOutput.writeLong(centroidOffsetAndLength.lengths().get(assignment));
                centroidOutput.writeInt(parentOrd);
                orderedCentroidOrds[orderedOrdinal] = assignment;
                parentOrds[orderedOrdinal++] = parentOrd;
            }
            parentOrd++;
        }
        return new OrderedCentroidData(orderedCentroidOrds, parentOrds);
    }

    private OrderedCentroidData writeCentroidsWithoutParents(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        IndexOutput centroidOutput
    ) throws IOException {
        centroidOutput.writeVInt(0);
        writeSlicesOffsets(centroidOutput, centroidSupplier.slices());
        DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(7, BULK_SIZE, centroidOutput, true, true);
        final OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        QuantizedCentroids quantizedCentroids = new QuantizedCentroids(
            centroidSupplier,
            fieldInfo.getVectorDimension(),
            osq,
            globalCentroid
        );
        bulkWriter.writeVectors(quantizedCentroids, null);
        // write the centroid offsets at the end of the file
        int[] orderedCentroidOrds = new int[centroidSupplier.size()];
        int[] parentOrds = new int[centroidSupplier.size()];
        Arrays.fill(parentOrds, PostingMetadata.NO_ORDINAL);
        for (int i = 0; i < centroidSupplier.size(); i++) {
            centroidOutput.writeLong(centroidOffsetAndLength.offsets().get(i));
            centroidOutput.writeLong(centroidOffsetAndLength.lengths().get(i));
            orderedCentroidOrds[i] = i;
        }
        return new OrderedCentroidData(orderedCentroidOrds, parentOrds);
    }

    private void writeCentroidGraphSection(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        OrderedCentroidData orderedCentroidData,
        IndexOutput centroidOutput
    ) throws IOException {
        int numCentroids = orderedCentroidData.centroidOrdinals().length;
        centroidOutput.writeVInt(numCentroids);
        int dimension = fieldInfo.getVectorDimension();
        ESNextDiskBBQVectorsFormat.QuantEncoding graphEncoding = ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC;
        int vectorByteLength = graphEncoding.getDocPackedLength(dimension);
        centroidOutput.writeVInt(vectorByteLength);
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        int[] quantizedScratch = new int[graphEncoding.discretizedDimensions(dimension)];
        float[] transformedScratch = new float[dimension];
        byte[] packed = new byte[vectorByteLength];
        float[][] orderedCentroids = new float[numCentroids][dimension];
        for (int i = 0; i < numCentroids; i++) {
            int originalOrd = orderedCentroidData.centroidOrdinals()[i];
            float[] centroid = centroidSupplier.centroid(originalOrd);
            orderedCentroids[i] = Arrays.copyOf(centroid, centroid.length);
            OptimizedScalarQuantizer.QuantizationResult corrections = quantizer.scalarQuantize(
                centroid,
                transformedScratch,
                quantizedScratch,
                (byte) 4,
                globalCentroid
            );
            graphEncoding.pack(quantizedScratch, packed);
            centroidOutput.writeBytes(packed, 0, packed.length);
            centroidOutput.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
            centroidOutput.writeInt(Float.floatToIntBits(corrections.upperInterval()));
            centroidOutput.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
            centroidOutput.writeInt(corrections.quantizedComponentSum());
        }
        for (int i = 0; i < numCentroids; i++) {
            int originalOrd = orderedCentroidData.centroidOrdinals()[i];
            centroidOutput.writeLong(centroidOffsetAndLength.offsets().get(originalOrd));
            centroidOutput.writeLong(centroidOffsetAndLength.lengths().get(originalOrd));
            centroidOutput.writeInt(orderedCentroidData.parentOrdinals()[i]);
        }
        OnHeapHnswGraph graph = HnswGraphBuilder.create(
            new CentroidScorerSupplier(orderedCentroids, fieldInfo.getVectorSimilarityFunction()),
            CENTROID_GRAPH_HNSW_M,
            CENTROID_GRAPH_HNSW_BEAM_WIDTH,
            42L,
            numCentroids
        ).build(numCentroids);
        writeGraphSectionLikeLucene(numCentroids, graph, centroidOutput);
    }

    private static void writeGraphSectionLikeLucene(int numCentroids, OnHeapHnswGraph graph, IndexOutput out) throws IOException {
        out.writeVInt(graph.numLevels());
        out.writeVInt(graph.entryNode());
        out.writeVInt(graph.maxConn());
        int[][] levelNodes = new int[graph.numLevels()][];
        long totalOrdinals = numCentroids;
        for (int level = 1; level < graph.numLevels(); level++) {
            int[] nodes = sortedNodes(graph, level);
            levelNodes[level] = nodes;
            totalOrdinals += nodes.length;
            out.writeVInt(nodes.length);
            for (int i = 0; i < nodes.length; i++) {
                int delta = i == 0 ? nodes[i] : nodes[i] - nodes[i - 1];
                out.writeVInt(delta);
            }
        }
        long[] adjacencyOffsets = new long[Math.toIntExact(totalOrdinals)];
        int[] neighborScratch = new int[graph.maxConn() * 2];
        long currentAdjacencyOffset = 0L;
        int ordinalIndex = 0;
        for (int node = 0; node < numCentroids; node++) {
            neighborScratch = ensureNeighborScratch(neighborScratch, graph.getNeighbors(0, node).size());
            adjacencyOffsets[ordinalIndex++] = currentAdjacencyOffset;
            currentAdjacencyOffset += encodedNeighborListSize(graph.getNeighbors(0, node), neighborScratch);
        }
        for (int level = 1; level < graph.numLevels(); level++) {
            int[] nodes = levelNodes[level];
            for (int node : nodes) {
                neighborScratch = ensureNeighborScratch(neighborScratch, graph.getNeighbors(level, node).size());
                adjacencyOffsets[ordinalIndex++] = currentAdjacencyOffset;
                currentAdjacencyOffset += encodedNeighborListSize(graph.getNeighbors(level, node), neighborScratch);
            }
        }
        out.writeVInt(adjacencyOffsets.length);
        for (long adjacencyOffset : adjacencyOffsets) {
            out.writeLong(adjacencyOffset);
        }
        for (int node = 0; node < numCentroids; node++) {
            neighborScratch = ensureNeighborScratch(neighborScratch, graph.getNeighbors(0, node).size());
            writeDeltaEncodedNeighborList(graph.getNeighbors(0, node), out, neighborScratch);
        }
        for (int level = 1; level < graph.numLevels(); level++) {
            int[] nodes = levelNodes[level];
            for (int node : nodes) {
                neighborScratch = ensureNeighborScratch(neighborScratch, graph.getNeighbors(level, node).size());
                writeDeltaEncodedNeighborList(graph.getNeighbors(level, node), out, neighborScratch);
            }
        }
    }

    private static int[] sortedNodes(OnHeapHnswGraph graph, int level) {
        var iterator = graph.getNodesOnLevel(level);
        int[] nodes = new int[iterator.size()];
        int consumed = iterator.consume(nodes);
        assert consumed == nodes.length;
        Arrays.sort(nodes);
        return nodes;
    }

    private static long encodedNeighborListSize(NeighborArray neighbors, int[] scratch) {
        int uniqueCount = toSortedUniqueDeltaEncoded(neighbors, scratch);
        long size = vIntLength(uniqueCount);
        for (int i = 0; i < uniqueCount; i++) {
            size += vIntLength(scratch[i]);
        }
        return size;
    }

    private static void writeDeltaEncodedNeighborList(NeighborArray neighbors, IndexOutput out, int[] scratch) throws IOException {
        int uniqueCount = toSortedUniqueDeltaEncoded(neighbors, scratch);
        out.writeVInt(uniqueCount);
        for (int i = 0; i < uniqueCount; i++) {
            out.writeVInt(scratch[i]);
        }
    }

    private static int toSortedUniqueDeltaEncoded(NeighborArray neighbors, int[] scratch) {
        int size = neighbors.size();
        int[] raw = neighbors.nodes();
        for (int i = 0; i < size; i++) {
            scratch[i] = raw[i];
        }
        Arrays.sort(scratch, 0, size);
        if (size == 0) {
            return 0;
        }
        int uniqueCount = 1;
        int previous = scratch[0];
        scratch[0] = previous;
        for (int i = 1; i < size; i++) {
            int current = scratch[i];
            if (current == previous) {
                continue;
            }
            scratch[uniqueCount++] = current - previous;
            previous = current;
        }
        return uniqueCount;
    }

    private static int[] ensureNeighborScratch(int[] scratch, int required) {
        if (required <= scratch.length) {
            return scratch;
        }
        return Arrays.copyOf(scratch, required);
    }

    private static int vIntLength(int value) {
        assert value >= 0;
        if ((value & ~0x7F) == 0) {
            return 1;
        }
        if ((value & ~0x3FFF) == 0) {
            return 2;
        }
        if ((value & ~0x1FFFFF) == 0) {
            return 3;
        }
        if ((value & ~0xFFFFFFF) == 0) {
            return 4;
        }
        return 5;
    }

    private static class CentroidScorerSupplier implements RandomVectorScorerSupplier {
        private final float[][] centroids;
        private final VectorSimilarityFunction similarityFunction;

        private CentroidScorerSupplier(float[][] centroids, VectorSimilarityFunction similarityFunction) {
            this.centroids = centroids;
            this.similarityFunction = similarityFunction;
        }

        @Override
        public UpdateableRandomVectorScorer scorer() {
            return new UpdateableRandomVectorScorer() {
                private int scoringOrdinal;

                @Override
                public float score(int node) {
                    return similarityFunction.compare(centroids[scoringOrdinal], centroids[node]);
                }

                @Override
                public float bulkScore(int[] nodes, float[] scores, int numNodes) {
                    float maxScore = Float.NEGATIVE_INFINITY;
                    for (int i = 0; i < numNodes; i++) {
                        float score = score(nodes[i]);
                        scores[i] = score;
                        maxScore = Math.max(maxScore, score);
                    }
                    return maxScore;
                }

                @Override
                public int maxOrd() {
                    return centroids.length;
                }

                @Override
                public void setScoringOrdinal(int scoringOrdinal) {
                    this.scoringOrdinal = scoringOrdinal;
                }
            };
        }

        @Override
        public RandomVectorScorerSupplier copy() {
            return new CentroidScorerSupplier(centroids, similarityFunction);
        }
    }

    private KMeansResult buildSecondLevelClusters(FieldInfo fieldInfo, ClusteringFloatVectorValues floatVectorValues, boolean isMerge)
        throws IOException {
        // we use the HierarchicalKMeans to partition the space of all vectors across merging segments
        // this are small numbers so we run it wih all the centroids.
        HierarchicalKMeans hierarchicalKMeans;
        if (isMerge && mergeExec != null) {
            hierarchicalKMeans = HierarchicalKMeans.ofConcurrent(
                fieldInfo.getVectorDimension(),
                mergeExec,
                numMergeWorkers,
                HierarchicalKMeans.MAX_ITERATIONS_DEFAULT,
                HierarchicalKMeans.SAMPLES_PER_CLUSTER_DEFAULT,
                HierarchicalKMeans.MAXK,
                -1 // disable SOAR assignments
            );
        } else {
            hierarchicalKMeans = HierarchicalKMeans.ofSerial(
                fieldInfo.getVectorDimension(),
                HierarchicalKMeans.MAX_ITERATIONS_DEFAULT,
                HierarchicalKMeans.SAMPLES_PER_CLUSTER_DEFAULT,
                HierarchicalKMeans.MAXK,
                -1 // disable SOAR assignments
            );
        }
        return hierarchicalKMeans.cluster(floatVectorValues, centroidsPerParentCluster);
    }

    private CentroidGroups buildCentroidGroups(KMeansResult kMeansResult) {
        final int[] centroidVectorCount = new int[kMeansResult.centroids().length];
        for (int i = 0; i < kMeansResult.assignments().length; i++) {
            centroidVectorCount[kMeansResult.assignments()[i]]++;
        }
        final int[][] vectorsPerCentroid = new int[kMeansResult.centroids().length][];
        int maxVectorsPerCentroidLength = 0;
        for (int i = 0; i < kMeansResult.centroidsSupplier().size(); i++) {
            vectorsPerCentroid[i] = new int[centroidVectorCount[i]];
            maxVectorsPerCentroidLength = Math.max(maxVectorsPerCentroidLength, centroidVectorCount[i]);
        }
        Arrays.fill(centroidVectorCount, 0);
        for (int i = 0; i < kMeansResult.assignments().length; i++) {
            final int c = kMeansResult.assignments()[i];
            vectorsPerCentroid[c][centroidVectorCount[c]++] = i;
        }
        return new CentroidGroups(kMeansResult.centroids(), vectorsPerCentroid, maxVectorsPerCentroidLength);
    }

    @Override
    public CentroidAssignments calculateCentroids(FieldInfo fieldInfo, KMeansFloatVectorValues floatVectorValues, MergeState mergeState)
        throws IOException {
        // TODO: consider hinting / bootstrapping hierarchical kmeans with the prior segments centroids
        // TODO: for flush we are doing this over the vectors and here centroids which seems duplicative
        // preliminary tests suggest recall is good using only centroids but need to do further evaluation
        HierarchicalKMeans hierarchicalKMeans = buildPrimaryClusters(floatVectorValues.dimension(), mergeExec != null);
        if (sliceField == null) { // no slice
            KMeansResult kMeansResult = calculateCentroids(hierarchicalKMeans, floatVectorValues);
            if (logger.isDebugEnabled()) {
                logger.debug("final centroid count: {}", kMeansResult.centroids().length);
            }
            int[] overspillAssignments = centroidSearchMode == ESNextDiskBBQVectorsFormat.CentroidSearchMode.HNSW_4BIT
                ? new int[0]
                : kMeansResult.soarAssignments();
            return new CentroidAssignments(
                fieldInfo.getVectorDimension(),
                kMeansResult.centroids(),
                kMeansResult.assignments(),
                overspillAssignments
            );
        } else {
            final FieldInfo slicedFieldInfo = mergeState.mergeFieldInfos.fieldInfo(sliceField);
            assert slicedFieldInfo != null;
            assert slicedFieldInfo.getDocValuesType() == DocValuesType.SORTED : "sliceField must be SortedDocValues";
            final SortedDocValues values = DocValueConsumerHelper.INSTANCE.getMergeSortedField(slicedFieldInfo, mergeState);
            final int numSlices = values.getValueCount();
            final KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
            iterator.advance(0);
            values.nextDoc();
            // slice field must be dense populated, but we might have documents without a vector.
            final int[] sliceOffsets = new int[numSlices];
            final int[] sliceLengths = new int[numSlices];
            List<KMeansResult> kmeansResults = new ArrayList<>();
            for (int i = 0; i < numSlices; i++) {
                if (iterator.docID() == DocIdSetIterator.NO_MORE_DOCS) {
                    // no more vectors, we are done
                    sliceLengths[i] = 0;
                    sliceOffsets[i] = i == 0 ? 0 : sliceOffsets[i - 1];
                    continue;
                }
                // get start and end of an slice
                int sliceDocStart = values.docID();
                while (values.docID() != DocIdSetIterator.NO_MORE_DOCS && values.ordValue() == i) {
                    values.nextDoc();
                }
                final int sliceDocEnd = values.docID();
                // get the vector ordinals for the slice
                int vectorDocStart = iterator.docID();
                if (vectorDocStart < sliceDocStart) {
                    // advance iterator to the beginning of the slice
                    vectorDocStart = iterator.advance(sliceDocStart);
                }
                if (vectorDocStart > sliceDocEnd) {
                    // no vectors in this slice
                    sliceLengths[i] = 0;
                    sliceOffsets[i] = i == 0 ? 0 : sliceOffsets[i - 1];
                    continue;
                }
                final int vectorOrdStart = iterator.index();
                final int docEnd = vectorDocStart == sliceDocEnd ? sliceDocEnd : iterator.advance(sliceDocEnd);
                final int vectorOrdEnd = docEnd == KnnVectorValues.DocIndexIterator.NO_MORE_DOCS
                    ? floatVectorValues.size()
                    : iterator.index();
                final int sliceNumVectors = vectorOrdEnd - vectorOrdStart;
                final ClusteringFloatVectorValuesSlice slice = new ClusteringFloatVectorValuesSlice(
                    floatVectorValues,
                    j -> vectorOrdStart + j,
                    sliceNumVectors
                );
                final KMeansResult kMeansResult = calculateCentroids(hierarchicalKMeans, slice);
                kmeansResults.add(kMeansResult);
                sliceLengths[i] = sliceNumVectors;
                sliceOffsets[i] = i == 0 ? kMeansResult.centroids().length : sliceOffsets[i - 1] + kMeansResult.centroids().length;
            }
            final KMeansResult merged = KMeansResult.merge(kmeansResults);
            if (logger.isDebugEnabled()) {
                logger.debug("final centroid count: {}", merged.centroids().length);
            }
            final CentroidSlices centroidSlices = new CentroidSlices(sliceOffsets, sliceLengths);
            int[] overspillAssignments = centroidSearchMode == ESNextDiskBBQVectorsFormat.CentroidSearchMode.HNSW_4BIT
                ? new int[0]
                : merged.soarAssignments();
            return new CentroidAssignments(
                floatVectorValues.dimension(),
                merged.centroids(),
                merged.assignments(),
                overspillAssignments,
                centroidSlices
            );
        }
    }

    // This class helps to access the merged view of a slice.
    private static class DocValueConsumerHelper extends DocValuesConsumer {

        static final DocValueConsumerHelper INSTANCE = new DocValueConsumerHelper();

        public SortedDocValues getMergeSortedField(FieldInfo fieldInfo, final MergeState mergeState) throws IOException {
            // This is the magic to get a merged view from the segments.
            final OrdinalMap map = createOrdinalMapForSortedDV(fieldInfo, mergeState);
            return getMergedSortedSetDocValues(fieldInfo, mergeState, map);
        }

        @Override
        public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new AssertionError("Method should not be called");
        }

        @Override
        public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new AssertionError("Method should not be called");
        }

        @Override
        public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new AssertionError("Method should not be called");
        }

        @Override
        public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new AssertionError("Method should not be called");
        }

        @Override
        public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new AssertionError("Method should not be called");
        }

        @Override
        public void close() {
            throw new AssertionError("Method should not be called");
        }
    }

    /**
     * Calculate the centroids for the given field.
     * We use the {@link HierarchicalKMeans} algorithm to partition the space of all vectors across merging segments
     *
     * @param fieldInfo merging field info
     * @param floatVectorValues the float vector values to merge
     * @return the vector assignments, soar assignments, and if asked the centroids themselves that were computed
     * @throws IOException if an I/O error occurs
     */
    @Override
    public CentroidAssignments calculateCentroids(FieldInfo fieldInfo, KMeansFloatVectorValues floatVectorValues) throws IOException {
        if (sliceField != null) {
            // for sliced indexed, we don't cluster the data during flush so we can search our vectors by docId range
            return buildFlatCentroidAssignments(fieldInfo, floatVectorValues);
        }
        HierarchicalKMeans hierarchicalKMeans = buildPrimaryClusters(floatVectorValues.dimension(), false);
        KMeansResult kMeansResult = calculateCentroids(hierarchicalKMeans, floatVectorValues);
        if (logger.isDebugEnabled()) {
            logger.debug("final centroid count: {}", kMeansResult.centroids().length);
        }
        int[] overspillAssignments = centroidSearchMode == ESNextDiskBBQVectorsFormat.CentroidSearchMode.HNSW_4BIT
            ? new int[0]
            : kMeansResult.soarAssignments();
        return new CentroidAssignments(
            fieldInfo.getVectorDimension(),
            kMeansResult.centroids(),
            kMeansResult.assignments(),
            overspillAssignments
        );
    }

    private HierarchicalKMeans buildPrimaryClusters(int dimension, boolean isMerge) {
        if (centroidSearchMode == ESNextDiskBBQVectorsFormat.CentroidSearchMode.HNSW_4BIT) {
            if (isMerge && mergeExec != null) {
                return HierarchicalKMeans.ofConcurrent(
                    dimension,
                    mergeExec,
                    numMergeWorkers,
                    HierarchicalKMeans.MAX_ITERATIONS_DEFAULT,
                    HierarchicalKMeans.SAMPLES_PER_CLUSTER_DEFAULT,
                    HierarchicalKMeans.MAXK,
                    -1 // disable SOAR assignments for HNSW centroid indexing mode
                );
            }
            return HierarchicalKMeans.ofSerial(
                dimension,
                HierarchicalKMeans.MAX_ITERATIONS_DEFAULT,
                HierarchicalKMeans.SAMPLES_PER_CLUSTER_DEFAULT,
                HierarchicalKMeans.MAXK,
                -1 // disable SOAR assignments for HNSW centroid indexing mode
            );
        }
        if (isMerge && mergeExec != null) {
            return HierarchicalKMeans.ofConcurrent(dimension, mergeExec, numMergeWorkers);
        }
        return HierarchicalKMeans.ofSerial(dimension);
    }

    private float[][] materializeCentroids(CentroidSupplier centroidSupplier) throws IOException {
        float[][] centroids = new float[centroidSupplier.size()][];
        for (int i = 0; i < centroids.length; i++) {
            centroids[i] = centroidSupplier.centroid(i).clone();
        }
        return centroids;
    }

    private ReplicaAssignments buildReplicaAssignmentsByNeighborhood(
        FloatVectorValues vectors,
        VectorSimilarityFunction similarityFunction,
        int[] assignments,
        float[][] centroids,
        int replicaLimit,
        ReplicaAssignmentSettings settings
    ) throws IOException {
        if (centroids.length <= 1 || replicaLimit <= 0) {
            return ReplicaAssignments.empty(assignments.length);
        }
        int cappedReplicaLimit = Math.min(replicaLimit, centroids.length - 1);
        OnHeapHnswGraph centroidGraph = HnswGraphBuilder.create(
            new CentroidScorerSupplier(centroids, similarityFunction),
            CENTROID_GRAPH_HNSW_M,
            CENTROID_GRAPH_HNSW_BEAM_WIDTH,
            42L,
            centroids.length
        ).build(centroids.length);

        int[][] replicas = new int[assignments.length][];
        float[][] replicaScores = new float[assignments.length][];
        for (int i = 0; i < assignments.length; i++) {
            int primaryCentroid = assignments[i];
            float[] vector = vectors.vectorValue(i);
            ScoreDoc[] candidates = collectCandidateCentroidsFromGraph(
                vector,
                centroids,
                similarityFunction,
                centroidGraph,
                settings.internalResultNum()
            );
            if (candidates.length == 0) {
                replicas[i] = new int[0];
                replicaScores[i] = new float[0];
                continue;
            }

            int[] selected = new int[Math.min(cappedReplicaLimit, candidates.length)];
            float[] selectedScores = new float[selected.length];
            int selectedSize = 0;
            for (ScoreDoc candidate : candidates) {
                int centroidOrd = candidate.doc;
                if (centroidOrd == primaryCentroid) {
                    continue;
                }
                float candidateDistance = ESVectorUtil.squareDistance(vector, centroids[centroidOrd]);
                boolean accepted = true;
                for (int j = 0; j < selectedSize; j++) {
                    int selectedCentroid = selected[j];
                    float interCentroidDistance = ESVectorUtil.squareDistance(centroids[centroidOrd], centroids[selectedCentroid]);
                    if (settings.rngFactor() * interCentroidDistance < candidateDistance) {
                        accepted = false;
                        break;
                    }
                }
                if (accepted) {
                    selected[selectedSize] = centroidOrd;
                    selectedScores[selectedSize] = -candidateDistance;
                    selectedSize++;
                    if (selectedSize == selected.length) {
                        break;
                    }
                }
            }
            replicas[i] = selectedSize == selected.length ? selected : Arrays.copyOf(selected, selectedSize);
            replicaScores[i] = selectedSize == selectedScores.length ? selectedScores : Arrays.copyOf(selectedScores, selectedSize);
        }
        return new ReplicaAssignments(replicas, replicaScores);
    }

    private static ScoreDoc[] collectCandidateCentroidsFromGraph(
        float[] vector,
        float[][] centroids,
        VectorSimilarityFunction similarityFunction,
        OnHeapHnswGraph graph,
        int internalResultNum
    ) throws IOException {
        int candidateCount = Math.max(1, Math.min(internalResultNum, centroids.length));
        UpdateableRandomVectorScorer scorer = new UpdateableRandomVectorScorer() {
            @Override
            public float score(int node) {
                return similarityFunction.compare(vector, centroids[node]);
            }

            @Override
            public float bulkScore(int[] nodes, float[] scores, int numNodes) {
                float max = Float.NEGATIVE_INFINITY;
                for (int i = 0; i < numNodes; i++) {
                    float s = score(nodes[i]);
                    scores[i] = s;
                    max = Math.max(max, s);
                }
                return max;
            }

            @Override
            public int maxOrd() {
                return centroids.length;
            }

            @Override
            public void setScoringOrdinal(int node) {
                // no-op; scorer is bound to the query vector
            }
        };
        var collector = HnswGraphSearcher.search(scorer, candidateCount, graph, null, Integer.MAX_VALUE);
        var topDocs = collector.topDocs();
        return topDocs == null ? new ScoreDoc[0] : topDocs.scoreDocs;
    }

    private int resolveReplicaLimit() {
        int defaultReplicaLimit = centroidSearchMode == ESNextDiskBBQVectorsFormat.CentroidSearchMode.HNSW_4BIT
            ? HNSW_REPLICA_LIMIT
            : REGULAR_IVF_REPLICA_LIMIT_ABLATION;
        String configuredValue = System.getProperty(SYSTEM_PROPERTY_IVF_REPLICA_LIMIT);
        if (configuredValue == null) {
            return defaultReplicaLimit;
        }
        try {
            return Math.max(0, Integer.parseInt(configuredValue));
        } catch (NumberFormatException e) {
            logger.warn(
                "Ignoring invalid value [{}] for system property [{}]; using default [{}]",
                configuredValue,
                SYSTEM_PROPERTY_IVF_REPLICA_LIMIT,
                defaultReplicaLimit
            );
            return defaultReplicaLimit;
        }
    }

    private ReplicaAssignmentSettings resolveReplicaAssignmentSettings(int replicaLimit) {
        int internalResultNum = parseIntProperty(SYSTEM_PROPERTY_IVF_REPLICA_INTERNAL_RESULT_NUM, SPANN_INTERNAL_RESULT_NUM_DEFAULT);
        float rngFactor = parseFloatProperty(SYSTEM_PROPERTY_IVF_REPLICA_RNG_FACTOR, SPANN_RNG_FACTOR_DEFAULT);
        float postingLimitMultiplier = parseFloatProperty(
            SYSTEM_PROPERTY_IVF_REPLICA_POSTING_LIMIT_MULTIPLIER,
            SPANN_POSTING_LIMIT_MULTIPLIER_DEFAULT
        );
        if (logger.isDebugEnabled()) {
            logger.debug(
                "SPANN-style replica assignment config: replicaLimit={}, internalResultNum={}, rngFactor={}, postingLimitMultiplier={}",
                replicaLimit,
                internalResultNum,
                rngFactor,
                postingLimitMultiplier
            );
        }
        return new ReplicaAssignmentSettings(Math.max(1, internalResultNum), Math.max(0f, rngFactor), Math.max(0f, postingLimitMultiplier));
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

    private record ReplicaAssignments(int[][] replicaOrds, float[][] replicaScores) {
        private static ReplicaAssignments empty(int size) {
            int[][] replicaOrds = new int[size][];
            float[][] replicaScores = new float[size][];
            for (int i = 0; i < size; i++) {
                replicaOrds[i] = new int[0];
                replicaScores[i] = new float[0];
            }
            return new ReplicaAssignments(replicaOrds, replicaScores);
        }
    }

    private record ReplicaAssignmentSettings(int internalResultNum, float rngFactor, float postingLimitMultiplier) {}

    private record ReplicaEdge(int vectorOrd, int slot, float score) {}

    private KMeansResult calculateCentroids(HierarchicalKMeans hierarchicalKMeans, ClusteringFloatVectorValues floatVectorValues)
        throws IOException {
        return hierarchicalKMeans.cluster(floatVectorValues, vectorPerCluster);
    }

    static void writeQuantizedValue(IndexOutput indexOutput, byte[] binaryValue, OptimizedScalarQuantizer.QuantizationResult corrections)
        throws IOException {
        indexOutput.writeBytes(binaryValue, binaryValue.length);
        indexOutput.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
        indexOutput.writeInt(Float.floatToIntBits(corrections.upperInterval()));
        indexOutput.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
        indexOutput.writeInt(corrections.quantizedComponentSum());
    }

    static class OffHeapCentroidSupplier implements CentroidSupplier {
        private final IndexInput centroidsInput;
        private final int numCentroids;
        private final int dimension;
        private final float[] scratch;
        private final KMeansResult clusters;
        private final CentroidSlices centroidSlices;
        private int currOrd = -1;

        OffHeapCentroidSupplier(
            IndexInput centroidsInput,
            int numCentroids,
            FieldInfo info,
            KMeansResult clusters,
            CentroidSlices centroidSlices
        ) {
            this.centroidsInput = centroidsInput;
            this.numCentroids = numCentroids;
            this.dimension = info.getVectorDimension();
            this.scratch = new float[dimension];
            this.clusters = clusters;
            this.centroidSlices = centroidSlices;
        }

        @Override
        public int size() {
            return numCentroids;
        }

        @Override
        public float[] centroid(int centroidOrdinal) throws IOException {
            if (centroidOrdinal == currOrd) {
                return scratch;
            }
            centroidsInput.seek((long) centroidOrdinal * dimension * Float.BYTES);
            centroidsInput.readFloats(scratch, 0, dimension);
            this.currOrd = centroidOrdinal;
            return scratch;
        }

        @Override
        public KMeansResult secondLevelClusters() {
            return clusters;
        }

        @Override
        public CentroidSlices slices() throws IOException {
            return centroidSlices;
        }

        @Override
        public KMeansFloatVectorValues asKmeansFloatVectorValues() throws IOException {
            return KMeansFloatVectorValues.build(centroidsInput, null, numCentroids, dimension);
        }
    }

    static class QuantizedCentroids implements QuantizedVectorValues {
        private final CentroidSupplier supplier;
        private final OptimizedScalarQuantizer quantizer;
        private final byte[] quantizedVector;
        private final int[] quantizedVectorScratch;
        private final float[] floatVectorScratch;
        private OptimizedScalarQuantizer.QuantizationResult corrections;
        private final float[] centroid;
        private int currOrd = -1;
        private IntToIntFunction ordTransformer = i -> i;
        int size;

        QuantizedCentroids(CentroidSupplier supplier, int dimension, OptimizedScalarQuantizer quantizer, float[] centroid) {
            this.supplier = supplier;
            this.quantizer = quantizer;
            this.quantizedVector = new byte[dimension];
            this.floatVectorScratch = new float[dimension];
            this.quantizedVectorScratch = new int[dimension];
            this.centroid = centroid;
            size = supplier.size();
        }

        @Override
        public int count() {
            return size;
        }

        void reset(IntToIntFunction ordTransformer, int size) {
            this.ordTransformer = ordTransformer;
            this.currOrd = -1;
            this.size = size;
            this.corrections = null;
        }

        @Override
        public byte[] next() throws IOException {
            if (currOrd >= count() - 1) {
                throw new IllegalStateException("No more vectors to read, current ord: " + currOrd + ", count: " + count());
            }
            currOrd++;
            float[] vector = supplier.centroid(ordTransformer.apply(currOrd));
            corrections = quantizer.scalarQuantize(vector, floatVectorScratch, quantizedVectorScratch, (byte) 7, centroid);
            for (int i = 0; i < quantizedVectorScratch.length; i++) {
                quantizedVector[i] = (byte) quantizedVectorScratch[i];
            }
            return quantizedVector;
        }

        @Override
        public OptimizedScalarQuantizer.QuantizationResult getCorrections() {
            return corrections;
        }
    }

    static class OnHeapQuantizedVectors implements QuantizedVectorValues {
        private final FloatVectorValues vectorValues;
        private final OptimizedScalarQuantizer quantizer;
        private final byte[] quantizedVector;
        private final int[] quantizedVectorScratch;
        private final float[] floatVectorScratch;
        private final ESNextDiskBBQVectorsFormat.QuantEncoding encoding;
        private OptimizedScalarQuantizer.QuantizationResult corrections;
        private final VectorSimilarityFunction similarityFunction;
        private float[] currentCentroid, currentParentCentroid;
        private IntToIntFunction ordTransformer = null;
        private int currOrd = -1;
        private int count;

        OnHeapQuantizedVectors(
            FloatVectorValues vectorValues,
            VectorSimilarityFunction similarityFunction,
            ESNextDiskBBQVectorsFormat.QuantEncoding encoding,
            int dimension,
            OptimizedScalarQuantizer quantizer
        ) {
            this.vectorValues = vectorValues;
            this.similarityFunction = similarityFunction;
            this.encoding = encoding;
            this.quantizer = quantizer;
            this.quantizedVector = new byte[encoding.getDocPackedLength(dimension)];
            this.floatVectorScratch = new float[dimension];
            this.quantizedVectorScratch = new int[encoding.discretizedDimensions(dimension)];
            this.corrections = null;
            this.currentParentCentroid = null;
        }

        private void reset(float[] centroid, float[] currentParentCentroid, int count, IntToIntFunction ordTransformer) {
            this.currentCentroid = centroid;
            this.ordTransformer = ordTransformer;
            this.currOrd = -1;
            this.count = count;
            this.currentParentCentroid = currentParentCentroid;
        }

        @Override
        public int count() {
            return count;
        }

        @Override
        public byte[] next() throws IOException {
            if (currOrd >= count() - 1) {
                throw new IllegalStateException("No more vectors to read, current ord: " + currOrd + ", count: " + count());
            }
            currOrd++;
            int ord = ordTransformer.apply(currOrd);
            float[] vector = vectorValues.vectorValue(ord);
            corrections = quantizer.scalarQuantize(vector, floatVectorScratch, quantizedVectorScratch, encoding.bits(), currentCentroid);
            // note, with a parent centroid, our correction needs to take it into account
            if (currentParentCentroid != null) {
                float additionalCorrection = similarityFunction == VectorSimilarityFunction.EUCLIDEAN
                    ? ESVectorUtil.squareDistance(vector, currentParentCentroid)
                    : ESVectorUtil.dotProduct(floatVectorScratch, currentParentCentroid);
                corrections = new OptimizedScalarQuantizer.QuantizationResult(
                    corrections.lowerInterval(),
                    corrections.upperInterval(),
                    additionalCorrection,
                    corrections.quantizedComponentSum()
                );
            }
            encoding.pack(quantizedVectorScratch, quantizedVector);
            return quantizedVector;
        }

        @Override
        public OptimizedScalarQuantizer.QuantizationResult getCorrections() {
            if (currOrd == -1) {
                throw new IllegalStateException("No vector read yet, call next first");
            }
            return corrections;
        }
    }

    static class OffHeapQuantizedVectors implements QuantizedVectorValues {
        private final IndexInput quantizedVectorsInput;
        private final byte[] binaryScratch;
        private final float[] corrections = new float[3];

        private final int vectorByteSize;
        private int bitSum;
        private int currOrd = -1;
        private int count;
        private IntToBooleanFunction isOverspill = null;
        private IntToIntFunction ordTransformer = null;

        OffHeapQuantizedVectors(IndexInput quantizedVectorsInput, ESNextDiskBBQVectorsFormat.QuantEncoding encoding, int dimension) {
            this.quantizedVectorsInput = quantizedVectorsInput;
            this.binaryScratch = new byte[encoding.getDocPackedLength(dimension)];
            this.vectorByteSize = (binaryScratch.length + 3 * Float.BYTES + Integer.BYTES);
        }

        private void reset(int count, IntToBooleanFunction isOverspill, IntToIntFunction ordTransformer) {
            this.count = count;
            this.isOverspill = isOverspill;
            this.ordTransformer = ordTransformer;
            this.currOrd = -1;
        }

        @Override
        public int count() {
            return count;
        }

        @Override
        public byte[] next() throws IOException {
            if (currOrd >= count - 1) {
                throw new IllegalStateException("No more vectors to read, current ord: " + currOrd + ", count: " + count);
            }
            currOrd++;
            int ord = ordTransformer.apply(currOrd);
            boolean isOverspill = this.isOverspill.apply(currOrd);
            return getVector(ord, isOverspill);
        }

        @Override
        public OptimizedScalarQuantizer.QuantizationResult getCorrections() {
            if (currOrd == -1) {
                throw new IllegalStateException("No vector read yet, call readQuantizedVector first");
            }
            return new OptimizedScalarQuantizer.QuantizationResult(corrections[0], corrections[1], corrections[2], bitSum);
        }

        byte[] getVector(int ord, boolean isOverspill) throws IOException {
            readQuantizedVector(ord, isOverspill);
            return binaryScratch;
        }

        public void readQuantizedVector(int ord, boolean isOverspill) throws IOException {
            long offset = (long) ord * (vectorByteSize * 2L) + (isOverspill ? vectorByteSize : 0);
            quantizedVectorsInput.seek(offset);
            quantizedVectorsInput.readBytes(binaryScratch, 0, binaryScratch.length);
            quantizedVectorsInput.readFloats(corrections, 0, 3);
            bitSum = quantizedVectorsInput.readInt();
        }
    }
}
