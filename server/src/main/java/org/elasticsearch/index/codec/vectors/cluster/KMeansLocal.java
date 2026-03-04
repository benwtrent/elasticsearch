/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;

/**
 * k-means implementation specific to the needs of the {@link HierarchicalKMeans} algorithm that deals specifically
 * with finalizing nearby pre-established clusters and generate
 * <a href="https://research.google/blog/soar-new-algorithms-for-even-faster-vector-search-with-scann/">SOAR</a> assignments
 */
abstract class KMeansLocal {

    private static final Logger logger = LogManager.getLogger(KMeansLocal.class);
    private static final String INSTRUMENTATION_PROPERTY = "es.kmeans.instrumentation.enabled";
    private final int sampleSize;
    private final int maxIterations;
    private static final int INITIAL_PREFIX_DIVISOR = 4;
    private static final int MIN_PREFIX_DIVISOR = 2;
    private static final int MAX_PREFIX_DIVISOR = 16;
    private static final float MIN_NOT_PRUNED_RATIO = 0.03f;
    private static final float MAX_NOT_PRUNED_RATIO = 0.05f;
    private static final float PREFIX_TUNE_ADJUSTMENT = 0.20f;

    KMeansLocal(int sampleSize, int maxIterations) {
        this.sampleSize = sampleSize;
        this.maxIterations = maxIterations;
    }

    /** Number of workers to use for parallelism **/
    protected abstract int numWorkers();

    /** assign to each vector the closest centroid **/
    protected abstract ClusteringFloatVectorValues.AssignmentStats stepLloyd(
        ClusteringFloatVectorValues vectors,
        IntToIntFunction translateOrd,
        float[][] centroids,
        FixedBitSet[] centroidChangedSlices,
        int[] assignments,
        NeighborHood[] neighborHoods,
        int prefixDivisor,
        boolean enableThresholdPruning
    ) throws IOException;

    /** assign to each vector the soar assignment **/
    protected abstract void assignSpilled(
        ClusteringFloatVectorValues vectors,
        KMeansIntermediate kmeansIntermediate,
        NeighborHood[] neighborhoods,
        float soarLambda
    ) throws IOException;

    /** compute the neighborhoods for the given centroids and clustersPerNeighborhood */
    protected abstract NeighborHood[] computeNeighborhoods(float[][] centroids, int clustersPerNeighborhood) throws IOException;

    /**
     * uses a Reservoir Sampling approach to picking the initial centroids which are subsequently expected
     * to be used by a clustering algorithm
     *
     * @param vectors used to pick an initial set of random centroids
     * @param centroidCount the total number of centroids to pick
     * @return randomly selected centroids that are the min of centroidCount and sampleSize
     * @throws IOException is thrown if vectors is inaccessible
     */
    static float[][] pickInitialCentroids(ClusteringFloatVectorValues vectors, int centroidCount) throws IOException {
        Random random = new Random(42L);
        int centroidsSize = Math.min(vectors.size(), centroidCount);
        float[][] centroids = new float[centroidsSize][vectors.dimension()];
        for (int i = 0; i < vectors.size(); i++) {
            float[] vector;
            if (i < centroidCount) {
                vector = vectors.vectorValue(i);
                System.arraycopy(vector, 0, centroids[i], 0, vector.length);
            } else if (random.nextDouble() < centroidCount * (1.0 / i)) {
                int c = random.nextInt(centroidCount);
                vector = vectors.vectorValue(i);
                System.arraycopy(vector, 0, centroids[c], 0, vector.length);
            }
        }
        return centroids;
    }

    /** Assign vectors from {@code startOrd} to {@code endOrd} to the closest centroid. */
    protected static ClusteringFloatVectorValues.AssignmentStats stepLloydSlice(
        ClusteringFloatVectorValues vectors,
        IntToIntFunction ordTranslator,
        float[][] centroids,
        FixedBitSet centroidChanged,
        int[] assignments,
        NeighborHood[] neighborhoods,
        int prefixDivisor,
        boolean enableThresholdPruning,
        int startOrd,
        int endOrd
    ) throws IOException {
        centroidChanged.clear();
        if (neighborhoods != null) {
            return vectors.bestCentroidsFromNeighbours(
                startOrd,
                endOrd,
                centroids,
                ordTranslator,
                centroidChanged,
                neighborhoods,
                assignments,
                prefixDivisor,
                enableThresholdPruning
            );
        } else {
            return vectors.bestCentroids(
                startOrd,
                endOrd,
                centroids,
                ordTranslator,
                centroidChanged,
                assignments,
                prefixDivisor,
                enableThresholdPruning
            );
        }
    }

    /** Assign vectors from {@code startOrd} to {@code endOrd} to the SOAR centroid. */
    protected static void assignSpilledSlice(
        ClusteringFloatVectorValues vectors,
        KMeansIntermediate kmeansIntermediate,
        NeighborHood[] neighborhoods,
        float soarLambda,
        int startOrd,
        int endOrd
    ) throws IOException {
        int[] assignments = kmeansIntermediate.assignments();
        assert assignments != null;
        assert assignments.length == vectors.size();
        int[] spilledAssignments = kmeansIntermediate.soarAssignments();
        assert spilledAssignments != null;
        assert spilledAssignments.length == vectors.size();
        float[][] centroids = kmeansIntermediate.centroids();
        vectors.assignSpilled(startOrd, endOrd, centroids, neighborhoods, soarLambda, assignments, spilledAssignments);
    }

    /**
     * cluster using a lloyd k-means algorithm that is not neighbor aware
     *
     * @param vectors the vectors to cluster
     * @param kMeansIntermediate the output object to populate which minimally includes centroids,
     *                     but may include assignments and soar assignments as well; care should be taken in
     *                     passing in a valid output object with a centroids array that is the size of centroids expected
     * @throws IOException is thrown if vectors is inaccessible
     */
    final void cluster(ClusteringFloatVectorValues vectors, KMeansIntermediate kMeansIntermediate) throws IOException {
        doCluster(vectors, kMeansIntermediate, -1, -1);
    }

    /**
     * cluster using a lloyd kmeans algorithm that also considers prior clustered neighborhoods when adjusting centroids
     * this also is used to generate the neighborhood aware additional (SOAR) assignments
     *
     * @param vectors the vectors to cluster
     * @param kMeansIntermediate the output object to populate which minimally includes centroids,
     *                     the prior assignments of the given vectors; care should be taken in
     *                     passing in a valid output object with a centroids array that is the size of centroids expected
     *                     and assignments that are the same size as the vectors.  The SOAR assignments are overwritten by this operation.
     * @param clustersPerNeighborhood number of nearby neighboring centroids to be used to update the centroid positions.
     * @param soarLambda   lambda used for SOAR assignments
     *
     * @throws IOException is thrown if vectors is inaccessible or if the clustersPerNeighborhood is less than 2
     */
    final void cluster(
        ClusteringFloatVectorValues vectors,
        KMeansIntermediate kMeansIntermediate,
        int clustersPerNeighborhood,
        float soarLambda
    ) throws IOException {
        if (clustersPerNeighborhood < 2) {
            throw new IllegalArgumentException("clustersPerNeighborhood must be at least 2, got [" + clustersPerNeighborhood + "]");
        }
        doCluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);
    }

    private void doCluster(
        ClusteringFloatVectorValues vectors,
        KMeansIntermediate kMeansIntermediate,
        int clustersPerNeighborhood,
        float soarLambda
    ) throws IOException {
        float[][] centroids = kMeansIntermediate.centroids();
        boolean neighborAware = clustersPerNeighborhood != -1 && centroids.length > 1;
        NeighborHood[] neighborhoods = null;
        // if there are very few centroids, don't bother with neighborhoods or neighbor aware clustering
        if (neighborAware && centroids.length > clustersPerNeighborhood) {
            neighborhoods = computeNeighborhoods(centroids, clustersPerNeighborhood);
        }
        cluster(vectors, kMeansIntermediate, neighborhoods);
        if (neighborAware && soarLambda >= 0) {
            assert kMeansIntermediate.soarAssignments().length == 0;
            kMeansIntermediate.setSoarAssignments(new int[vectors.size()]);
            assignSpilled(vectors, kMeansIntermediate, neighborhoods, soarLambda);
        }
    }

    private void cluster(ClusteringFloatVectorValues vectors, KMeansIntermediate kMeansIntermediate, NeighborHood[] neighborhoods)
        throws IOException {
        float[][] centroids = kMeansIntermediate.centroids();
        int k = centroids.length;
        int n = vectors.size();
        int[] assignments = kMeansIntermediate.assignments();

        if (k == 1) {
            Arrays.fill(assignments, 0);
            return;
        }
        IntToIntFunction ordTranslator = i -> i;
        ClusteringFloatVectorValues sampledVectors = vectors;
        if (sampleSize < n) {
            sampledVectors = ClusteringFloatVectorValuesSlice.createRandomSlice(vectors, sampleSize, 42L);
            ordTranslator = sampledVectors::ordToDoc;
        }

        assert assignments.length == n;
        FixedBitSet[] centroidChangedSlices = new FixedBitSet[numWorkers()];
        for (int i = 0; i < numWorkers(); i++) {
            centroidChangedSlices[i] = new FixedBitSet(centroids.length);
        }
        int[] centroidCounts = new int[centroids.length];
        int prefixDivisor = INITIAL_PREFIX_DIVISOR;
        long totalCandidates = 0;
        long totalFullScores = 0;
        int iterationsRun = 0;
        int minScoredDimensions = Integer.MAX_VALUE;
        long[] scoredDimensionsHistogram = null;
        if (Boolean.getBoolean(INSTRUMENTATION_PROPERTY)) {
            scoredDimensionsHistogram = new long[vectors.dimension() + 1];
        }
        for (int i = 0; i < maxIterations; i++) {
            boolean enableThresholdPruning = i > 0;
            // This is potentially sampled, so we need to translate ordinals
            ClusteringFloatVectorValues.AssignmentStats stats = stepLloyd(
                sampledVectors,
                ordTranslator,
                centroids,
                centroidChangedSlices,
                assignments,
                neighborhoods,
                prefixDivisor,
                enableThresholdPruning
            );
            totalCandidates += stats.candidateCount;
            totalFullScores += stats.refinedCount;
            iterationsRun++;
            minScoredDimensions = Math.min(minScoredDimensions, stats.minScoredDimensions);
            if (scoredDimensionsHistogram != null && stats.scoredDimensionsHistogram != null) {
                accumulateHistogram(scoredDimensionsHistogram, stats.scoredDimensionsHistogram);
            }
            int changedCount = stats.changedCount;
            if (changedCount > 0) {
                sampledVectors.updateCentroids(centroids, ordTranslator, centroidChangedSlices, centroidCounts, assignments);
                if (stats.candidateCount > 0) {
                    float notPrunedRatio = (float) stats.refinedCount / stats.candidateCount;
                    if (notPrunedRatio > MAX_NOT_PRUNED_RATIO) {
                        int decrease = Math.max(1, Math.round(prefixDivisor * PREFIX_TUNE_ADJUSTMENT));
                        prefixDivisor = Math.max(MIN_PREFIX_DIVISOR, prefixDivisor - decrease);
                    } else if (notPrunedRatio < MIN_NOT_PRUNED_RATIO) {
                        int increase = Math.max(1, Math.round(prefixDivisor * PREFIX_TUNE_ADJUSTMENT));
                        prefixDivisor = Math.min(MAX_PREFIX_DIVISOR, prefixDivisor + increase);
                    }
                }
            } else {
                break;
            }
        }
        // If we were sampled, do a once over the full set of vectors to finalize the centroids
        if (sampleSize < n || maxIterations == 0) {
            // No ordinal translation needed here, we are using the full set of vectors
            ClusteringFloatVectorValues.AssignmentStats stats = stepLloyd(
                vectors,
                i -> i,
                centroids,
                centroidChangedSlices,
                assignments,
                neighborhoods,
                prefixDivisor,
                true
            );
            totalCandidates += stats.candidateCount;
            totalFullScores += stats.refinedCount;
            iterationsRun++;
            minScoredDimensions = Math.min(minScoredDimensions, stats.minScoredDimensions);
            if (scoredDimensionsHistogram != null && stats.scoredDimensionsHistogram != null) {
                accumulateHistogram(scoredDimensionsHistogram, stats.scoredDimensionsHistogram);
            }
            int changedCount = stats.changedCount;
            if (changedCount > 0) {
                sampledVectors.updateCentroids(centroids, ordTranslator, centroidChangedSlices, centroidCounts, assignments);
            }
        }
        if (Boolean.getBoolean(INSTRUMENTATION_PROPERTY)) {
            long pruned = Math.max(0, totalCandidates - totalFullScores);
            double pruneRatio = totalCandidates == 0 ? 0d : (double) pruned / totalCandidates;
            long totalScoredCandidates = scoredDimensionsHistogram == null ? 0 : Arrays.stream(scoredDimensionsHistogram).sum();
            int medianScoredDimensions = scoredDimensionsHistogram == null
                ? 0
                : histogramMedian(scoredDimensionsHistogram, totalScoredCandidates);
            int minDims = minScoredDimensions == Integer.MAX_VALUE ? 0 : minScoredDimensions;
            logger.info(
                "kmeans_pruning_stats vectors={} centroids={} iterations={} candidates={} full_scores={} pruned={} prune_ratio={} min_scored_dims={} median_scored_dims={} final_prefix_divisor={}",
                vectors.size(),
                centroids.length,
                iterationsRun,
                totalCandidates,
                totalFullScores,
                pruned,
                String.format(Locale.ROOT, "%.4f", pruneRatio),
                minDims,
                medianScoredDimensions,
                prefixDivisor
            );
        }
    }

    private static void accumulateHistogram(long[] target, long[] source) {
        for (int i = 0; i < target.length && i < source.length; i++) {
            target[i] += source[i];
        }
    }

    private static int histogramMedian(long[] histogram, long totalCount) {
        if (totalCount <= 0) {
            return 0;
        }
        long half = (totalCount + 1) / 2;
        long cumulative = 0;
        for (int dims = 0; dims < histogram.length; dims++) {
            cumulative += histogram[dims];
            if (cumulative >= half) {
                return dims;
            }
        }
        return histogram.length - 1;
    }

    /**
     * helper that calls {@link KMeansLocal#cluster(ClusteringFloatVectorValues, KMeansIntermediate)} given a set of initialized centroids,
     * this call is not neighbor aware
     *
     * @param vectors the vectors to cluster
     * @param centroids the initialized centroids to be shifted using k-means
     * @param sampleSize the subset of vectors to use when shifting centroids
     * @param maxIterations the max iterations to shift centroids
     */
    public static void cluster(ClusteringFloatVectorValues vectors, float[][] centroids, int sampleSize, int maxIterations)
        throws IOException {
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, new int[vectors.size()], vectors::ordToDoc);
        KMeansLocal kMeans = new KMeansLocalSerial(sampleSize, maxIterations);
        kMeans.cluster(vectors, kMeansIntermediate);
    }
}
