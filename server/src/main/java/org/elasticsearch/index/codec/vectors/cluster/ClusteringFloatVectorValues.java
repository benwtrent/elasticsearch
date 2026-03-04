/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans.NO_SOAR_ASSIGNMENT;

/**
 * A {@link FloatVectorValues} that adds best-centroid computation.
 */
public abstract sealed class ClusteringFloatVectorValues extends FloatVectorValues permits KMeansFloatVectorValues,
    ClusteringFloatVectorValuesSlice {

    // the minimum distance that is considered to be "far enough" to a centroid in order to compute the soar distance.
    // For vectors that are closer than this distance to the centroid don't get spilled because they are well represented
    // by the centroid itself. In many cases, it indicates a degenerated distribution, e.g the cluster is composed of the
    // many equal vectors.
    private static final float SOAR_MIN_DISTANCE = 1e-16f;
    /**
     * Minimum number of dimensions used for the prefix score.
     */
    private static final int PREFIX_MIN_DIMENSIONS = 16;
    /**
     * Prefix split is only beneficial when there are enough candidate centroids to prune.
     */
    private static final int PREFIX_SHORTLIST_MIN_CANDIDATES = 32;
    private static final String PREFIX_REFINEMENT_CAP_RATIO_PROPERTY = "es.kmeans.prefix_refinement_cap_ratio";
    private static final float PREFIX_REFINEMENT_CAP_RATIO = loadPrefixRefinementCapRatio();
    private static final int PREFIX_REFINEMENT_CAP_MIN = 16;
    /**
     * PDX-style pruning granularity: score candidates in 64-dim chunks.
     */
    private static final int PREFIX_PRUNING_BATCH_DIMS = 64;

    static final class AssignmentStats {
        final int changedCount;
        final long candidateCount;
        final long refinedCount;
        final int minScoredDimensions;
        final long[] scoredDimensionsHistogram;

        AssignmentStats(
            int changedCount,
            long candidateCount,
            long refinedCount,
            int minScoredDimensions,
            long[] scoredDimensionsHistogram
        ) {
            this.changedCount = changedCount;
            this.candidateCount = candidateCount;
            this.refinedCount = refinedCount;
            this.minScoredDimensions = minScoredDimensions;
            this.scoredDimensionsHistogram = scoredDimensionsHistogram;
        }
    }

    @Override
    public abstract ClusteringFloatVectorValues copy() throws IOException;

    /**
     * Find the closest centroid for a batch of contiguous vectors, considering all centroids.
     *
     * @param startOrd        the first vector ordinal (inclusive) to process
     * @param endOrd          the last vector ordinal (exclusive) to process
     * @param centroids       the centroid vectors to compare against
     * @param ordTranslator  translate the vector ord to the position of the vector on the result array
     * @param centroidChanged a bitset tracking which centroids had assignments change;
     *                        bits are set for both the old and new centroid when a vector is reassigned
     * @param results         input/output array indexed by document ordinal; on entry holds the
     *                        current centroid assignments (or {@code -1} for unassigned),
     *                        on exit holds the updated assignments
     * @return {@code true} if any assignment changed, {@code false} if all assignments remained the same
     */
    final AssignmentStats bestCentroids(
        int startOrd,
        int endOrd,
        float[][] centroids,
        IntToIntFunction ordTranslator,
        FixedBitSet centroidChanged,
        int[] results,
        int prefixDivisor,
        boolean enableThresholdPruning
    ) throws IOException {
        final boolean collectDimensionStats = Boolean.getBoolean("es.kmeans.instrumentation.enabled");
        final float[] distances = new float[4];
        final float[] candidateScoresScratch = new float[centroids.length];
        final int[] candidateScratch = new int[centroids.length];
        final int[] candidateDimsScratch = new int[centroids.length];
        final long[] scoredDimensionsHistogram = collectDimensionStats ? new long[dimension() + 1] : null;
        int minScoredDimensions = Integer.MAX_VALUE;
        int changedCount = 0;
        long candidateCount = 0;
        long refinedCount = 0;
        final long[] pruningStats = new long[2];
        final int[] minDimensionsStats = new int[] { Integer.MAX_VALUE };
        for (int i = startOrd; i < endOrd; i++) {
            float[] vector = vectorValue(i);
            final int translatedOrd = ordTranslator.apply(i);
            final int assignment = results[translatedOrd];
            pruningStats[0] = 0;
            pruningStats[1] = 0;
            minDimensionsStats[0] = Integer.MAX_VALUE;
            final int bestCentroid = computeBestCentroid(
                vector,
                centroids,
                assignment,
                distances,
                candidateScoresScratch,
                candidateScratch,
                candidateDimsScratch,
                prefixDivisor,
                enableThresholdPruning,
                pruningStats,
                scoredDimensionsHistogram,
                minDimensionsStats
            );
            if (bestCentroid != assignment) {
                if (assignment != -1) {
                    centroidChanged.set(assignment);
                }
                centroidChanged.set(bestCentroid);
                changedCount++;
                results[translatedOrd] = bestCentroid;
            }
            candidateCount += pruningStats[0];
            refinedCount += pruningStats[1];
            minScoredDimensions = Math.min(minScoredDimensions, minDimensionsStats[0]);
        }
        return new AssignmentStats(changedCount, candidateCount, refinedCount, minScoredDimensions, scoredDimensionsHistogram);
    }

    /**
     * Find the closest centroid for a batch of contiguous vectors, restricting the search to each
     * vector's current centroid and its pre-computed neighborhood of nearby centroids.
     *
     * @param startOrd            the first vector ordinal (inclusive) to process
     * @param endOrd              the last vector ordinal (exclusive) to process
     * @param centroids       the centroid vectors to compare against
     * @param ordTranslator  translate the vector ord to the position of the vector on the result array
     * @param centroidChanged a bitset tracking which centroids had assignments change;
     *                        bits are set for both the old and new centroid when a vector is reassigned
     * @param neighborhoods   per-centroid neighborhoods; {@code neighborhoods[c]} contains the
     *                        neighboring centroid indices and maximum intra-cluster distance for centroid {@code c}
     * @param results         input/output array indexed by document ordinal; on entry holds the
     *                        current centroid assignments, on exit holds the updated assignments
     * @return {@code true} if any assignment changed, {@code false} if all assignments remained the same
     */
    final AssignmentStats bestCentroidsFromNeighbours(
        int startOrd,
        int endOrd,
        float[][] centroids,
        IntToIntFunction ordTranslator,
        FixedBitSet centroidChanged,
        NeighborHood[] neighborhoods,
        int[] results,
        int prefixDivisor,
        boolean enableThresholdPruning
    ) throws IOException {
        final boolean collectDimensionStats = Boolean.getBoolean("es.kmeans.instrumentation.enabled");
        final float[] distances = new float[4];
        final float[] candidateScoresScratch = new float[centroids.length];
        final int[] candidateScratch = new int[centroids.length];
        final int[] candidateDimsScratch = new int[centroids.length];
        final long[] scoredDimensionsHistogram = collectDimensionStats ? new long[dimension() + 1] : null;
        int minScoredDimensions = Integer.MAX_VALUE;
        int changedCount = 0;
        long candidateCount = 0;
        long refinedCount = 0;
        final long[] pruningStats = new long[2];
        final int[] minDimensionsStats = new int[] { Integer.MAX_VALUE };
        for (int i = startOrd; i < endOrd; i++) {
            float[] vector = vectorValue(i);
            final int translatedOrd = ordTranslator.apply(i);
            final int assignment = results[translatedOrd];
            assert assignment != -1 : "vector is not assigned to any cluster: ord=" + translatedOrd;
            pruningStats[0] = 0;
            pruningStats[1] = 0;
            minDimensionsStats[0] = Integer.MAX_VALUE;
            final int bestCentroid = computeBestCentroidFromNeighbours(
                vector,
                centroids,
                assignment,
                neighborhoods[assignment],
                distances,
                candidateScoresScratch,
                candidateScratch,
                candidateDimsScratch,
                prefixDivisor,
                enableThresholdPruning,
                pruningStats,
                scoredDimensionsHistogram,
                minDimensionsStats
            );
            if (bestCentroid != assignment) {
                centroidChanged.set(assignment);
                centroidChanged.set(bestCentroid);
                changedCount++;
                results[translatedOrd] = bestCentroid;
            }
            candidateCount += pruningStats[0];
            refinedCount += pruningStats[1];
            minScoredDimensions = Math.min(minScoredDimensions, minDimensionsStats[0]);
        }
        return new AssignmentStats(changedCount, candidateCount, refinedCount, minScoredDimensions, scoredDimensionsHistogram);
    }

    /**
     * Recompute centroid positions as the mean of their assigned vectors. Only centroids whose
     * assignments changed (as indicated by the union of {@code centroidChangedSlices}) are
     * recomputed; unchanged centroids are left as-is.
     *
     * @param centroids             the centroid vectors; updated in place with the new mean positions
     * @param ordTranslator         translate the vector ord to thr position of the vector on the assignments array
     * @param centroidChangedSlices per-thread bitsets indicating which centroids had assignment changes;
     *                              these are OR'd together to determine the full set of changed centroids
     * @param centroidCounts        scratch array of length {@code centroids.length}; on exit holds the
     *                              number of vectors assigned to each changed centroid
     * @param assignments           the current centroid assignment for each document ordinal
     */
    final void updateCentroids(
        float[][] centroids,
        IntToIntFunction ordTranslator,
        FixedBitSet[] centroidChangedSlices,
        int[] centroidCounts,
        int[] assignments
    ) throws IOException {
        Arrays.fill(centroidCounts, 0);
        FixedBitSet centroidChanged = centroidChangedSlices[0];
        for (int j = 1; j < centroidChangedSlices.length; j++) {
            centroidChanged.or(centroidChangedSlices[j]);
        }
        int dim = dimension();
        for (int idx = 0; idx < size(); idx++) {
            final int assignment = assignments[ordTranslator.apply(idx)];
            if (centroidChanged.get(assignment)) {
                float[] centroid = centroids[assignment];
                float[] vector = vectorValue(idx);
                if (centroidCounts[assignment]++ == 0) {
                    System.arraycopy(vector, 0, centroid, 0, dim);
                } else {
                    for (int d = 0; d < dim; d++) {
                        centroid[d] += vector[d];
                    }
                }
            }
        }

        for (int clusterIdx = 0; clusterIdx < centroids.length; clusterIdx++) {
            if (centroidChanged.get(clusterIdx)) {
                float count = (float) centroidCounts[clusterIdx];
                if (count > 0) {
                    float[] centroid = centroids[clusterIdx];
                    for (int d = 0; d < dim; d++) {
                        centroid[d] /= count;
                    }
                }
            }
        }
    }

    /**
     * Assign a secondary ("spilled") centroid to each vector in the given ordinal range using the
     * <a href="https://arxiv.org/abs/2404.18984">SOAR</a> adjusted distance. The SOAR distance for
     * a vector {@code x} with primary centroid {@code c_1} to a candidate centroid {@code c} is:
     * <pre>
     *   soar(x, c) = ||x - c||^2 + lambda * ((x - c_1)^T (x - c))^2 / ||x - c_1||^2
     * </pre>
     * Each vector is assigned to the candidate centroid with the smallest SOAR distance. Vectors
     * that are extremely close to their primary centroid (within {@link #SOAR_MIN_DISTANCE}) receive
     * {@link HierarchicalKMeans#NO_SOAR_ASSIGNMENT} since they are already well represented.
     * <p>
     * When {@code neighborhoods} is non-null, only the neighboring centroids of the vector's
     * primary assignment are considered as candidates; otherwise all centroids (excluding the
     * primary) are evaluated.
     *
     * @param startOrd            the first vector ordinal (inclusive) to process
     * @param endOrd              the last vector ordinal (exclusive) to process
     * @param centroids           the centroid vectors
     * @param neighborhoods       per-centroid neighborhoods used to restrict candidate centroids,
     *                            or {@code null} to consider all centroids
     * @param soarLambda          the lambda weighting factor for the SOAR residual penalty term
     * @param assignments         the primary centroid assignment for each vector ordinal
     * @param spilledAssignments  output array; {@code spilledAssignments[i]} receives the secondary
     *                            centroid index for vector {@code i}, or
     *                            {@link HierarchicalKMeans#NO_SOAR_ASSIGNMENT} if the vector is too
     *                            close to its primary centroid
     */
    final void assignSpilled(
        int startOrd,
        int endOrd,
        float[][] centroids,
        NeighborHood[] neighborhoods,
        float soarLambda,
        int[] assignments,
        int[] spilledAssignments
    ) throws IOException {
        // SOAR uses an adjusted distance for assigning spilled documents which is
        // given by:
        //
        // soar(x, c) = ||x - c||^2 + lambda * ((x - c_1)^t (x - c))^2 / ||x - c_1||^2
        //
        // Here, x is the document, c is the nearest centroid, and c_1 is the first
        // centroid the document was assigned to. The document is assigned to the
        // cluster with the smallest soar(x, c).
        float[] diffs = new float[dimension()];
        final float[] distances = new float[4];
        for (int i = startOrd; i < endOrd; i++) {
            float[] vector = vectorValue(i);
            final int currAssignment = assignments[i];
            final int centroidCount;
            final IntToIntFunction centroidOrds;
            if (neighborhoods != null) {
                assert neighborhoods[currAssignment] != null;
                NeighborHood neighborhood = neighborhoods[currAssignment];
                centroidCount = neighborhood.neighbors().length;
                centroidOrds = c -> neighborhood.neighbors()[c];
            } else {
                centroidCount = centroids.length - 1;
                centroidOrds = c -> c < currAssignment ? c : c + 1; // skip the current centroid
            }
            spilledAssignments[i] = computeSoarAssignment(
                vector,
                centroids,
                currAssignment,
                centroidCount,
                centroidOrds,
                soarLambda,
                diffs,
                distances
            );
        }
    }

    /**
     * Find the closest centroid for a materialized vector, considering all centroids.
     *
     * @param vector    the vector to assign
     * @param centroids the centroid vectors to compare against
     * @param distances scratch array of length 4 used for bulk distance results
     * @return the index into {@code centroids} of the nearest centroid
     */
    private static int computeBestCentroid(
        float[] vector,
        float[][] centroids,
        int assignedCentroidIdx,
        float[] distances,
        float[] candidateScoresScratch,
        int[] candidateScratch,
        int[] candidateDimsScratch,
        int prefixDivisor,
        boolean enableThresholdPruning,
        long[] pruningStats,
        long[] scoredDimensionsHistogram,
        int[] minDimensionsStats
    ) {
        final int prefixLength = computePrefixLength(vector.length, prefixDivisor);
        final int suffixOffset = prefixLength;
        final int suffixLength = vector.length - prefixLength;
        if (usePrefixThresholdPruning(centroids.length, suffixLength, enableThresholdPruning) == false) {
            pruningStats[0] += centroids.length;
            pruningStats[1] += centroids.length;
            if (scoredDimensionsHistogram != null) {
                scoredDimensionsHistogram[vector.length] += centroids.length;
                updateMinDimensions(minDimensionsStats, vector.length);
            }
            return computeBestCentroidFull(vector, centroids, distances);
        }
        pruningStats[0] += centroids.length;
        int bestCentroidOffset = 0;
        float minDsq = Float.MAX_VALUE;
        if (assignedCentroidIdx >= 0 && assignedCentroidIdx < centroids.length) {
            minDsq = ESVectorUtil.squareDistance(vector, centroids[assignedCentroidIdx]);
            bestCentroidOffset = assignedCentroidIdx;
        }
        int activeCount = centroids.length;
        for (int i = 0; i < centroids.length; i++) {
            candidateScratch[i] = i;
            candidateScoresScratch[i] = 0f;
            candidateDimsScratch[i] = 0;
        }

        for (int prefixOffset = 0; prefixOffset < prefixLength && activeCount > 0; prefixOffset += PREFIX_PRUNING_BATCH_DIMS) {
            final int batchLength = Math.min(PREFIX_PRUNING_BATCH_DIMS, prefixLength - prefixOffset);
            int kept = 0;
            int i = 0;
            int bulkLimit = activeCount - 3;
            for (; i < bulkLimit; i += 4) {
                int c0 = candidateScratch[i];
                int c1 = candidateScratch[i + 1];
                int c2 = candidateScratch[i + 2];
                int c3 = candidateScratch[i + 3];
                ESVectorUtil.squareDistanceBulk(
                    vector,
                    prefixOffset,
                    centroids[c0],
                    prefixOffset,
                    centroids[c1],
                    prefixOffset,
                    centroids[c2],
                    prefixOffset,
                    centroids[c3],
                    prefixOffset,
                    batchLength,
                    distances
                );
                float s0 = candidateScoresScratch[i] + distances[0];
                float s1 = candidateScoresScratch[i + 1] + distances[1];
                float s2 = candidateScoresScratch[i + 2] + distances[2];
                float s3 = candidateScoresScratch[i + 3] + distances[3];
                int d0 = candidateDimsScratch[i] + batchLength;
                int d1 = candidateDimsScratch[i + 1] + batchLength;
                int d2 = candidateDimsScratch[i + 2] + batchLength;
                int d3 = candidateDimsScratch[i + 3] + batchLength;
                if (enableThresholdPruning == false || s0 < minDsq) {
                    candidateScratch[kept++] = c0;
                    candidateScoresScratch[kept - 1] = s0;
                    candidateDimsScratch[kept - 1] = d0;
                } else if (scoredDimensionsHistogram != null) {
                    scoredDimensionsHistogram[d0]++;
                    updateMinDimensions(minDimensionsStats, d0);
                }
                if (enableThresholdPruning == false || s1 < minDsq) {
                    candidateScratch[kept++] = c1;
                    candidateScoresScratch[kept - 1] = s1;
                    candidateDimsScratch[kept - 1] = d1;
                } else if (scoredDimensionsHistogram != null) {
                    scoredDimensionsHistogram[d1]++;
                    updateMinDimensions(minDimensionsStats, d1);
                }
                if (enableThresholdPruning == false || s2 < minDsq) {
                    candidateScratch[kept++] = c2;
                    candidateScoresScratch[kept - 1] = s2;
                    candidateDimsScratch[kept - 1] = d2;
                } else if (scoredDimensionsHistogram != null) {
                    scoredDimensionsHistogram[d2]++;
                    updateMinDimensions(minDimensionsStats, d2);
                }
                if (enableThresholdPruning == false || s3 < minDsq) {
                    candidateScratch[kept++] = c3;
                    candidateScoresScratch[kept - 1] = s3;
                    candidateDimsScratch[kept - 1] = d3;
                } else if (scoredDimensionsHistogram != null) {
                    scoredDimensionsHistogram[d3]++;
                    updateMinDimensions(minDimensionsStats, d3);
                }
            }
            for (; i < activeCount; i++) {
                int centroidOrd = candidateScratch[i];
                float score = candidateScoresScratch[i] + ESVectorUtil.squareDistance(
                    vector,
                    prefixOffset,
                    centroids[centroidOrd],
                    prefixOffset,
                    batchLength
                );
                int dims = candidateDimsScratch[i] + batchLength;
                if (enableThresholdPruning == false || score < minDsq) {
                    candidateScratch[kept++] = centroidOrd;
                    candidateScoresScratch[kept - 1] = score;
                    candidateDimsScratch[kept - 1] = dims;
                } else if (scoredDimensionsHistogram != null) {
                    scoredDimensionsHistogram[dims]++;
                    updateMinDimensions(minDimensionsStats, dims);
                }
            }
            activeCount = kept;
        }
        final int refinementCap = refinementCapSize(centroids.length);
        if (activeCount > refinementCap) {
            final int originalActiveCount = activeCount;
            final int[] originalCandidates = scoredDimensionsHistogram == null
                ? null
                : Arrays.copyOf(candidateScratch, originalActiveCount);
            final int[] originalDims = scoredDimensionsHistogram == null ? null : Arrays.copyOf(candidateDimsScratch, originalActiveCount);
            final NeighborQueue shortlist = new NeighborQueue(refinementCap, true);
            for (int i = 0; i < activeCount; i++) {
                shortlist.insertWithOverflow(candidateScratch[i], candidateScoresScratch[i]);
            }
            activeCount = shortlist.size();
            for (int i = 0; i < activeCount; i++) {
                long candidate = shortlist.popRaw();
                candidateScratch[i] = shortlist.decodeNodeId(candidate);
                candidateScoresScratch[i] = shortlist.decodeScore(candidate);
                if (scoredDimensionsHistogram != null) {
                    candidateDimsScratch[i] = findDimensions(originalCandidates, originalDims, originalActiveCount, candidateScratch[i]);
                }
            }
            if (scoredDimensionsHistogram != null && originalActiveCount > activeCount) {
                int[] selected = Arrays.copyOf(candidateScratch, activeCount);
                Arrays.sort(selected);
                for (int i = 0; i < originalActiveCount; i++) {
                    if (Arrays.binarySearch(selected, originalCandidates[i]) < 0) {
                        scoredDimensionsHistogram[originalDims[i]]++;
                        updateMinDimensions(minDimensionsStats, originalDims[i]);
                    }
                }
            }
        }
        pruningStats[1] += activeCount;
        if (suffixLength == 0) {
            if (scoredDimensionsHistogram != null) {
                recordActiveDimensions(scoredDimensionsHistogram, minDimensionsStats, candidateDimsScratch, 0, activeCount);
            }
            for (int i = 0; i < activeCount; i++) {
                int centroidOrd = candidateScratch[i];
                float dsq = candidateScoresScratch[i];
                if (dsq < minDsq) {
                    minDsq = dsq;
                    bestCentroidOffset = centroidOrd;
                }
            }
            return bestCentroidOffset;
        }
        int i = 0;
        int shortlistLimit = activeCount - 3;
        for (; i < shortlistLimit; i += 4) {
            int c0 = candidateScratch[i];
            int c1 = candidateScratch[i + 1];
            int c2 = candidateScratch[i + 2];
            int c3 = candidateScratch[i + 3];
            ESVectorUtil.squareDistanceBulk(
                vector,
                suffixOffset,
                centroids[c0],
                suffixOffset,
                centroids[c1],
                suffixOffset,
                centroids[c2],
                suffixOffset,
                centroids[c3],
                suffixOffset,
                suffixLength,
                distances
            );
            float dsq0 = candidateScoresScratch[i] + distances[0];
            float dsq1 = candidateScoresScratch[i + 1] + distances[1];
            float dsq2 = candidateScoresScratch[i + 2] + distances[2];
            float dsq3 = candidateScoresScratch[i + 3] + distances[3];
            if (dsq0 < minDsq) {
                minDsq = dsq0;
                bestCentroidOffset = c0;
            }
            if (scoredDimensionsHistogram != null) {
                scoredDimensionsHistogram[vector.length]++;
                scoredDimensionsHistogram[vector.length]++;
                scoredDimensionsHistogram[vector.length]++;
                scoredDimensionsHistogram[vector.length]++;
                updateMinDimensions(minDimensionsStats, vector.length);
            }
            if (dsq1 < minDsq) {
                minDsq = dsq1;
                bestCentroidOffset = c1;
            }
            if (dsq2 < minDsq) {
                minDsq = dsq2;
                bestCentroidOffset = c2;
            }
            if (dsq3 < minDsq) {
                minDsq = dsq3;
                bestCentroidOffset = c3;
            }
        }
        for (; i < activeCount; i++) {
            int centroidOrd = candidateScratch[i];
            float dsq = candidateScoresScratch[i] + ESVectorUtil.squareDistance(
                vector,
                suffixOffset,
                centroids[centroidOrd],
                suffixOffset,
                suffixLength
            );
            if (scoredDimensionsHistogram != null) {
                scoredDimensionsHistogram[vector.length]++;
                updateMinDimensions(minDimensionsStats, vector.length);
            }
            if (dsq < minDsq) {
                minDsq = dsq;
                bestCentroidOffset = centroidOrd;
            }
        }
        return bestCentroidOffset;
    }

    /**
     * Find the closest centroid for a materialized vector, restricting the search to its
     * currently assigned centroid and that centroid's pre-computed neighborhood.
     *
     * @param vector       the vector to assign
     * @param centroids    the centroid vectors to compare against
     * @param centroidIdx  the index of the vector's current centroid assignment
     * @param neighborhood the neighborhood of {@code centroidIdx}, containing neighboring
     *                     centroid indices and the maximum intra-cluster distance
     * @param distances    scratch array of length 4 used for bulk distance results
     * @return the index into {@code centroids} of the nearest centroid (may be {@code centroidIdx}
     *         if no closer neighbor was found)
     */
    private static int computeBestCentroidFromNeighbours(
        float[] vector,
        float[][] centroids,
        int centroidIdx,
        NeighborHood neighborhood,
        float[] distances,
        float[] candidateScoresScratch,
        int[] candidateScratch,
        int[] candidateDimsScratch,
        int prefixDivisor,
        boolean enableThresholdPruning,
        long[] pruningStats,
        long[] scoredDimensionsHistogram,
        int[] minDimensionsStats
    ) {
        final int prefixLength = computePrefixLength(vector.length, prefixDivisor);
        final int suffixOffset = prefixLength;
        final int suffixLength = vector.length - prefixLength;
        final int[] neighbors = neighborhood.neighbors();
        if (usePrefixThresholdPruning(neighbors.length, suffixLength, enableThresholdPruning) == false) {
            pruningStats[0] += neighbors.length;
            pruningStats[1] += neighbors.length;
            if (scoredDimensionsHistogram != null) {
                scoredDimensionsHistogram[vector.length] += neighbors.length;
                updateMinDimensions(minDimensionsStats, vector.length);
            }
            return computeBestCentroidFromNeighboursFull(vector, centroids, centroidIdx, neighborhood, distances);
        }
        pruningStats[0] += neighbors.length;
        int bestCentroidOffset = centroidIdx;
        assert centroidIdx >= 0 && centroidIdx < centroids.length;
        float minDsq = ESVectorUtil.squareDistance(vector, centroids[centroidIdx]);
        int activeCount = neighbors.length;
        for (int i = 0; i < neighbors.length; i++) {
            int centroidOrd = neighbors[i];
            candidateScratch[i] = centroidOrd;
            candidateScoresScratch[i] = 0f;
            candidateDimsScratch[i] = 0;
        }
        for (int prefixOffset = 0; prefixOffset < prefixLength && activeCount > 0; prefixOffset += PREFIX_PRUNING_BATCH_DIMS) {
            if (minDsq < neighborhood.maxIntraDistance()) {
                if (scoredDimensionsHistogram != null) {
                    recordActiveDimensions(scoredDimensionsHistogram, minDimensionsStats, candidateDimsScratch, 0, activeCount);
                }
                return bestCentroidOffset;
            }
            final int batchLength = Math.min(PREFIX_PRUNING_BATCH_DIMS, prefixLength - prefixOffset);
            int kept = 0;
            int i = 0;
            int bulkLimit = activeCount - 3;
            for (; i < bulkLimit; i += 4) {
                int c0 = candidateScratch[i];
                int c1 = candidateScratch[i + 1];
                int c2 = candidateScratch[i + 2];
                int c3 = candidateScratch[i + 3];
                ESVectorUtil.squareDistanceBulk(
                    vector,
                    prefixOffset,
                    centroids[c0],
                    prefixOffset,
                    centroids[c1],
                    prefixOffset,
                    centroids[c2],
                    prefixOffset,
                    centroids[c3],
                    prefixOffset,
                    batchLength,
                    distances
                );
                float s0 = candidateScoresScratch[i] + distances[0];
                float s1 = candidateScoresScratch[i + 1] + distances[1];
                float s2 = candidateScoresScratch[i + 2] + distances[2];
                float s3 = candidateScoresScratch[i + 3] + distances[3];
                int d0 = candidateDimsScratch[i] + batchLength;
                int d1 = candidateDimsScratch[i + 1] + batchLength;
                int d2 = candidateDimsScratch[i + 2] + batchLength;
                int d3 = candidateDimsScratch[i + 3] + batchLength;
                if (enableThresholdPruning == false || s0 < minDsq) {
                    candidateScratch[kept++] = c0;
                    candidateScoresScratch[kept - 1] = s0;
                    candidateDimsScratch[kept - 1] = d0;
                } else if (scoredDimensionsHistogram != null) {
                    scoredDimensionsHistogram[d0]++;
                    updateMinDimensions(minDimensionsStats, d0);
                }
                if (enableThresholdPruning == false || s1 < minDsq) {
                    candidateScratch[kept++] = c1;
                    candidateScoresScratch[kept - 1] = s1;
                    candidateDimsScratch[kept - 1] = d1;
                } else if (scoredDimensionsHistogram != null) {
                    scoredDimensionsHistogram[d1]++;
                    updateMinDimensions(minDimensionsStats, d1);
                }
                if (enableThresholdPruning == false || s2 < minDsq) {
                    candidateScratch[kept++] = c2;
                    candidateScoresScratch[kept - 1] = s2;
                    candidateDimsScratch[kept - 1] = d2;
                } else if (scoredDimensionsHistogram != null) {
                    scoredDimensionsHistogram[d2]++;
                    updateMinDimensions(minDimensionsStats, d2);
                }
                if (enableThresholdPruning == false || s3 < minDsq) {
                    candidateScratch[kept++] = c3;
                    candidateScoresScratch[kept - 1] = s3;
                    candidateDimsScratch[kept - 1] = d3;
                } else if (scoredDimensionsHistogram != null) {
                    scoredDimensionsHistogram[d3]++;
                    updateMinDimensions(minDimensionsStats, d3);
                }
            }
            for (; i < activeCount; i++) {
                int centroidOrd = candidateScratch[i];
                float score = candidateScoresScratch[i] + ESVectorUtil.squareDistance(
                    vector,
                    prefixOffset,
                    centroids[centroidOrd],
                    prefixOffset,
                    batchLength
                );
                int dims = candidateDimsScratch[i] + batchLength;
                if (enableThresholdPruning == false || score < minDsq) {
                    candidateScratch[kept++] = centroidOrd;
                    candidateScoresScratch[kept - 1] = score;
                    candidateDimsScratch[kept - 1] = dims;
                } else if (scoredDimensionsHistogram != null) {
                    scoredDimensionsHistogram[dims]++;
                    updateMinDimensions(minDimensionsStats, dims);
                }
            }
            activeCount = kept;
        }
        final int refinementCap = refinementCapSize(neighbors.length);
        if (activeCount > refinementCap) {
            final int originalActiveCount = activeCount;
            final int[] originalCandidates = scoredDimensionsHistogram == null
                ? null
                : Arrays.copyOf(candidateScratch, originalActiveCount);
            final int[] originalDims = scoredDimensionsHistogram == null ? null : Arrays.copyOf(candidateDimsScratch, originalActiveCount);
            final NeighborQueue shortlist = new NeighborQueue(refinementCap, true);
            for (int i = 0; i < activeCount; i++) {
                shortlist.insertWithOverflow(candidateScratch[i], candidateScoresScratch[i]);
            }
            activeCount = shortlist.size();
            for (int i = 0; i < activeCount; i++) {
                long candidate = shortlist.popRaw();
                candidateScratch[i] = shortlist.decodeNodeId(candidate);
                candidateScoresScratch[i] = shortlist.decodeScore(candidate);
                if (scoredDimensionsHistogram != null) {
                    candidateDimsScratch[i] = findDimensions(originalCandidates, originalDims, originalActiveCount, candidateScratch[i]);
                }
            }
            if (scoredDimensionsHistogram != null && originalActiveCount > activeCount) {
                int[] selected = Arrays.copyOf(candidateScratch, activeCount);
                Arrays.sort(selected);
                for (int i = 0; i < originalActiveCount; i++) {
                    if (Arrays.binarySearch(selected, originalCandidates[i]) < 0) {
                        scoredDimensionsHistogram[originalDims[i]]++;
                        updateMinDimensions(minDimensionsStats, originalDims[i]);
                    }
                }
            }
        }
        pruningStats[1] += activeCount;
        if (suffixLength == 0) {
            if (scoredDimensionsHistogram != null) {
                recordActiveDimensions(scoredDimensionsHistogram, minDimensionsStats, candidateDimsScratch, 0, activeCount);
            }
            for (int i = 0; i < activeCount; i++) {
                int centroidOrd = candidateScratch[i];
                float dsq = candidateScoresScratch[i];
                if (dsq < minDsq) {
                    minDsq = dsq;
                    bestCentroidOffset = centroidOrd;
                }
            }
            return bestCentroidOffset;
        }
        int i = 0;
        int shortlistLimit = activeCount - 3;
        for (; i < shortlistLimit; i += 4) {
            if (minDsq < neighborhood.maxIntraDistance()) {
                if (scoredDimensionsHistogram != null) {
                    recordActiveDimensions(scoredDimensionsHistogram, minDimensionsStats, candidateDimsScratch, i, activeCount);
                }
                return bestCentroidOffset;
            }
            int c0 = candidateScratch[i];
            int c1 = candidateScratch[i + 1];
            int c2 = candidateScratch[i + 2];
            int c3 = candidateScratch[i + 3];
            ESVectorUtil.squareDistanceBulk(
                vector,
                suffixOffset,
                centroids[c0],
                suffixOffset,
                centroids[c1],
                suffixOffset,
                centroids[c2],
                suffixOffset,
                centroids[c3],
                suffixOffset,
                suffixLength,
                distances
            );
            float dsq0 = candidateScoresScratch[i] + distances[0];
            float dsq1 = candidateScoresScratch[i + 1] + distances[1];
            float dsq2 = candidateScoresScratch[i + 2] + distances[2];
            float dsq3 = candidateScoresScratch[i + 3] + distances[3];
            if (scoredDimensionsHistogram != null) {
                scoredDimensionsHistogram[vector.length]++;
                scoredDimensionsHistogram[vector.length]++;
                scoredDimensionsHistogram[vector.length]++;
                scoredDimensionsHistogram[vector.length]++;
                updateMinDimensions(minDimensionsStats, vector.length);
            }
            if (dsq0 < minDsq) {
                minDsq = dsq0;
                bestCentroidOffset = c0;
            }
            if (dsq1 < minDsq) {
                minDsq = dsq1;
                bestCentroidOffset = c1;
            }
            if (dsq2 < minDsq) {
                minDsq = dsq2;
                bestCentroidOffset = c2;
            }
            if (dsq3 < minDsq) {
                minDsq = dsq3;
                bestCentroidOffset = c3;
            }
        }
        for (; i < activeCount; i++) {
            if (minDsq < neighborhood.maxIntraDistance()) {
                if (scoredDimensionsHistogram != null) {
                    recordActiveDimensions(scoredDimensionsHistogram, minDimensionsStats, candidateDimsScratch, i, activeCount);
                }
                return bestCentroidOffset;
            }
            int centroidOrd = candidateScratch[i];
            float dsq = candidateScoresScratch[i] + ESVectorUtil.squareDistance(
                vector,
                suffixOffset,
                centroids[centroidOrd],
                suffixOffset,
                suffixLength
            );
            if (scoredDimensionsHistogram != null) {
                scoredDimensionsHistogram[vector.length]++;
                updateMinDimensions(minDimensionsStats, vector.length);
            }
            if (dsq < minDsq) {
                minDsq = dsq;
                bestCentroidOffset = centroidOrd;
            }
        }
        return bestCentroidOffset;
    }

    private static int refinementCapSize(int candidateCount) {
        if (candidateCount <= PREFIX_SHORTLIST_MIN_CANDIDATES) {
            return candidateCount;
        }
        int shortlist = Math.max(PREFIX_REFINEMENT_CAP_MIN, Math.round(candidateCount * PREFIX_REFINEMENT_CAP_RATIO));
        return Math.max(1, Math.min(candidateCount, shortlist));
    }

    private static float loadPrefixRefinementCapRatio() {
        String value = System.getProperty(PREFIX_REFINEMENT_CAP_RATIO_PROPERTY);
        if (value == null) {
            return 0.125f;
        }
        try {
            float ratio = Float.parseFloat(value);
            if (ratio <= 0f || ratio > 1f) {
                return 0.125f;
            }
            return ratio;
        } catch (NumberFormatException e) {
            return 0.125f;
        }
    }

    private static void recordActiveDimensions(
        long[] histogram,
        int[] minDimensionsStats,
        int[] candidateDimsScratch,
        int start,
        int endExclusive
    ) {
        for (int i = start; i < endExclusive; i++) {
            int dims = candidateDimsScratch[i];
            histogram[dims]++;
            updateMinDimensions(minDimensionsStats, dims);
        }
    }

    private static void updateMinDimensions(int[] minDimensionsStats, int dims) {
        minDimensionsStats[0] = Math.min(minDimensionsStats[0], dims);
    }

    private static int findDimensions(int[] candidateIds, int[] candidateDims, int length, int candidateId) {
        for (int i = 0; i < length; i++) {
            if (candidateIds[i] == candidateId) {
                return candidateDims[i];
            }
        }
        return 0;
    }

    private static int computeBestCentroidFull(float[] vector, float[][] centroids, float[] distances) {
        final int limit = centroids.length - 3;
        int bestCentroidOffset = 0;
        float minDsq = Float.MAX_VALUE;
        int i = 0;
        for (; i < limit; i += 4) {
            ESVectorUtil.squareDistanceBulk(vector, centroids[i], centroids[i + 1], centroids[i + 2], centroids[i + 3], distances);
            for (int j = 0; j < distances.length; j++) {
                float dsq = distances[j];
                if (dsq < minDsq) {
                    minDsq = dsq;
                    bestCentroidOffset = i + j;
                }
            }
        }
        for (; i < centroids.length; i++) {
            float dsq = ESVectorUtil.squareDistance(vector, centroids[i]);
            if (dsq < minDsq) {
                minDsq = dsq;
                bestCentroidOffset = i;
            }
        }
        return bestCentroidOffset;
    }

    private static int computeBestCentroidFromNeighboursFull(
        float[] vector,
        float[][] centroids,
        int centroidIdx,
        NeighborHood neighborhood,
        float[] distances
    ) {
        final int[] neighbors = neighborhood.neighbors();
        final int limit = neighbors.length - 3;
        int bestCentroidOffset = centroidIdx;
        assert centroidIdx >= 0 && centroidIdx < centroids.length;
        float minDsq = ESVectorUtil.squareDistance(vector, centroids[centroidIdx]);
        int i = 0;
        for (; i < limit; i += 4) {
            if (minDsq < neighborhood.maxIntraDistance()) {
                return bestCentroidOffset;
            }
            ESVectorUtil.squareDistanceBulk(
                vector,
                centroids[neighbors[i]],
                centroids[neighbors[i + 1]],
                centroids[neighbors[i + 2]],
                centroids[neighbors[i + 3]],
                distances
            );
            for (int j = 0; j < distances.length; j++) {
                float dsq = distances[j];
                if (dsq < minDsq) {
                    minDsq = dsq;
                    bestCentroidOffset = neighbors[i + j];
                }
            }
        }
        for (; i < neighbors.length; i++) {
            if (minDsq < neighborhood.maxIntraDistance()) {
                return bestCentroidOffset;
            }
            int offset = neighbors[i];
            assert offset >= 0 && offset < centroids.length : "Invalid neighbor offset: " + offset;
            float dsq = ESVectorUtil.squareDistance(vector, centroids[offset]);
            if (dsq < minDsq) {
                minDsq = dsq;
                bestCentroidOffset = offset;
            }
        }
        return bestCentroidOffset;
    }

    private static boolean usePrefixThresholdPruning(int candidateCount, int suffixLength, boolean enableThresholdPruning) {
        if (enableThresholdPruning == false) {
            return false;
        }
        if (suffixLength <= 0 || candidateCount < PREFIX_SHORTLIST_MIN_CANDIDATES) {
            return false;
        }
        return true;
    }

    private static int computePrefixLength(int dimension, int prefixDivisor) {
        if (dimension <= 0) {
            return 0;
        }
        int divisor = Math.max(1, prefixDivisor);
        int prefix = Math.max(PREFIX_MIN_DIMENSIONS, dimension / divisor);
        return Math.min(prefix, dimension);
    }

    private static int computeSoarAssignment(
        float[] vector,
        float[][] centroids,
        int currAssignment,
        int centroidCount,
        IntToIntFunction centroidOrds,
        float soarLambda,
        float[] diffs,
        float[] distances
    ) {
        float[] currentCentroid = centroids[currAssignment];
        // TODO: cache these?
        float vectorCentroidDist = ESVectorUtil.squareDistance(vector, currentCentroid);
        if (vectorCentroidDist <= SOAR_MIN_DISTANCE) {
            return NO_SOAR_ASSIGNMENT; // no SOAR assignment
        }

        for (int j = 0; j < diffs.length; j++) {
            diffs[j] = vector[j] - currentCentroid[j];
        }

        final int limit = centroidCount - 3;
        int bestAssignment = -1;
        float minSoar = Float.MAX_VALUE;
        int j = 0;
        for (; j < limit; j += 4) {
            ESVectorUtil.soarDistanceBulk(
                vector,
                centroids[centroidOrds.apply(j)],
                centroids[centroidOrds.apply(j + 1)],
                centroids[centroidOrds.apply(j + 2)],
                centroids[centroidOrds.apply(j + 3)],
                diffs,
                soarLambda,
                vectorCentroidDist,
                distances
            );
            for (int k = 0; k < distances.length; k++) {
                float soar = distances[k];
                if (soar < minSoar) {
                    minSoar = soar;
                    bestAssignment = centroidOrds.apply(j + k);
                }
            }
        }

        for (; j < centroidCount; j++) {
            int centroidOrd = centroidOrds.apply(j);
            float soar = ESVectorUtil.soarDistance(vector, centroids[centroidOrd], diffs, soarLambda, vectorCentroidDist);
            if (soar < minSoar) {
                minSoar = soar;
                bestAssignment = centroidOrd;
            }
        }
        assert bestAssignment != -1 : "Failed to assign soar vector to centroid";
        return bestAssignment;
    }
}
