/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.index.codec.vectors.diskbbq.CsrOverspillAssignments;
import org.elasticsearch.index.codec.vectors.diskbbq.OverspillAssignments;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * POC alternative to {@link Soar}: instead of a vector choosing its own secondary ("spilled")
 * centroid, each centroid {@code C} pulls in the true nearest neighbors of a sample of its own
 * primary members. For each sampled member {@code x}, its own {@code nearestCentroidCount}
 * nearest centroids are picked individually out of {@code C}'s precomputed neighborhood, scored
 * the same way {@link Soar} scores {@code x}'s own secondary assignment (SOAR distance relative
 * to {@code x}'s residual from {@code C}, not raw Euclidean distance) so the picked centroids are
 * directionally complementary to {@code C} rather than merely nearby. {@code x}'s nearest
 * neighbors are then pulled from the primary members of just those centroids using plain
 * Euclidean distance, since that step is a retrieval-relevance question rather than a
 * coverage-diversity one. When trimming down to the hard limit, {@link TrimMode} controls which
 * candidates survive: the default {@link TrimMode#PRIORITY} keeps multi-pulled candidates first
 * (broad agreement across independent subset members outweighs raw distance), while the opt-in
 * {@link TrimMode#EXP_WEIGHTED} instead scores every candidate by a single continuous
 * distance-decayed frequency score. This gives finer-grained, neighbor-aware overspill than
 * SOAR's single secondary assignment, at the cost of an {@code O(subset * (neighborhood + pool))}
 * search per centroid at index time.
 */
public final class ForwardLinkOverspill {

    private static final Logger logger = LogManager.getLogger(ForwardLinkOverspill.class);

    // mirrors Soar.SOAR_MIN_DISTANCE: below this, x's residual from C is too small to divide by
    // safely in the SOAR distance formula.
    private static final float MIN_RESIDUAL_DISTANCE = 1e-16f;

    private ForwardLinkOverspill() {}

    /** How trimming picks which pulled-in candidates to keep once a centroid exceeds its budget. */
    public enum TrimMode {
        /** Vectors pulled by more than one subset member are kept first (sorted by pull count desc,
         *  then distance-to-C asc); remaining capacity round-robins the single-pulled leftovers. */
        PRIORITY,
        /** Every candidate is scored by {@code sum(exp(-pullDistance / scoreDecay))} across all the
         *  subset members that pulled it, and the top-scoring candidates (by that single continuous
         *  score) are kept - frequency and proximity blend smoothly instead of a hard two-tier split. */
        EXP_WEIGHTED
    }

    private static final float DEFAULT_SCORE_DECAY = 1.0f;

    /**
     * @param nnK number of nearest neighbors ("overspilled_nn_k") pulled in per sampled primary member
     * @param primarySubsetPercent percentage ("overspill_primary_subset_percent") of a centroid's
     *                             primary members sampled to search for pull-in candidates
     * @param nearestCentroidCount number of nearest neighboring centroids ("overspill_nearest_centroid_count")
     *                             whose primary members form the candidate pool
     * @param limitMultiple hard cap on pulled-in vectors per centroid ("overspill_limit_multiple"),
     *                      as a multiple of that centroid's own primary member count (adapts to
     *                      per-cluster size variance rather than assuming every cluster hits a
     *                      global target size)
     * @param trimMode how to pick which candidates survive trimming ("overspill_trim_mode")
     * @param scoreDecay the {@code exp(-dist/scoreDecay)} scale used only in {@link TrimMode#EXP_WEIGHTED}
     *                   ("overspill_score_decay")
     */
    public record Params(
        int nnK,
        float primarySubsetPercent,
        int nearestCentroidCount,
        float limitMultiple,
        TrimMode trimMode,
        float scoreDecay
    ) {
        public Params {
            if (nnK <= 0) {
                throw new IllegalArgumentException("nnK must be > 0, got: " + nnK);
            }
            if (primarySubsetPercent <= 0 || primarySubsetPercent > 100) {
                throw new IllegalArgumentException("primarySubsetPercent must be in (0, 100], got: " + primarySubsetPercent);
            }
            if (nearestCentroidCount <= 0) {
                throw new IllegalArgumentException("nearestCentroidCount must be > 0, got: " + nearestCentroidCount);
            }
            if (limitMultiple <= 0) {
                throw new IllegalArgumentException("limitMultiple must be > 0, got: " + limitMultiple);
            }
            if (trimMode == null) {
                throw new IllegalArgumentException("trimMode must not be null");
            }
            if (scoreDecay <= 0) {
                throw new IllegalArgumentException("scoreDecay must be > 0, got: " + scoreDecay);
            }
        }

        public Params(int nnK, float primarySubsetPercent, int nearestCentroidCount, float limitMultiple) {
            this(nnK, primarySubsetPercent, nearestCentroidCount, limitMultiple, TrimMode.PRIORITY, DEFAULT_SCORE_DECAY);
        }
    }

    /**
     * Computes forward-link overspill assignments for a finished clustering. Requires
     * {@code neighborhoods} (the nearest neighboring centroids per centroid); if absent, or there
     * is only a single centroid, there is nothing to pull from and {@link OverspillAssignments#NONE}
     * is returned, mirroring {@link HierarchicalKMeans#computeSoar}'s guard.
     */
    public static <V> OverspillAssignments computeOverspill(
        CentroidOps<V> ops,
        ClusteringVectorValues<V> vectors,
        KMeansResult<V> kMeans,
        NeighborHood[] neighborhoods,
        Params params
    ) throws IOException {
        V[] centroids = kMeans.centroids();
        if (centroids.length <= 1 || neighborhoods == null) {
            return OverspillAssignments.NONE;
        }
        int[][] membersByCentroid = membersByCentroid(kMeans.assignments(), centroids.length);

        PairAccumulator pairs = new PairAccumulator();
        NeighborQueue nnQueue = new NeighborQueue(params.nnK(), true);
        NeighborQueue centroidQueue = new NeighborQueue(params.nearestCentroidCount(), true);
        float[] diffs = new float[vectors.dimension()];

        long totalClusterSize = 0;
        long totalCandidates = 0;
        long totalKept = 0;
        int minClusterSize = Integer.MAX_VALUE;
        int maxClusterSize = 0;
        for (int c = 0; c < centroids.length; c++) {
            CentroidStats stats = pullForCentroid(
                ops,
                vectors,
                centroids,
                membersByCentroid,
                neighborhoods,
                c,
                params,
                nnQueue,
                centroidQueue,
                diffs,
                pairs
            );
            totalClusterSize += stats.clusterSize();
            totalCandidates += stats.candidates();
            totalKept += stats.kept();
            minClusterSize = Math.min(minClusterSize, stats.clusterSize());
            maxClusterSize = Math.max(maxClusterSize, stats.clusterSize());
        }

        if (logger.isDebugEnabled()) {
            // overspillRate: how much bigger each posting list gets, on average, relative to its
            // own primary assignment (i.e. replication factor - 1).
            // assignedOfCandidates: of all the (deduped) pull-in candidates considered, what
            // fraction actually survived trimming into the hard limit.
            double avgClusterSize = (double) totalClusterSize / centroids.length;
            double avgCandidates = (double) totalCandidates / centroids.length;
            double avgKept = (double) totalKept / centroids.length;
            double overspillRatePct = totalClusterSize > 0 ? 100.0 * totalKept / totalClusterSize : 0.0;
            double assignedOfCandidatesPct = totalCandidates > 0 ? 100.0 * totalKept / totalCandidates : 0.0;
            logger.debug(
                "forward-link overspill stats: centroids=[{}] clusterSize=[avg={}, min={}, max={}] "
                    + "candidates=[avg={}] kept=[avg={}] overspillRate=[{}% of primary size] "
                    + "assignedOfCandidates=[{}%]",
                centroids.length,
                avgClusterSize,
                minClusterSize,
                maxClusterSize,
                avgCandidates,
                avgKept,
                overspillRatePct,
                assignedOfCandidatesPct
            );
        }
        return pairs.buildCsr(vectors.size());
    }

    /**
     * @param clusterSize number of primary members assigned to the centroid
     * @param candidates number of distinct pull-in candidates considered (after dedup, before trim)
     * @param kept number of candidates that actually survived trimming and were assigned as overspill
     */
    private record CentroidStats(int clusterSize, int candidates, int kept) {}

    private static <V> CentroidStats pullForCentroid(
        CentroidOps<V> ops,
        ClusteringVectorValues<V> vectors,
        V[] centroids,
        int[][] membersByCentroid,
        NeighborHood[] neighborhoods,
        int c,
        Params params,
        NeighborQueue nnQueue,
        NeighborQueue centroidQueue,
        float[] diffs,
        PairAccumulator pairs
    ) throws IOException {
        int[] members = membersByCentroid[c];
        if (members.length == 0) {
            return new CentroidStats(0, 0, 0);
        }
        // per-cluster-relative budget: adapts to this centroid's actual size instead of assuming
        // every cluster hits the global vectorPerCluster target.
        int limit = Math.max(1, Math.round(params.limitMultiple() * members.length));
        int subsetSize = Math.min(members.length, Math.max(1, Math.round(params.primarySubsetPercent() / 100f * members.length)));
        int[] subset = sampleSubset(members, subsetSize, c);

        int[] neighborCentroids = neighborhoods[c].neighbors();
        if (neighborCentroids.length == 0) {
            return new CentroidStats(members.length, 0, 0);
        }
        V centroid = centroids[c];

        // the first subset member to pull a candidate claims it for round-robin bucketing; later
        // duplicate pulls of the same vector by other subset members are dropped from bySource, but
        // still counted in pullCount (a vector pulled by multiple independent subset members is
        // more broadly agreed-upon and is biased toward being kept when trimming below).
        Map<Integer, Integer> sourceOf = new HashMap<>();
        Map<Integer, Integer> pullCount = new HashMap<>();
        Map<Integer, Double> expScore = new HashMap<>();
        Map<Integer, List<Integer>> bySource = new LinkedHashMap<>();
        for (int x : subset) {
            bySource.put(x, new ArrayList<>());
        }
        for (int x : subset) {
            V vx = ops.copyOf(vectors.vectorValue(x));

            // each subset member individually picks its own nearestCentroidCount nearest centroids
            // out of C's neighborhood, scored by SOAR distance relative to x's own residual from C
            // (the same scoring SOAR itself uses), so the picked centroids complement C instead of
            // just duplicating coverage in the direction x already leans.
            float vectorCentroidDist = ops.squareDistance(vx, centroid);
            centroidQueue.clear();
            if (vectorCentroidDist <= MIN_RESIDUAL_DISTANCE) {
                // x sits (almost) exactly on C: its residual direction is undefined, so the SOAR
                // penalty term would divide by ~0. Mirrors Soar's own SOAR_MIN_DISTANCE guard;
                // fall back to plain distance for this x rather than propagating a NaN score.
                for (int neighborCentroid : neighborCentroids) {
                    centroidQueue.insertWithOverflow(neighborCentroid, ops.squareDistance(vx, centroids[neighborCentroid]));
                }
            } else {
                ops.computeDiffs(vx, centroid, diffs);
                for (int neighborCentroid : neighborCentroids) {
                    float soarDistance = ops.soarDistance(
                        vx,
                        centroids[neighborCentroid],
                        diffs,
                        HierarchicalKMeans.DEFAULT_SOAR_LAMBDA,
                        vectorCentroidDist
                    );
                    centroidQueue.insertWithOverflow(neighborCentroid, soarDistance);
                }
            }
            int[] ownNeighborCentroids = new int[centroidQueue.size()];
            for (int i = ownNeighborCentroids.length - 1; i >= 0; i--) {
                ownNeighborCentroids[i] = centroidQueue.pop();
            }
            int[] pool = candidatePool(membersByCentroid, ownNeighborCentroids);
            if (pool.length == 0) {
                continue;
            }

            nnQueue.clear();
            for (int v : pool) {
                nnQueue.insertWithOverflow(v, ops.squareDistance(vx, vectors.vectorValue(v)));
            }
            while (nnQueue.size() > 0) {
                float pullDistance = nnQueue.topScore();
                int v = nnQueue.pop();
                pullCount.merge(v, 1, Integer::sum);
                expScore.merge(v, Math.exp(-pullDistance / params.scoreDecay()), Double::sum);
                if (sourceOf.putIfAbsent(v, x) == null) {
                    bySource.get(x).add(v);
                }
            }
        }
        if (sourceOf.isEmpty()) {
            return new CentroidStats(members.length, 0, 0);
        }

        if (sourceOf.size() <= limit) {
            for (int v : sourceOf.keySet()) {
                pairs.add(v, c);
            }
            return new CentroidStats(members.length, sourceOf.size(), sourceOf.size());
        }

        if (params.trimMode() == TrimMode.EXP_WEIGHTED) {
            // frequency and proximity blend into one continuous score instead of a hard two-tier
            // split: keep the top-limit candidates by summed exp(-pullDistance/scoreDecay).
            List<Integer> byScore = new ArrayList<>(sourceOf.keySet());
            byScore.sort((a, b) -> Double.compare(expScore.get(b), expScore.get(a)));
            for (int i = 0; i < limit; i++) {
                pairs.add(byScore.get(i), c);
            }
            return new CentroidStats(members.length, sourceOf.size(), limit);
        }

        Map<Integer, Float> distToC = new HashMap<>(sourceOf.size());
        for (int v : sourceOf.keySet()) {
            distToC.put(v, ops.squareDistance(centroid, vectors.vectorValue(v)));
        }

        // vectors pulled in by more than one subset member are more broadly agreed-upon and are
        // kept preferentially (by pull count, then nearest-to-C), even over single-pulled vectors
        // that sit closer to C.
        List<Integer> multiPulled = new ArrayList<>();
        for (int v : sourceOf.keySet()) {
            if (pullCount.get(v) > 1) {
                multiPulled.add(v);
            }
        }
        multiPulled.sort((a, b) -> {
            int cmp = Integer.compare(pullCount.get(b), pullCount.get(a));
            return cmp != 0 ? cmp : Float.compare(distToC.get(a), distToC.get(b));
        });

        int kept = 0;
        if (multiPulled.size() >= limit) {
            for (int i = 0; i < limit; i++) {
                pairs.add(multiPulled.get(i), c);
            }
            return new CentroidStats(members.length, sourceOf.size(), limit);
        }
        for (int v : multiPulled) {
            pairs.add(v, c);
            kept++;
        }

        // fill remaining capacity via round-robin across sources among the leftover single-pulled
        // candidates, nearest-to-C first per source, preserving source diversity for the rest.
        for (List<Integer> bucket : bySource.values()) {
            bucket.removeIf(v -> pullCount.get(v) > 1);
            bucket.sort((a, b) -> Float.compare(distToC.get(a), distToC.get(b)));
        }

        int[] cursors = new int[subset.length];
        boolean progress = true;
        while (kept < limit && progress) {
            progress = false;
            for (int i = 0; i < subset.length && kept < limit; i++) {
                List<Integer> bucket = bySource.get(subset[i]);
                if (cursors[i] < bucket.size()) {
                    pairs.add(bucket.get(cursors[i]), c);
                    cursors[i]++;
                    kept++;
                    progress = true;
                }
            }
        }
        return new CentroidStats(members.length, sourceOf.size(), kept);
    }

    private static int[][] membersByCentroid(int[] assignments, int numCentroids) {
        int[] counts = new int[numCentroids];
        for (int c : assignments) {
            counts[c]++;
        }
        int[][] members = new int[numCentroids][];
        for (int c = 0; c < numCentroids; c++) {
            members[c] = new int[counts[c]];
        }
        Arrays.fill(counts, 0);
        for (int i = 0; i < assignments.length; i++) {
            int c = assignments[i];
            members[c][counts[c]++] = i;
        }
        return members;
    }

    /** Deterministic (seeded by centroid ordinal) partial Fisher-Yates sample of {@code subsetSize} members. */
    private static int[] sampleSubset(int[] members, int subsetSize, int centroidOrd) {
        if (subsetSize >= members.length) {
            return members;
        }
        int[] pool = Arrays.copyOf(members, members.length);
        Random random = new Random(centroidOrd);
        for (int i = 0; i < subsetSize; i++) {
            int j = i + random.nextInt(pool.length - i);
            int tmp = pool[i];
            pool[i] = pool[j];
            pool[j] = tmp;
        }
        return Arrays.copyOf(pool, subsetSize);
    }

    private static int[] candidatePool(int[][] membersByCentroid, int[] neighborCentroids) {
        int total = 0;
        for (int neighborCentroid : neighborCentroids) {
            total += membersByCentroid[neighborCentroid].length;
        }
        int[] pool = new int[total];
        int pos = 0;
        for (int neighborCentroid : neighborCentroids) {
            int[] neighborMembers = membersByCentroid[neighborCentroid];
            System.arraycopy(neighborMembers, 0, pool, pos, neighborMembers.length);
            pos += neighborMembers.length;
        }
        return pool;
    }

    /** Accumulates {@code (vectorOrdinal, centroidOrdinal)} pulled-in pairs and compresses them into CSR form. */
    private static final class PairAccumulator {
        private int[] vectorOrds = new int[64];
        private int[] centroidOrds = new int[64];
        private int size = 0;

        void add(int vectorOrd, int centroidOrd) {
            if (size == vectorOrds.length) {
                vectorOrds = ArrayUtil.grow(vectorOrds, size + 1);
                centroidOrds = ArrayUtil.grow(centroidOrds, size + 1);
            }
            vectorOrds[size] = vectorOrd;
            centroidOrds[size] = centroidOrd;
            size++;
        }

        OverspillAssignments buildCsr(int numVectors) {
            if (size == 0) {
                return OverspillAssignments.NONE;
            }
            int[] counts = new int[numVectors];
            for (int i = 0; i < size; i++) {
                counts[vectorOrds[i]]++;
            }
            int[] offsets = new int[numVectors + 1];
            for (int v = 0; v < numVectors; v++) {
                offsets[v + 1] = offsets[v] + counts[v];
            }
            int[] fillPos = Arrays.copyOf(offsets, offsets.length);
            int[] values = new int[size];
            for (int i = 0; i < size; i++) {
                int v = vectorOrds[i];
                values[fillPos[v]++] = centroidOrds[i];
            }
            return new CsrOverspillAssignments(offsets, values);
        }
    }
}
