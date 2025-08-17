/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.elasticsearch.index.codec.vectors.reflect.OffHeapStats;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Map;

import static org.apache.lucene.codecs.lucene102.Lucene102BinaryQuantizedVectorsFormat.QUERY_BITS;
import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
import static org.elasticsearch.index.codec.vectors.BQSpaceUtils.transposeHalfByte;
import static org.elasticsearch.index.codec.vectors.BQVectorUtils.discretize;
import static org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer.DEFAULT_LAMBDA;
import static org.elasticsearch.simdvec.ES91OSQVectorsScorer.BULK_SIZE;

/**
 * Default implementation of {@link IVFVectorsReader}. It scores the posting lists centroids using
 * brute force and then scores the top ones using the posting list.
 */
public class DefaultIVFVectorsReader extends IVFVectorsReader implements OffHeapStats {

    private static final Logger logger = LogManager.getLogger(DefaultIVFVectorsReader.class);
    // The percentage of centroids that are scored to keep recall
    public static final double CENTROID_SAMPLING_PERCENTAGE = 0.2;

    public DefaultIVFVectorsReader(SegmentReadState state, FlatVectorsReader rawVectorsReader) throws IOException {
        super(state, rawVectorsReader);
    }

    @Override
    CentroidIterator getCentroidIterator(FieldInfo fieldInfo, int numCentroids, IndexInput centroids, float[] targetQuery)
        throws IOException {
        final FieldEntry fieldEntry = fields.get(fieldInfo.number);
        final float globalCentroidDp = fieldEntry.globalCentroidDp();
        final OptimizedScalarQuantizer scalarQuantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        final int[] scratch = new int[targetQuery.length];
        float[] targetQueryCopy = ArrayUtil.copyArray(targetQuery);
        if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
            VectorUtil.l2normalize(targetQueryCopy);
        }
        final OptimizedScalarQuantizer.QuantizationResult queryParams = scalarQuantizer.scalarQuantize(
            targetQueryCopy,
            scratch,
            (byte) 7,
            fieldEntry.globalCentroid()
        );
        final byte[] quantized = new byte[targetQuery.length];
        for (int i = 0; i < quantized.length; i++) {
            quantized[i] = (byte) scratch[i];
        }
        final ES92Int7VectorsScorer scorer = ESVectorUtil.getES92Int7VectorsScorer(centroids, fieldInfo.getVectorDimension());
        centroids.seek(0L);
        int numParents = centroids.readVInt();
        if (numParents > 0) {
            return getCentroidIteratorWithParents(
                fieldInfo,
                centroids,
                numParents,
                numCentroids,
                scorer,
                quantized,
                queryParams,
                globalCentroidDp
            );
        }
        return getCentroidIteratorNoParent(fieldInfo, centroids, numCentroids, scorer, quantized, queryParams, globalCentroidDp);
    }

    private static CentroidIterator getCentroidIteratorNoParent(
        FieldInfo fieldInfo,
        IndexInput centroids,
        int numCentroids,
        ES92Int7VectorsScorer scorer,
        byte[] quantizeQuery,
        OptimizedScalarQuantizer.QuantizationResult queryParams,
        float globalCentroidDp
    ) throws IOException {
        final NeighborQueue neighborQueue = new NeighborQueue(numCentroids, true);
        score(
            neighborQueue,
            numCentroids,
            0,
            scorer,
            quantizeQuery,
            queryParams,
            globalCentroidDp,
            fieldInfo.getVectorSimilarityFunction(),
            new float[ES92Int7VectorsScorer.BULK_SIZE]
        );
        long offset = centroids.getFilePointer();
        return new CentroidIterator() {
            float score = Float.NEGATIVE_INFINITY;
            float radius = Float.NEGATIVE_INFINITY;
            float additionalBlockSkippingMetric = Float.NEGATIVE_INFINITY;

            @Override
            public boolean hasNext() {
                return neighborQueue.size() > 0;
            }

            @Override
            public long nextPostingListOffset() throws IOException {
                score = neighborQueue.topScore();
                int centroidOrdinal = neighborQueue.pop();
                centroids.seek(offset + (long) (Long.BYTES + 2 * Integer.BYTES) * centroidOrdinal);
                long postingListOffset = centroids.readLong();
                radius = Float.intBitsToFloat(centroids.readInt());
                additionalBlockSkippingMetric = Float.intBitsToFloat(centroids.readInt());
                return postingListOffset;
            }

            @Override
            public float score() {
                return score;
            }

            @Override
            public float radius() {
                return radius;
            }

            @Override
            public float additionalBlockSkippingMetric() {
                return additionalBlockSkippingMetric;
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
        float globalCentroidDp
    ) throws IOException {
        // build the three queues we are going to use
        final NeighborQueue parentsQueue = new NeighborQueue(numParents, true);
        final int maxChildrenSize = centroids.readVInt();
        final NeighborQueue currentParentQueue = new NeighborQueue(maxChildrenSize, true);
        final int bufferSize = (int) Math.max(numCentroids * CENTROID_SAMPLING_PERCENTAGE, 1);
        final NeighborQueue neighborQueue = new NeighborQueue(bufferSize, true);
        // score the parents
        final float[] scores = new float[ES92Int7VectorsScorer.BULK_SIZE];
        score(
            parentsQueue,
            numParents,
            0,
            scorer,
            quantizeQuery,
            queryParams,
            globalCentroidDp,
            fieldInfo.getVectorSimilarityFunction(),
            scores
        );
        final long centroidQuantizeSize = fieldInfo.getVectorDimension() + 3 * Float.BYTES + Integer.BYTES;
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
                scores
            );
            while (currentParentQueue.size() > 0 && neighborQueue.size() < bufferSize) {
                final float score = currentParentQueue.topScore();
                final int children = currentParentQueue.pop();
                neighborQueue.add(children, score);
            }
        }
        final long childrenFileOffsets = childrenOffset + centroidQuantizeSize * numCentroids;
        return new CentroidIterator() {
            float score = Float.NEGATIVE_INFINITY;
            float radius = Float.NEGATIVE_INFINITY;
            float skippingMetric = Float.NEGATIVE_INFINITY;

            @Override
            public boolean hasNext() {
                return neighborQueue.size() > 0;
            }

            @Override
            public long nextPostingListOffset() throws IOException {
                score = neighborQueue.topScore();
                int centroidOrdinal = neighborQueue.pop();
                updateQueue(); // add one children if available so the queue remains fully populated
                centroids.seek(childrenFileOffsets + (long) (Long.BYTES + 2 * Integer.BYTES) * centroidOrdinal);
                long postingListOffset = centroids.readLong();
                radius = Float.intBitsToFloat(centroids.readInt());
                skippingMetric = Float.intBitsToFloat(centroids.readInt());
                return postingListOffset;
            }

            @Override
            public float score() {
                return score;
            }

            @Override
            public float radius() {
                return radius;
            }

            @Override
            public float additionalBlockSkippingMetric() {
                return skippingMetric;
            }

            private void updateQueue() throws IOException {
                if (currentParentQueue.size() > 0) {
                    // add a children from the current parent queue
                    float score = currentParentQueue.topScore();
                    int children = currentParentQueue.pop();
                    neighborQueue.add(children, score);
                } else if (parentsQueue.size() > 0) {
                    // add a new parent from the current parent queue
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
                        scores
                    );
                    updateQueue();
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
        float[] scores
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
            quantizeQuery,
            queryParams,
            globalCentroidDp,
            fieldInfo.getVectorSimilarityFunction(),
            scores
        );
    }

    private static void score(
        NeighborQueue neighborQueue,
        int size,
        int scoresOffset,
        ES92Int7VectorsScorer scorer,
        byte[] quantizeQuery,
        OptimizedScalarQuantizer.QuantizationResult queryCorrections,
        float centroidDp,
        VectorSimilarityFunction similarityFunction,
        float[] scores
    ) throws IOException {
        int limit = size - ES92Int7VectorsScorer.BULK_SIZE + 1;
        int i = 0;
        for (; i < limit; i += ES92Int7VectorsScorer.BULK_SIZE) {
            scorer.scoreBulk(
                quantizeQuery,
                queryCorrections.lowerInterval(),
                queryCorrections.upperInterval(),
                queryCorrections.quantizedComponentSum(),
                queryCorrections.additionalCorrection(),
                similarityFunction,
                centroidDp,
                scores
            );
            for (int j = 0; j < ES92Int7VectorsScorer.BULK_SIZE; j++) {
                neighborQueue.add(scoresOffset + i + j, scores[j]);
            }
        }

        for (; i < size; i++) {
            float score = scorer.score(
                quantizeQuery,
                queryCorrections.lowerInterval(),
                queryCorrections.upperInterval(),
                queryCorrections.quantizedComponentSum(),
                queryCorrections.additionalCorrection(),
                similarityFunction,
                centroidDp
            );
            neighborQueue.add(scoresOffset + i, score);
        }
    }

    @Override
    PostingVisitor getPostingVisitor(FieldInfo fieldInfo, IndexInput indexInput, float[] target, Bits acceptDocs) throws IOException {
        FieldEntry entry = fields.get(fieldInfo.number);
        final int maxPostingListSize = indexInput.readVInt();
        return new MemorySegmentPostingsVisitor(target, indexInput, entry, fieldInfo, maxPostingListSize, acceptDocs);
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        return Map.of();
    }

    private static class MemorySegmentPostingsVisitor implements PostingVisitor {
        final long quantizedByteLength;
        final IndexInput indexInput;
        final float[] target;
        final FieldEntry entry;
        final FieldInfo fieldInfo;
        final Bits acceptDocs;
        private final ES91OSQVectorsScorer osqVectorsScorer;
        final float[] scores = new float[BULK_SIZE];
        final float[] correctionsLower = new float[BULK_SIZE];
        final float[] correctionsUpper = new float[BULK_SIZE];
        final int[] correctionsSum = new int[BULK_SIZE];
        final float[] correctionsAdd = new float[BULK_SIZE];
        final int[] docIdsScratch;

        int vectors;
        boolean quantized = false;
        float centroidDp;
        final float[] centroid;
        long slicePos;
        OptimizedScalarQuantizer.QuantizationResult queryCorrections;

        final float[] scratch;
        final int[] quantizationScratch;
        final byte[] quantizedQueryScratch;
        final OptimizedScalarQuantizer quantizer;
        final DocIdsWriter idsWriter = new DocIdsWriter();
        final float[] correctiveValues = new float[3];
        final long quantizedVectorByteSize;

        MemorySegmentPostingsVisitor(
            float[] target,
            IndexInput indexInput,
            FieldEntry entry,
            FieldInfo fieldInfo,
            int maxPostingListSize,
            Bits acceptDocs
        ) throws IOException {
            this.target = target;
            this.indexInput = indexInput;
            this.entry = entry;
            this.fieldInfo = fieldInfo;
            this.acceptDocs = acceptDocs;
            centroid = new float[fieldInfo.getVectorDimension()];
            scratch = new float[target.length];
            quantizationScratch = new int[target.length];
            final int discretizedDimensions = discretize(fieldInfo.getVectorDimension(), 64);
            quantizedQueryScratch = new byte[QUERY_BITS * discretizedDimensions / 8];
            quantizedByteLength = discretizedDimensions / 8 + (Float.BYTES * 3) + Short.BYTES;
            quantizedVectorByteSize = (discretizedDimensions / 8);
            quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction(), DEFAULT_LAMBDA, 1);
            osqVectorsScorer = ESVectorUtil.getES91OSQVectorsScorer(indexInput, fieldInfo.getVectorDimension());
            this.docIdsScratch = new int[maxPostingListSize];
        }

        @Override
        public int resetPostingsScorer(long offset) throws IOException {
            quantized = false;
            indexInput.seek(offset);
            indexInput.readFloats(centroid, 0, centroid.length);
            centroidDp = Float.intBitsToFloat(indexInput.readInt());
            vectors = indexInput.readVInt();
            // read the doc ids
            assert vectors <= docIdsScratch.length;
            idsWriter.readInts(indexInput, vectors, docIdsScratch);
            // reconstitute from the deltas
            int sum = 0;
            for (int i = 0; i < vectors; i++) {
                sum += docIdsScratch[i];
                docIdsScratch[i] = sum;
            }
            slicePos = indexInput.getFilePointer();
            return vectors;
        }

        private float scoreIndividually(int offset) throws IOException {
            float maxScore = Float.NEGATIVE_INFINITY;
            // score individually, first the quantized byte chunk
            for (int j = 0; j < BULK_SIZE; j++) {
                int doc = docIdsScratch[j + offset];
                if (doc != -1) {
                    indexInput.seek(slicePos + (offset * quantizedByteLength) + (j * quantizedVectorByteSize));
                    float qcDist = osqVectorsScorer.quantizeScore(quantizedQueryScratch);
                    scores[j] = qcDist;
                }
            }
            // read in all corrections
            indexInput.seek(slicePos + (offset * quantizedByteLength) + (BULK_SIZE * quantizedVectorByteSize));
            indexInput.readFloats(correctionsLower, 0, BULK_SIZE);
            indexInput.readFloats(correctionsUpper, 0, BULK_SIZE);
            for (int j = 0; j < BULK_SIZE; j++) {
                correctionsSum[j] = Short.toUnsignedInt(indexInput.readShort());
            }
            indexInput.readFloats(correctionsAdd, 0, BULK_SIZE);
            // Now apply corrections
            for (int j = 0; j < BULK_SIZE; j++) {
                int doc = docIdsScratch[offset + j];
                if (doc != -1) {
                    scores[j] = osqVectorsScorer.score(
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        fieldInfo.getVectorSimilarityFunction(),
                        centroidDp,
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

        private static int docToBulkScore(int[] docIds, int offset, Bits acceptDocs) {
            assert acceptDocs != null : "acceptDocs must not be null";
            int docToScore = ES91OSQVectorsScorer.BULK_SIZE;
            for (int i = 0; i < ES91OSQVectorsScorer.BULK_SIZE; i++) {
                final int idx = offset + i;
                if (acceptDocs.get(docIds[idx]) == false) {
                    docIds[idx] = -1;
                    docToScore--;
                }
            }
            return docToScore;
        }

        private static void collectBulk(int[] docIds, int offset, KnnCollector knnCollector, float[] scores) {
            for (int i = 0; i < ES91OSQVectorsScorer.BULK_SIZE; i++) {
                final int doc = docIds[offset + i];
                if (doc != -1 && knnCollector.minCompetitiveSimilarity() < scores[i]) {
                    knnCollector.collect(doc, scores[i]);
                }
            }
        }

        private float[] getMinMaxIntervalsForPostingList() throws  IOException {
            float[] minMax = new float[2];
            minMax[0] = Float.POSITIVE_INFINITY;
            minMax[1] = Float.NEGATIVE_INFINITY;
            int limit = vectors - BULK_SIZE + 1;
            int i = 0;
            for (; i < limit; i += BULK_SIZE) {
                // read in all corrections
                indexInput.seek(slicePos + (i * quantizedByteLength) + (BULK_SIZE * quantizedVectorByteSize));
                indexInput.readFloats(correctionsLower, 0, BULK_SIZE);
                indexInput.readFloats(correctionsUpper, 0, BULK_SIZE);
                for (int j = 0; j < BULK_SIZE; j++) {
                    minMax[0] = Math.min(minMax[0], correctionsLower[j]);
                    minMax[1] = Math.max(minMax[1], correctionsUpper[j]);
                }
            }
            // process tail
            for (; i < vectors; i++) {
                indexInput.seek(slicePos + i * quantizedByteLength + quantizedVectorByteSize);
                indexInput.readFloats(correctiveValues, 0, 3);
                minMax[0] = Math.min(minMax[0], correctiveValues[0]);
                minMax[1] = Math.max(minMax[1], correctiveValues[1]);
            }
            return minMax;
        }

        @Override
        public int visit(KnnCollector knnCollector) throws IOException {
            // block processing
            int scoredDocs = 0;
            int limit = vectors - BULK_SIZE + 1;
            int i = 0;
            quantizeQueryIfNecessary();
            if (knnCollector.minCompetitiveSimilarity() != Float.NEGATIVE_INFINITY) {
                float intervalCheck = 0;
                // in blocks of 4 multiply all scratch where > 0 by maxUpperInterval and all scratch where < 0 by minLowerInterval
                float[] minMax = getMinMaxIntervalsForPostingList();
                float minLowerInterval = minMax[0];
                float maxUpperInterval = minMax[1];
                if (fieldInfo.getVectorSimilarityFunction() == EUCLIDEAN) {
                    for (int j = 0; j < quantizationScratch.length; j++) {
                        float diff = quantizationScratch[j] < 0
                            ? minLowerInterval - quantizationScratch[j]
                            : maxUpperInterval - quantizationScratch[j];
                        intervalCheck += diff * diff;
                    }
                } else {
                    for (int j = 0; j < quantizationScratch.length; j++) {
                        intervalCheck += quantizationScratch[j] < 0
                            ? minLowerInterval * quantizationScratch[j]
                            : maxUpperInterval * quantizationScratch[j];
                    }
                }
                float est = intervalCheck;
                if (fieldInfo.getVectorSimilarityFunction() == MAXIMUM_INNER_PRODUCT) {
                    est = VectorUtil.scaleMaxInnerProductScore(est);
                    logger.info(
                        "IVF vectors: minLowerInterval: {}, maxUpperInterval: {}, est: {}, competitive: {}",
                        minLowerInterval,
                        maxUpperInterval,
                        est,
                        knnCollector.minCompetitiveSimilarity()
                    );
                    if (knnCollector.minCompetitiveSimilarity() > est) {
                        return vectors; // skip this posting list, it is not competitive
                    }
                } else if (fieldInfo.getVectorSimilarityFunction() == DOT_PRODUCT || fieldInfo.getVectorSimilarityFunction() == COSINE) {
                    est = (est + 1f) / 2f;
                    logger.info(
                        "IVF vectors: minLowerInterval: {}, maxUpperInterval: {}, est: {}, competitive: {}",
                        minLowerInterval,
                        maxUpperInterval,
                        est,
                        knnCollector.minCompetitiveSimilarity()
                    );
                    if (knnCollector.minCompetitiveSimilarity() > est) {
                        return vectors; // skip this posting list, it is not competitive
                    }
                } else if (fieldInfo.getVectorSimilarityFunction() == EUCLIDEAN) {
                    // not handling this yet...
                    est = 1 / (1 + est);
                    logger.info(
                        "IVF vectors: minLowerInterval: {}, maxUpperInterval: {}, est: {}, competitive: {}",
                        minLowerInterval,
                        maxUpperInterval,
                        est,
                        knnCollector.minCompetitiveSimilarity()
                    );
                    if (knnCollector.minCompetitiveSimilarity() > est) {
                        return vectors; // skip this posting list, it is not competitive
                    }
                }
            }
            for (; i < limit; i += BULK_SIZE) {
                final int docsToBulkScore = acceptDocs == null ? BULK_SIZE : docToBulkScore(docIdsScratch, i, acceptDocs);
                if (docsToBulkScore == 0) {
                    continue;
                }
                indexInput.seek(slicePos + i * quantizedByteLength);
                final float maxScore;
                if (docsToBulkScore < BULK_SIZE / 2) {
                    maxScore = scoreIndividually(i);
                } else {
                    maxScore = osqVectorsScorer.scoreBulk(
                        quantizedQueryScratch,
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        fieldInfo.getVectorSimilarityFunction(),
                        centroidDp,
                        scores
                    );
                }
                if (knnCollector.minCompetitiveSimilarity() < maxScore) {
                    collectBulk(docIdsScratch, i, knnCollector, scores);
                }
                scoredDocs += docsToBulkScore;
            }
            // process tail
            for (; i < vectors; i++) {
                int doc = docIdsScratch[i];
                if (acceptDocs == null || acceptDocs.get(doc)) {
                    quantizeQueryIfNecessary();
                    indexInput.seek(slicePos + i * quantizedByteLength);
                    float qcDist = osqVectorsScorer.quantizeScore(quantizedQueryScratch);
                    indexInput.readFloats(correctiveValues, 0, 3);
                    final int quantizedComponentSum = Short.toUnsignedInt(indexInput.readShort());
                    float score = osqVectorsScorer.score(
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        fieldInfo.getVectorSimilarityFunction(),
                        centroidDp,
                        correctiveValues[0],
                        correctiveValues[1],
                        quantizedComponentSum,
                        correctiveValues[2],
                        qcDist
                    );
                    scoredDocs++;
                    if (score < knnCollector.minCompetitiveSimilarity()) {
                        continue; // skip this doc, it is not competitive
                    }
                    knnCollector.collect(doc, score);
                }
            }
            if (scoredDocs > 0) {
                knnCollector.incVisitedCount(scoredDocs);
            }
            return scoredDocs;
        }

        private void quantizeQueryIfNecessary() {
            if (quantized == false) {
                System.arraycopy(target, 0, scratch, 0, target.length);
                if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
                    VectorUtil.l2normalize(scratch);
                }
                queryCorrections = quantizer.scalarQuantize(scratch, quantizationScratch, (byte) 4, centroid);
                transposeHalfByte(quantizationScratch, quantizedQueryScratch);
                quantized = true;
            }
        }
    }

}
