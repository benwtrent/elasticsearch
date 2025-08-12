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
            float score;

            @Override
            public boolean hasNext() {
                return neighborQueue.size() > 0;
            }

            @Override
            public long nextPostingListOffset() throws IOException {
                score = neighborQueue.topScore();
                int centroidOrdinal = neighborQueue.pop();
                centroids.seek(offset + (long) Long.BYTES * centroidOrdinal);
                return centroids.readLong();
            }

            @Override
            public float score() {
                return score;
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
            float score;

            @Override
            public boolean hasNext() {
                return neighborQueue.size() > 0;
            }

            @Override
            public long nextPostingListOffset() throws IOException {
                score = neighborQueue.topScore();
                int centroidOrdinal = neighborQueue.pop();
                updateQueue(); // add one children if available so the queue remains fully populated
                centroids.seek(childrenFileOffsets + (long) Long.BYTES * centroidOrdinal);
                return centroids.readLong();
            }

            @Override
            public float score() {
                return score;
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
        float centroidScore;
        OptimizedScalarQuantizer.QuantizationResult queryCorrections;
        DocIdsWriter docIdsWriter = new DocIdsWriter();

        final float[] scratch;
        final int[] quantizationScratch;
        final byte[] quantizedQueryScratch;
        final OptimizedScalarQuantizer quantizer;
        final float[] correctiveValues = new float[3];
        final long quantizedVectorByteSize;
        final long quantizedBlockSize;

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
            // BULK_SIZE vectors and a block estimator
            quantizedBlockSize = (BULK_SIZE) * quantizedByteLength + Float.BYTES * 2;
            quantizedVectorByteSize = (discretizedDimensions / 8);
            quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction(), DEFAULT_LAMBDA, 1);
            osqVectorsScorer = ESVectorUtil.getES91OSQVectorsScorer(indexInput, fieldInfo.getVectorDimension());
            this.docIdsScratch = new int[maxPostingListSize];
        }

        @Override
        public int resetPostingsScorer(long offset, float centroidScore) throws IOException {
            quantized = false;
            indexInput.seek(offset);
            indexInput.readFloats(centroid, 0, centroid.length);
            centroidDp = Float.intBitsToFloat(indexInput.readInt());
            vectors = indexInput.readVInt();
            // read the doc ids
            assert vectors <= docIdsScratch.length;
            docIdsWriter.readInts(indexInput, vectors, docIdsScratch);
            slicePos = indexInput.getFilePointer();
            this.centroidScore = centroidScore;
            return vectors;
        }

        float scoreIndividually(int block) throws IOException {
            float maxScore = Float.NEGATIVE_INFINITY;
            int absOffset = block * BULK_SIZE;
            // get to the slice position for the given block, past the estimator
            long offsetSlice = slicePos + (block * quantizedBlockSize) + quantizedByteLength;
            // score individually, first the quantized byte chunk
            for (int j = 0; j < BULK_SIZE; j++) {
                int doc = docIdsScratch[j + absOffset];
                if (doc != -1) {
                    indexInput.seek(offsetSlice + (j * quantizedVectorByteSize));
                    float qcDist = osqVectorsScorer.quantizeScore(quantizedQueryScratch);
                    scores[j] = qcDist;
                }
            }
            // read in all corrections
            indexInput.seek(offsetSlice + (BULK_SIZE * quantizedVectorByteSize));
            indexInput.readFloats(correctionsLower, 0, BULK_SIZE);
            indexInput.readFloats(correctionsUpper, 0, BULK_SIZE);
            for (int j = 0; j < BULK_SIZE; j++) {
                correctionsSum[j] = Short.toUnsignedInt(indexInput.readShort());
            }
            indexInput.readFloats(correctionsAdd, 0, BULK_SIZE);
            // Now apply corrections
            for (int j = 0; j < BULK_SIZE; j++) {
                int doc = docIdsScratch[absOffset + j];
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

        private static void collectBulk(int[] docIds, int blk, KnnCollector knnCollector, float[] scores) {
            int offset = blk * ES91OSQVectorsScorer.BULK_SIZE;
            for (int i = 0; i < ES91OSQVectorsScorer.BULK_SIZE; i++) {
                final int doc = docIds[offset + i];
                if (doc != -1) {
                    knnCollector.collect(doc, scores[i]);
                }
            }
        }

        private float estimateBlockScore(int blk, int stdDeviations) throws IOException {
            // let's score against the block estimator and see if we can skip it.
            indexInput.seek(this.slicePos + (blk * quantizedBlockSize));
            float mean = Float.intBitsToFloat(indexInput.readInt());
            float stdDev = Float.intBitsToFloat(indexInput.readInt());
            return mean + (stdDeviations * stdDev);
            //long qcDist = osqVectorsScorer.quantizeScore(quantizedQueryScratch);
            //indexInput.readFloats(correctiveValues, 0, 3);
            //final int quantizedComponentSum = Short.toUnsignedInt(indexInput.readShort());
            //return osqVectorsScorer.score(
            //    queryCorrections.lowerInterval(),
            //    queryCorrections.upperInterval(),
            //    queryCorrections.quantizedComponentSum(),
            //    queryCorrections.additionalCorrection(),
            //    fieldInfo.getVectorSimilarityFunction(),
            //    centroidDp,
            //    correctiveValues[0],
            //    correctiveValues[1],
            //    quantizedComponentSum,
            //    correctiveValues[2],
            //    qcDist
            //);
        }

        @Override
        public int visit(KnnCollector knnCollector) throws IOException {
            // block processing
            int scoredDocs = 0;
            int numBlks = vectors / BULK_SIZE;
            int blk = 0;
            for (; blk < numBlks; blk++) {
                final int docsToBulkScore = acceptDocs == null ? BULK_SIZE : docToBulkScore(docIdsScratch, blk * BULK_SIZE, acceptDocs);
                if (docsToBulkScore == 0) {
                    continue;
                }
                quantizeQueryIfNecessary();
                // TODO estimate blk score....
                float blockEstimate = Float.NEGATIVE_INFINITY;
                float minCompetitiveSimilarity = knnCollector.minCompetitiveSimilarity();
                // we are hitting further centroids than we have gathered, start checking block estimates for skipping
                if (minCompetitiveSimilarity > centroidScore) {
                    // let's score against the block estimator and see if we can skip it.
                    blockEstimate = estimateBlockScore(blk, 0);
                    if (minCompetitiveSimilarity > (blockEstimate + centroidScore)/2) {
                        continue; // skip this block
                    }
                }
                indexInput.seek(slicePos + (blk * quantizedBlockSize) + Float.BYTES * 2);
                float maxScore;
                if (docsToBulkScore < BULK_SIZE / 2) {
                    maxScore = scoreIndividually(blk);
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
                if (minCompetitiveSimilarity < maxScore) {
                    collectBulk(docIdsScratch, blk, knnCollector, scores);
                }
                scoredDocs += docsToBulkScore;
            }
            // get to the tail
            long slicePos = this.slicePos + (numBlks * quantizedBlockSize);
            for (int d = numBlks * BULK_SIZE, posIdx = 0; d < vectors; d++, posIdx++) {
                int doc = docIdsScratch[d];
                if (acceptDocs == null || acceptDocs.get(doc)) {
                    quantizeQueryIfNecessary();
                    indexInput.seek(slicePos + posIdx * quantizedByteLength);
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
                    if (knnCollector.minCompetitiveSimilarity() < score) {
                        knnCollector.collect(doc, score);
                    }
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
