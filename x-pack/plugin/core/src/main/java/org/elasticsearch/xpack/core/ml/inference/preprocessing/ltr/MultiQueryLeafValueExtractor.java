/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.ltr;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LeafReaderContextExtractor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MultiQueryLeafValueExtractor implements LeafReaderContextExtractor {

    private final List<String> outputFieldName;
    private final List<Weight> weights;
    private DisjunctionDISI rankerIterator;
    private final Map<Integer, float[]> docScores;
    private final Map<String, Scorer> scorers;
    private int baseDoc;

    public MultiQueryLeafValueExtractor(List<String> names, List<Weight> queries) {
        this.weights = queries;
        this.outputFieldName = names;
        this.docScores = Maps.newMapWithExpectedSize(50);
        this.scorers = Maps.newMapWithExpectedSize(queries.size());
    }

    @Override
    public void resetContext(LeafReaderContext context) throws IOException {
        DisiPriorityQueue disiPriorityQueue = new DisiPriorityQueue(weights.size());
        for (int i = 0; i < weights.size(); i++) {
            Weight weight = weights.get(i);
            String name = outputFieldName.get(i);
            Scorer scorer = weight.scorer(context);
            // Could we just skip all this if the scorer is null???
            if (scorer == null) {
                scorer = new NoopScorer(weight, DocIdSetIterator.empty());
            }
            scorers.put(name, scorer);
            disiPriorityQueue.add(new DisiWrapper(scorer));
        }
        rankerIterator = new DisjunctionDISI(DocIdSetIterator.all(context.reader().maxDoc()), disiPriorityQueue);
        baseDoc = context.docBase;
    }

    @Override
    public void extract(int docId) throws IOException {
        rankerIterator.advance(docId);
        float[] scores = new float[outputFieldName.size()];
        Arrays.fill(scores, Float.NaN);
        int scoreIndex = 0;
        for (String name : outputFieldName) {
            Scorer scorer = scorers.get(name);
            if (scorer != null && scorer.docID() == docId) {
                scores[scoreIndex] = scorer.score();
            }
            scoreIndex++;
        }
        docScores.put(docId + baseDoc, scores);
    }

    @Override
    public void populate(int docId, Map<String, Object> output) {
        float[] docScore = docScores.get(docId);
        int scoreIndex = 0;
        for (String name : outputFieldName) {
            float score = docScore[scoreIndex++];
            if (Float.isNaN(score) == false) {
                output.put(name, score);
            }
        }
    }

    @Override
    public int expectedFieldOutputSize() {
        return weights.size();
    }

    static class DisjunctionDISI extends DocIdSetIterator {
        private final DocIdSetIterator main;
        private final DisiPriorityQueue subIteratorsPriorityQueue;

        DisjunctionDISI(DocIdSetIterator main, DisiPriorityQueue subIteratorsPriorityQueue) {
            this.main = main;
            this.subIteratorsPriorityQueue = subIteratorsPriorityQueue;
        }

        @Override
        public int docID() {
            return main.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            int doc = main.nextDoc();
            advanceSubIterators(doc);
            return doc;
        }

        @Override
        public int advance(int target) throws IOException {
            int docId = main.advance(target);
            advanceSubIterators(docId);
            return docId;
        }

        private void advanceSubIterators(int target) throws IOException {
            if (target == NO_MORE_DOCS) {
                return;
            }
            DisiWrapper top = subIteratorsPriorityQueue.top();
            while (top.doc < target) {
                top.doc = top.iterator.advance(target);
                top = subIteratorsPriorityQueue.updateTop();
            }
        }

        @Override
        public long cost() {
            return main.cost();
        }
    }

}
