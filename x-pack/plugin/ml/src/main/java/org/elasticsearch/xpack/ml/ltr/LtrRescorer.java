/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.ltr;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toUnmodifiableSet;

public class LtrRescorer implements Rescorer {

    private static final Comparator<ScoreDoc> SCORE_DOC_COMPARATOR = (o1, o2) -> {
        int cmp = Float.compare(o2.score, o1.score);
        return cmp == 0 ? Integer.compare(o1.doc, o2.doc) : cmp;
    };

    @Override
    public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {
        LtrRescoreContext ltrRescoreContext = (LtrRescoreContext) rescoreContext;

        InferenceDefinition definition = ltrRescoreContext.inferenceDefinition;

        // First take top slice of incoming docs, to be rescored:
        TopDocs topNFirstPass = topN(topDocs, rescoreContext.getWindowSize());
        List<String> inputFieldsFromDoc = new ArrayList<>();
        for (String field : definition.getTrainedModel().getFeatureNames()) {
            if (ltrRescoreContext.executionContext.isFieldMapped(field) || ltrRescoreContext.executionContext.fieldExistsInIndex(field)) {
                inputFieldsFromDoc.add(field);
            }
        }

        // Save doc IDs for which rescoring was applied to be used in score explanation
        Set<Integer> topNDocIDs = Arrays.stream(topNFirstPass.scoreDocs).map(scoreDoc -> scoreDoc.doc).collect(toUnmodifiableSet());
        rescoreContext.setRescoredDocs(topNDocIDs);
        ScoreDoc[] hits = topNFirstPass.scoreDocs.clone();
        int[] docs = Arrays.stream(topNFirstPass.scoreDocs).mapToInt(sd -> sd.doc).toArray();
        Arrays.sort(hits, Comparator.comparingInt(a -> a.doc));
        SourceLoader sourceLoader = ltrRescoreContext.executionContext.newSourceLoader(false);
        ltrRescoreContext.executionContext.searcher().getIndexReader().leaves().forEach(lc -> {
            // sourceLoader.leaf(lc.reader(), docs).source()
        });

        for (ScoreDoc hit : hits) {
            int docId = hit.doc;
            // ltrRescoreContext.executionContext.
        }
        // Rescore them:
        TopDocs rescored = new TopDocs(topDocs.totalHits, hits);
        // Splice back to non-topN hits and resort all of them:
        return combine(topDocs, rescored, (LtrRescoreContext) rescoreContext);
    }

    /*    private void collectTermStats(IndexSearcher searcher) {
        for (Term t : terms) {
            TermStates ctx = TermStates.build(searcher.getTopReaderContext(), t, true);

            if (ctx != null && ctx.docFreq() > 0) {
                searcher.collectionStatistics(t.field());
                searcher.termStatistics(t, ctx.docFreq(), ctx.totalTermFreq());
            }

            termContexts.put(t, ctx);
        }
    }*/

    @Override
    public Explanation explain(int topLevelDocId, IndexSearcher searcher, RescoreContext rescoreContext, Explanation sourceExplanation)
        throws IOException {
        return null;
    }

    /** Returns a new {@link TopDocs} with the topN from the incoming one, or the same TopDocs if the number of hits is already &lt;=
     *  topN. */
    private static TopDocs topN(TopDocs in, int topN) {
        if (in.scoreDocs.length < topN) {
            return in;
        }

        ScoreDoc[] subset = new ScoreDoc[topN];
        System.arraycopy(in.scoreDocs, 0, subset, 0, topN);

        return new TopDocs(in.totalHits, subset);
    }

    private static TopDocs combine(TopDocs in, TopDocs resorted, LtrRescoreContext ctx) {
        System.arraycopy(resorted.scoreDocs, 0, in.scoreDocs, 0, resorted.scoreDocs.length);
        if (in.scoreDocs.length > resorted.scoreDocs.length) {
            // for (int i = resorted.scoreDocs.length; i < in.scoreDocs.length; i++) {
            // TODO: shouldn't this be up to the ScoreMode? I.e., we should just invoke ScoreMode.combine, passing 0.0f for the
            // secondary score?
            // in.scoreDocs[i].score *= ctx.queryWeight();
            // }

            // TODO: this is wrong, i.e. we are comparing apples and oranges at this point. It would be better if we always rescored all
            // incoming first pass hits, instead of allowing recoring of just the top subset:
            Arrays.sort(in.scoreDocs, SCORE_DOC_COMPARATOR);
        }
        return in;
    }
}
