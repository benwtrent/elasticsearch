/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.ltr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LeafReaderContextExtractor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LeafReaderContextExtractorFactory;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toUnmodifiableSet;

public class LtrRescorer implements Rescorer {
    private static final Logger logger = LogManager.getLogger(LtrRescorer.class);

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
        logger.info("Got some input fields from doc {}", inputFieldsFromDoc);

        // Save doc IDs for which rescoring was applied to be used in score explanation
        Set<Integer> topNDocIDs = Arrays.stream(topNFirstPass.scoreDocs).map(scoreDoc -> scoreDoc.doc).collect(toUnmodifiableSet());
        rescoreContext.setRescoredDocs(topNDocIDs);
        ScoreDoc[] hits = topNFirstPass.scoreDocs.clone();
        List<LeafReaderContextExtractor> extractors = new ArrayList<>();
        for (LeafReaderContextExtractorFactory factory : definition.processors()
            .stream()
            .filter(p -> p instanceof LeafReaderContextExtractorFactory)
            .map(p -> (LeafReaderContextExtractorFactory) p)
            .toList()) {
            extractors.add(factory.createLeafReaderExtractor(ltrRescoreContext.executionContext));
        }
        Arrays.sort(hits, Comparator.comparingInt(a -> a.doc));
        SourceLookup sourceLookup = ltrRescoreContext.executionContext.lookup().source();
        int hitUpto = 0;
        int readerUpto = -1;
        int endDoc = 0;
        int docBase = 0;
        List<LeafReaderContext> leaves = ltrRescoreContext.executionContext.searcher().getIndexReader().leaves();
        LeafReaderContext currentSegment = null;
        boolean changedSegment = true;
        List<Map<String, Object>> docFeatures = new ArrayList<>(topNDocIDs.size());
        int featureSize = inputFieldsFromDoc.size() + extractors.stream()
            .mapToInt(LeafReaderContextExtractor::expectedFieldOutputSize)
            .sum();
        while (hitUpto < hits.length) {
            final ScoreDoc hit = hits[hitUpto];
            final int docID = hit.doc;
            while (docID >= endDoc) {
                readerUpto++;
                currentSegment = leaves.get(readerUpto);
                endDoc = currentSegment.docBase + currentSegment.reader().maxDoc();
                changedSegment = true;
            }
            assert currentSegment != null : "Unexpected null segment";
            if (changedSegment) {
                // Adjust scorers
                // We advanced to another segment:
                docBase = currentSegment.docBase;
                // TODO adjust scorer
                // scorer = weight.scorer(readerContext);
                for (var extractor : extractors) {
                    extractor.resetContext(currentSegment);
                }
                changedSegment = false;
            }
            int targetDoc = docID - docBase;
            Map<String, Object> features = Maps.newMapWithExpectedSize(featureSize);
            sourceLookup.setSegmentAndDocument(currentSegment, targetDoc);
            if (inputFieldsFromDoc.isEmpty() == false) {
                if (inputFieldsFromDoc.size() == 1) {
                    List<Object> extractedValues = sourceLookup.extractRawValuesWithoutCaching(inputFieldsFromDoc.get(0));
                    if (extractedValues.size() > 0) {
                        features.put(inputFieldsFromDoc.get(0), extractedValues.get(0));
                    }
                } else {
                    for (String inputField : inputFieldsFromDoc) {
                        features.put(inputField, sourceLookup.extractValue(inputField, null));
                    }
                }
            }
            for (var extractor : extractors) {
                extractor.extract(targetDoc);
            }
            docFeatures.add(features);
            hitUpto++;
        }
        for (int i = 0; i < hits.length; i++) {
            Map<String, Object> features = docFeatures.get(i);
            for (var extractor : extractors) {
                extractor.populate(hits[i].doc, features);
            }
            logger.info("EXTRACTED FEATURES!!! {}", features);
            hits[i].score = ((Number) definition.infer(features, RegressionConfig.EMPTY_PARAMS).predictedValue()).floatValue();
        }
        if (rescoreContext.getWindowSize() < hits.length) {
            ArrayUtil.select(hits, 0, hits.length, rescoreContext.getWindowSize(), SCORE_DOC_COMPARATOR);
            ScoreDoc[] subset = new ScoreDoc[rescoreContext.getWindowSize()];
            System.arraycopy(hits, 0, subset, 0, rescoreContext.getWindowSize());
            hits = subset;
        }

        Arrays.sort(hits, SCORE_DOC_COMPARATOR);

        // Rescore them:
        TopDocs rescored = new TopDocs(topDocs.totalHits, hits);
        // Splice back to non-topN hits and resort all of them:
        return combine(topDocs, rescored, (LtrRescoreContext) rescoreContext);
    }

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
