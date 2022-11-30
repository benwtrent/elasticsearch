/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.ltr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LeafReaderContextExtractor;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TermStatsLeafValueExtractor implements LeafReaderContextExtractor {
    private static final Logger logger = LogManager.getLogger(TermStatsLeafValueExtractor.class);
    private final ClassicSimilarity sim;
    private final Map<String, CollectionStatisticsBuilder> fieldStats;
    private final Map<Term, TermStates> termStates;
    private final Map<Integer, DocStats> docStats;
    private final List<String> statistics;
    private final Map<String, Map<TermStats.TermAndName, Set<Term>>> fieldToTerms;
    private final int totalOutputFields;
    private LeafReaderContext currentContext;

    public TermStatsLeafValueExtractor(
        Map<String, Map<TermStats.TermAndName, Set<Term>>> fieldToTerms,
        int totalOutputFields,
        List<String> statistics,
        IndexSearcher searcher
    ) {
        this.totalOutputFields = totalOutputFields;
        this.fieldStats = Maps.newMapWithExpectedSize(fieldToTerms.size());
        for (String fieldName : fieldToTerms.keySet()) {
            fieldStats.put(fieldName, new CollectionStatisticsBuilder(searcher.getIndexReader().maxDoc()));
        }
        this.termStates = Maps.newMapWithExpectedSize(fieldToTerms.values().stream().mapToInt(Map::size).sum());
        fieldToTerms.values()
            .forEach(
                stringSetMap -> stringSetMap.values()
                    .forEach(v -> v.forEach(term -> termStates.computeIfAbsent(term, t -> new TermStates(searcher.getTopReaderContext()))))
            );
        this.statistics = statistics;
        this.fieldToTerms = fieldToTerms;
        this.sim = new ClassicSimilarity();
        this.docStats = Maps.newMapWithExpectedSize(50);
    }

    @Override
    public void resetContext(LeafReaderContext context) throws IOException {
        Map<String, Terms> seenFields = Maps.newMapWithExpectedSize(fieldStats.size());
        for (var fieldAndStats : fieldStats.entrySet()) {
            Terms terms = Terms.getTerms(context.reader(), fieldAndStats.getKey());
            seenFields.put(fieldAndStats.getKey(), terms);
            fieldAndStats.getValue().updateStatistics(terms);
        }
        logger.info("updated field stats [{}]", fieldStats);
        for (var termAndStates : termStates.entrySet()) {
            Terms terms = seenFields.get(termAndStates.getKey().field());
            TermsEnum termsEnum = terms.iterator();
            if (termsEnum.seekExact(termAndStates.getKey().bytes())) {
                final TermState termState = termsEnum.termState();
                logger.info("Got term state [{}] {} [{}]", termAndStates.getKey().text(), termState, context.ord);
                termAndStates.getValue().register(termState, context.ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
            } else {
                logger.info("Term not found in field [{}]", termAndStates.getKey().text());
            }
        }
        currentContext = context;
    }

    @Override
    public void extract(int docId) throws IOException {
        PostingsEnum postingsEnum = null;

        int matchedTermCount = 0;
        DocStats individualDocStats = new DocStats(termStates.size());
        for (var termAndStats : termStates.entrySet()) {
            logger.info("looking for term state [{}] [{}]", termAndStats.getKey().text(), currentContext.ord);
            TermState state = termAndStats.getValue().get(currentContext);
            if (state != null) {
                logger.info("Found previously stored term state [{}] {} [{}]", termAndStats.getKey().text(), state, currentContext.ord);
                Term term = termAndStats.getKey();
                TermsEnum termsEnum = currentContext.reader().terms(termAndStats.getKey().field()).iterator();
                termsEnum.seekExact(term.bytes(), state);
                postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.ALL);
                // Verify document is in postings
                if (postingsEnum.advance(docId) == docId) {
                    matchedTermCount++;
                    individualDocStats.termFreq.computeIfAbsent(term, t -> new StatisticsCollector()).addValue(postingsEnum.freq());
                    if (postingsEnum.freq() > 0) {
                        StatisticsCollector collector = individualDocStats.pos.computeIfAbsent(term, t -> new StatisticsCollector());
                        for (int i = 0; i < postingsEnum.freq(); i++) {
                            collector.addValue(postingsEnum.nextPosition() + 1);
                        }
                    }
                }
            }
        }
        individualDocStats.matchedTermCount = matchedTermCount;
        docStats.put(docId + currentContext.docBase, individualDocStats);
    }

    @Override
    public void populate(int docId, Map<String, Object> output) {
        logger.info("populating!!! {}", output);
        fieldToTerms.forEach((field, termToTerms) -> {
            long fieldDocCount = fieldStats.get(field).docCount;
            termToTerms.forEach((originalTerm, term) -> {
                final DocStats individualDocStats = docStats.get(docId);
                final StatisticsCollector termFreq = individualDocStats.mergeForTermsFreq(term);
                final StatisticsCollector pos = individualDocStats.mergeForTermsPos(term);
                final long df = term.stream().mapToInt(t -> termStates.get(t).docFreq()).sum();
                statistics.forEach(s -> {
                    final String outputName = field + "_" + originalTerm.name() + "_" + s;
                    Number statValue = switch (s) {
                        case "df" -> df;
                        case "ttf" -> term.stream().mapToLong(t -> termStates.get(t).totalTermFreq()).sum();
                        case "matches" -> individualDocStats.matchedTermCount;
                        case "pos_avg" -> pos != null && pos.count > 0 ? pos.mean() : null;
                        case "pos_min" -> pos != null && pos.count > 0 ? pos.min : null;
                        case "pos_max" -> pos != null && pos.count > 0 ? pos.max : null;
                        case "pos_std" -> pos != null && pos.count > 0 ? pos.std() : null;
                        case "tf_avg" -> termFreq != null && termFreq.count > 0 ? termFreq.mean() : null;
                        case "tf_min" -> termFreq != null && termFreq.count > 0 ? termFreq.min : null;
                        case "tf_max" -> termFreq != null && termFreq.count > 0 ? termFreq.max : null;
                        case "tf_std" -> termFreq != null && termFreq.count > 0 ? termFreq.std() : null;
                        case "idf" -> sim.idf(df, fieldDocCount);
                        default -> throw new IllegalArgumentException("WOW");
                    };
                    if (statValue != null) {
                        logger.info("adding field!!!!!! {} {}", outputName, statValue);
                        output.put(outputName, statValue);
                    }
                });
            });
        });
    }

    @Override
    public int expectedFieldOutputSize() {
        return totalOutputFields;
    }

    static class DocStats {
        Map<Term, StatisticsCollector> termFreq;
        Map<Term, StatisticsCollector> pos;
        int matchedTermCount;

        DocStats(int numTerms) {
            termFreq = Maps.newMapWithExpectedSize(numTerms);
            pos = Maps.newMapWithExpectedSize(numTerms);
        }

        StatisticsCollector mergeForTermsFreq(Collection<Term> terms) {
            if (terms.size() == 1) {
                return termFreq.get(terms.iterator().next());
            }
            StatisticsCollector merge = new StatisticsCollector();
            for (Term t : terms) {
                merge.addAll(termFreq.get(t));
            }
            return merge;
        }

        StatisticsCollector mergeForTermsPos(Collection<Term> terms) {
            if (terms.size() == 1) {
                return pos.get(terms.iterator().next());
            }
            StatisticsCollector merge = new StatisticsCollector();
            for (Term t : terms) {
                merge.addAll(termFreq.get(t));
            }
            return merge;
        }
    }

    static class StatisticsCollector {
        private int count;
        private double sum;
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sumOfSqrs;

        void addAll(StatisticsCollector statisticsCollector) {
            if (statisticsCollector == null) {
                return;
            }
            this.count += statisticsCollector.count;
            min = Math.min(min, statisticsCollector.min);
            max = Math.max(max, statisticsCollector.max);
            sum += statisticsCollector.sum;
            sumOfSqrs += statisticsCollector.sumOfSqrs;
        }

        void addValue(float v) {
            count += 1;
            min = Math.min(min, v);
            max = Math.max(max, v);
            sum += v;
            sumOfSqrs += v * v;
        }

        double variance() {
            return Math.max((sumOfSqrs - ((sum * sum) / count)) / count, 0.0);
        }

        double mean() {
            return sum / count;
        }

        double std() {
            return Math.sqrt(variance());
        }

        @Override
        public String toString() {
            return "StatisticsCollector{"
                + "count="
                + count
                + ", sum="
                + sum
                + ", min="
                + min
                + ", max="
                + max
                + ", sumOfSqrs="
                + sumOfSqrs
                + '}';
        }
    }

    static class CollectionStatisticsBuilder {
        private final long maxDoc;
        private long docCount;
        private long sumTotalTermFreq;
        private long sumDocFreq;

        CollectionStatisticsBuilder(long maxDoc) {
            this.maxDoc = maxDoc;
        }

        void updateStatistics(Terms terms) throws IOException {
            docCount += terms.getDocCount();
            sumTotalTermFreq += terms.getSumTotalTermFreq();
            sumDocFreq += terms.getSumDocFreq();
        }

        CollectionStatistics build(String field) {
            return new CollectionStatistics(field, maxDoc, docCount, sumTotalTermFreq, sumDocFreq);
        }

        @Override
        public String toString() {
            return "CollectionStatisticsBuilder{"
                + "maxDoc="
                + maxDoc
                + ", docCount="
                + docCount
                + ", sumTotalTermFreq="
                + sumTotalTermFreq
                + ", sumDocFreq="
                + sumDocFreq
                + '}';
        }
    }
}
