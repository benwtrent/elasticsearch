/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.analysis.interpolation.SplineInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.commons.math3.analysis.solvers.BrentSolver;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.StartInvestigationAction;
import org.elasticsearch.xpack.core.ml.action.StartInvestigationAction.Request;
import org.elasticsearch.xpack.core.ml.action.StartInvestigationAction.Response;
import org.elasticsearch.xpack.core.ml.investigation.InvestigationConfig;
import org.elasticsearch.xpack.core.ml.investigation.InvestigationSource;
import org.elasticsearch.xpack.core.security.SecurityContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;


public class TransportStartInvestigationAction extends HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportStartInvestigationAction.class);


    private final ThreadPool threadPool;
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final SecurityContext securityContext;

    @Inject
    public TransportStartInvestigationAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                             ActionFilters actionFilters, Client client,
                                             NamedXContentRegistry xContentRegistry) {
        super(StartInvestigationAction.NAME, transportService, actionFilters, Request::new);
        this.threadPool = threadPool;
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings) ?
            new SecurityContext(settings, threadPool.getThreadContext()) : null;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        threadPool.generic().execute(() -> investigate(request, listener));
    }

    void investigate(Request request, ActionListener<Response> listener) {
        CorrelatorFactory correlatorFactory = new CorrelatorFactory(client, request.getConfig());
        correlatorFactory.execute(listener);
    }

    static class CorrelatorFactory {
        static final double[] PERCENTILE_STEPS = IntStream.range(2, 99)
            .filter(i -> i % 2 == 0)
            .mapToDouble(Double::valueOf)
            .toArray();

        private final InvestigationConfig investigationConfig;
        private final OriginSettingClient client;

        CorrelatorFactory(Client client, InvestigationConfig config) {
            this.investigationConfig = config;
            this.client = new OriginSettingClient(client, ML_ORIGIN);
        }

        void execute(ActionListener<Response> responseActionListener) {
            client.prepareSearch(investigationConfig.getSourceConfig().getIndex())
                .setSource(SearchSourceBuilder.searchSource()
                    .aggregation(
                        AggregationBuilders.percentiles("correlation_percentiles")
                            .field(investigationConfig.getKeyIndicator())
                            .percentiles(PERCENTILE_STEPS)
                    )
                    .size(0)
                    .trackTotalHits(true)
                    .query(QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery("browser")).filter(investigationConfig.getSourceConfig().getQuery()))
                )
                .execute(ActionListener.wrap(
                    searchResponse -> {
                        Percentiles percentiles = searchResponse.getAggregations().get("correlation_percentiles");
                        var val = TermCorrelator.buildRangeAggAndSetExpectations(percentiles,
                            PERCENTILE_STEPS,
                            investigationConfig.getKeyIndicator()
                        );
                        final long totalHits = searchResponse.getHits().getTotalHits().value;

                        GroupedActionListener<Map<String, Object>> groupedActionListener = new GroupedActionListener<>(
                            ActionListener.wrap(
                                results -> responseActionListener.onResponse(new Response(results)),
                                responseActionListener::onFailure
                            ),
                            investigationConfig.getTerms().size()
                        );

                        for (String term : investigationConfig.getTerms()) {
                            TermCorrelator termCorrelator = new TermCorrelator(term,
                                val,
                                totalHits,
                                client,
                                investigationConfig.getSourceConfig()
                            );
                            termCorrelator.correlation(ActionListener.wrap(
                                correlationResults -> {
                                    correlationResults.sort(Comparator.comparing(CorrelationResult::getCorrelation));
                                    groupedActionListener.onResponse(Collections.singletonMap(term, correlationResults));
                                },
                                groupedActionListener::onFailure
                            ));
                        }
                    },
                    responseActionListener::onFailure
                ));
        }
    }

    static class CorrelationResult implements ToXContentObject {
        private final String value;
        private final double correlation;
        private final double meanInfluence;
        private final double percentileInfluence;

        CorrelationResult(String value, double correlation, double meanInfluence, double percentileInfluence) {
            this.value = value;
            this.correlation = correlation;
            this.meanInfluence = meanInfluence;
            this.percentileInfluence = percentileInfluence;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field("value", value)
                .field("correlation", correlation)
                .field("mean_influence", meanInfluence)
                .field("75_percentile_influence", percentileInfluence)
                .endObject();
        }

        public String getValue() {
            return value;
        }

        public double getCorrelation() {
            return correlation;
        }
    }

    static class Statistics {
        final AggregationBuilder rangeAgg;
        final double[] expectations;
        final double mean;
        final PolynomialSplineFunction univariateSpline;

        Statistics(AggregationBuilder rangeAgg, double[] expectations, PolynomialSplineFunction univariateSpline, double mean) {
            this.rangeAgg = rangeAgg;
            this.expectations = expectations;
            this.univariateSpline = univariateSpline;
            this.mean = mean;
        }
    }

    static class TermCorrelator {

        static Statistics buildRangeAggAndSetExpectations(Percentiles raw_percentiles,
                                                          double[] steps,
                                                          String indicatorFieldName) {
            List<Double> percentiles = new ArrayList<>();
            List<Double> fractions = new ArrayList<>();
            RangeAggregationBuilder builder = AggregationBuilders.range("correlation_range").field(indicatorFieldName);
            double percentile_0 = raw_percentiles.percentile(steps[0]);
            builder.addUnboundedTo(percentile_0);
            fractions.add(0.02);
            percentiles.add(percentile_0);
            int last_added = 0;
            for (int i = 1; i < steps.length; i++) {
                double percentile_l = raw_percentiles.percentile(steps[i - 1]);
                double percentile_r = raw_percentiles.percentile(steps[i]);
                if (Double.compare(percentile_l, percentile_r) == 0) {
                    fractions.set(last_added, fractions.get(last_added) + 0.02);
                } else {
                    last_added = i;
                    fractions.add(0.02);
                    percentiles.add(percentile_r);
                }
            }
            fractions.add(2.0/100);
            double[] cumSumFactions = new double[fractions.size()];
            double[] expectations = new double[percentiles.size() + 1];
            expectations[0] = percentile_0;
            cumSumFactions[0] = fractions.get(0);
            double mean = percentile_0 * fractions.get(0);
            for (int i = 1; i < percentiles.size(); i++) {
                double percentile_l = percentiles.get(i - 1);
                double percentile_r = percentiles.get(i);
                double fractions_l = fractions.get(i - 1);
                double fractions_r = fractions.get(i);
                cumSumFactions[i] = cumSumFactions[i - 1] + fractions_r;
                builder.addRange(percentile_l, percentile_r);
                expectations[i] = (fractions_l * percentile_l + fractions_r * percentile_r) / (fractions_l + fractions_r);
                mean += expectations[i] * fractions_r;
            }
            cumSumFactions[fractions.size() - 1] = cumSumFactions[fractions.size() - 2] + fractions.get(fractions.size() - 1);
            double percentile_n = percentiles.get(percentiles.size() - 1);
            builder.addUnboundedFrom(percentile_n);
            expectations[percentiles.size()] = percentile_n;
            mean += expectations[percentiles.size()] * fractions.get(percentiles.size());
            logger.info("Stats objects [{}] [{}] [{}]", expectations, mean, cumSumFactions);
            return new Statistics(builder, expectations, new SplineInterpolator().interpolate(expectations, cumSumFactions), mean);
        }

        private List<CorrelationResult> results = new ArrayList<>();
        private double[] expectations;
        private final String termField;
        private final PolynomialSplineFunction function;
        private final double mean;
        private final long totalHits;
        private final CompositeAggregationBuilder aggregationBuilder;
        private final Client client;
        private final InvestigationSource source;
        TermCorrelator(String termField,
                       Statistics statistics,
                       long totalHits,
                       OriginSettingClient client,
                       InvestigationSource source) {
            this.termField = termField;
            this.expectations = statistics.expectations;
            this.aggregationBuilder = AggregationBuilders.composite(
                "composite_" + termField,
                List.of(new TermsValuesSourceBuilder(termField).field(termField)))
                .size(100)
                .subAggregation(statistics.rangeAgg);
            this.client = client;
            this.source = source;
            this.function = statistics.univariateSpline;
            this.mean = statistics.mean;
            this.totalHits = totalHits;
        }

        void correlation(ActionListener<List<CorrelationResult>> listener) {
            client.prepareSearch(source.getIndex())
                .setSize(0)
                .setQuery(source.getQuery())
                .addAggregation(aggregationBuilder)
                .execute(ActionListener.wrap(
                    response -> {
                        if (response.getAggregations() == null) {
                            listener.onResponse(results);
                            return;
                        }
                        CompositeAggregation aggregation = response.getAggregations().get("composite_" + termField);
                        if (aggregation == null || aggregation.getBuckets().isEmpty()) {
                            listener.onResponse(results);
                            return;
                        }
                        this.aggregationBuilder.aggregateAfter(aggregation.afterKey());
                        aggregation.getBuckets().forEach(this::handleBucket);
                        correlation(listener);
                    },
                    listener::onFailure
                ));
        }

        void handleBucket(CompositeAggregation.Bucket bucket) {
            String term = bucket.getKey().get(termField).toString();
            Range termRanges = bucket.getAggregations().get("correlation_range");
            double[] counts = new double[termRanges.getBuckets().size()];
            double browserCount = 0;
            double meanBrowser = 0;
            int i = 0;
            for (Range.Bucket rangeBucket : termRanges.getBuckets()) {
                meanBrowser += rangeBucket.getDocCount() * expectations[i];
                browserCount += rangeBucket.getDocCount();
                counts[i++] = rangeBucket.getDocCount();
            }
            meanBrowser /= browserCount;
            double weight = browserCount / totalHits;
            double[] fractionsBrowserCumulativeSum = new double[counts.length];
            fractionsBrowserCumulativeSum[0] = counts[0]/browserCount;
            for (int j = 1; j < counts.length; j++) {
                fractionsBrowserCumulativeSum[j] = fractionsBrowserCumulativeSum[j - 1] + counts[j] / browserCount;
            }
            PearsonsCorrelation pearsonsCorrelation = new PearsonsCorrelation();
            double corr = pearsonsCorrelation.correlation(expectations, counts);

            logger.info("Browser Stats objects [{}] [{}] [{}] [{}]", term, meanBrowser, weight, fractionsBrowserCumulativeSum);
            PolynomialSplineFunction f_b = new SplineInterpolator().interpolate(expectations, fractionsBrowserCumulativeSum);

            UnivariateFunction cdf_75 = (x) -> (function.value(x)) - 0.75;
            UnivariateFunction cdf_75_eps = (x) -> ((1 - 0.01 * weight) * function.value(x) + weight * f_b.value(x)) - 0.75;
            logger.info("cdf_75 {} {} {} {} {} {}",
                cdf_75.value(expectations[0]),
                cdf_75.value(expectations[expectations.length - 1]),
                function.value(expectations[0]),
                function.value(expectations[expectations.length - 1]),
                f_b.value(expectations[0]),
                f_b.value(expectations[expectations.length - 1])
                );
            logger.info("cdf_75_eps {} {}", cdf_75_eps.value(expectations[0]), cdf_75_eps.value(expectations[expectations.length - 1]));
            BrentSolver solver = new BrentSolver();
            double a = solver.solve(10_000,
                cdf_75,
                expectations[0],
                expectations[expectations.length - 1]
            );
            double b = solver.solve(10_000,
                cdf_75_eps,
                expectations[0],
                expectations[expectations.length - 1]
            );

            results.add(new CorrelationResult(term, corr, weight * (meanBrowser - mean), (b - a)/0.01));
        }

    }

}
