/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;


public class TransportStartInvestigationAction extends HandledTransportAction<Request, Response> {

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
                    .query(investigationConfig.getSourceConfig().getQuery())
                )
                .execute(ActionListener.wrap(
                    searchResponse -> {
                        Percentiles percentiles = searchResponse.getAggregations().get("correlation_percentiles");
                        var val = TermCorrelator.buildRangeAggAndSetExpectations(percentiles,
                            PERCENTILE_STEPS,
                            investigationConfig.getKeyIndicator()
                        );

                        GroupedActionListener<Map<String, Object>> groupedActionListener = new GroupedActionListener<>(
                            ActionListener.wrap(
                                results -> responseActionListener.onResponse(new Response(results)),
                                responseActionListener::onFailure
                            ),
                            investigationConfig.getTerms().size()
                        );

                        for (String term : investigationConfig.getTerms()) {
                            TermCorrelator termCorrelator = new TermCorrelator(term,
                                val.v2(),
                                val.v1(),
                                client,
                                investigationConfig.getSourceConfig()
                            );
                            termCorrelator.correlation(groupedActionListener);
                        }
                    },
                    responseActionListener::onFailure
                ));
        }
    }

    static class TermCorrelator {

        static Tuple<AggregationBuilder, double[]> buildRangeAggAndSetExpectations(Percentiles percentiles,
                                                                                   double[] steps,
                                                                                   String indicatorFieldName) {
            double[] expectations = new double[steps.length + 1];
            RangeAggregationBuilder builder = AggregationBuilders.range("correlation_range").field(indicatorFieldName);
            double percentile_0 = percentiles.percentile(steps[0]);
            builder.addUnboundedTo(percentile_0);
            expectations[0] = percentile_0;
            for (int i = 1; i < steps.length; i++) {
                double percentile_l = percentiles.percentile(steps[i - 1]);
                double percentile_r = percentiles.percentile(steps[i]);
                builder.addRange(percentile_l, percentile_r);
                expectations[i] = 0.5 * (percentile_l + percentile_r);
            }
            double percentile_n = percentiles.percentile(steps[steps.length - 1]);
            builder.addUnboundedFrom(percentile_n);
            expectations[steps.length] = percentile_n;
            return Tuple.tuple(builder, expectations);
        }

        private Map<String, Object> results = new ConcurrentHashMap<>();
        private double[] expectations;
        private final String termField;
        private final CompositeAggregationBuilder aggregationBuilder;
        private final Client client;
        private final InvestigationSource source;
        TermCorrelator(String termField,
                       double[] expectations,
                       AggregationBuilder rangeAgg,
                       OriginSettingClient client,
                       InvestigationSource source) {
            this.termField = termField;
            this.expectations = expectations;
            this.aggregationBuilder = AggregationBuilders.composite(
                "composite_" + termField,
                List.of(new TermsValuesSourceBuilder(termField).field(termField)))
                .size(100)
                .subAggregation(rangeAgg);
            this.client = client;
            this.source = source;
        }

        void correlation(ActionListener<Map<String, Object>> listener) {
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
            final int l = termRanges.getBuckets().size();
            double[] counts = termRanges.getBuckets()
                .stream()
                .mapToLong(MultiBucketsAggregation.Bucket::getDocCount)
                .mapToDouble(Double::valueOf)
                .toArray();
            PearsonsCorrelation pearsonsCorrelation = new PearsonsCorrelation();
            double corr = pearsonsCorrelation.correlation(expectations, counts);
            results.put(term, corr);
        }

    }

}
