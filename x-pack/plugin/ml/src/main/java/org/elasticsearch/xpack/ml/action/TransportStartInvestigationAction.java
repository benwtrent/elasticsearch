/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
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
import org.elasticsearch.xpack.core.security.SecurityContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;


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
        final String termField = request.getConfig().getTerms().get(0);
        Correlator correlator = new Correlator(termField);
        client.prepareSearch("correlation_demo")
            .setSource(correlator.rangeQuery())
            .execute(ActionListener.wrap(
                searchResponse -> {
                    AggregationBuilder metricAgg = correlator.buildRangeAggAndSetExpectations(searchResponse.getAggregations());
                    client.prepareSearch("correlation_demo").addAggregation(
                        AggregationBuilders.composite(
                            "correlation_composite",
                            List.of(new TermsValuesSourceBuilder(termField).field(termField))
                        ).subAggregation(metricAgg).size(10000) // TODO actually scroll just all for now
                    ).setSize(0).execute(
                        ActionListener.wrap(
                            compositeSearchResponse -> {
                                CompositeAggregation agg = compositeSearchResponse.getAggregations().get("correlation_composite");
                                for (CompositeAggregation.Bucket bucket : agg.getBuckets()) {
                                    correlator.handleBucket(bucket);
                                }
                                listener.onResponse(new Response(correlator.results));
                            },
                            listener::onFailure
                        )
                    );
                },
                listener::onFailure
            ));

    }

    static class Correlator {

        private Map<String, Object> compositePage;
        private List<Map<String, Object>> results = new ArrayList<>();
        private double[] expectations;
        private final String termField;
        private final KolmogorovSmirnovTest ks2Test = new KolmogorovSmirnovTest();
        private static final double[] PERCENTILE_STEPS = IntStream.range(2, 99)
            .filter(i -> i % 2 == 0)
            .mapToDouble(Double::valueOf)
            .toArray();

        Correlator(String termField) {
            this.termField = termField;
        }

        SearchSourceBuilder rangeQuery() {
            return SearchSourceBuilder.searchSource()
                .aggregation(
                    AggregationBuilders.percentiles("correlation_percentiles")
                        .field("latency")
                        .percentiles(PERCENTILE_STEPS)
                )
                .size(0)
                .trackTotalHits(true);
        }

        AggregationBuilder buildRangeAggAndSetExpectations(Aggregations aggregations) {
            Percentiles percentiles = aggregations.get("correlation_percentiles");
            expectations = new double[PERCENTILE_STEPS.length + 1];
            RangeAggregationBuilder builder = AggregationBuilders.range("correlation_range").field("latency");
            double percentile_0 = percentiles.percentile(PERCENTILE_STEPS[0]);
            builder.addUnboundedTo(percentile_0);
            expectations[0] = percentile_0;
            for (int i = 1; i < PERCENTILE_STEPS.length; i++) {
                double percentile_l = percentiles.percentile(PERCENTILE_STEPS[i - 1]);
                double percentile_r = percentiles.percentile(PERCENTILE_STEPS[i]);
                builder.addRange(percentile_l, percentile_r);
                expectations[i] = 0.5 * (percentile_l + percentile_r);
            }
            double percentile_n = percentiles.percentile(PERCENTILE_STEPS[PERCENTILE_STEPS.length - 1]);
            builder.addUnboundedFrom(percentile_n);
            expectations[PERCENTILE_STEPS.length] = percentile_n;
            return builder;
        }

        void handleBucket(CompositeAggregation.Bucket bucket) {
            String term = bucket.getKey().get(termField).toString();
            Range termRanges = bucket.getAggregations().get("correlation_range");
            final int l = termRanges.getBuckets().size();
            final int a = (int) (l / 5.0 + 0.5);
            final int b = (int) (4.0 * l / 5.0 + 0.5);
            long[] x_b = LongStream.range(0, a)
                .map(i -> termRanges.getBuckets().get((int)i).getDocCount())
                .toArray();
            long[] x_a = LongStream.range(a, l)
                .map(i -> termRanges.getBuckets().get((int)i).getDocCount())
                .toArray();
            double[] counts = termRanges.getBuckets()
                .stream()
                .mapToLong(MultiBucketsAggregation.Bucket::getDocCount)
                .mapToDouble(Double::valueOf)
                .toArray();
            PearsonsCorrelation pearsonsCorrelation = new PearsonsCorrelation();
            double corr = pearsonsCorrelation.correlation(expectations, counts);
            results.add(Collections.singletonMap(term, corr));
        }

    }

}
