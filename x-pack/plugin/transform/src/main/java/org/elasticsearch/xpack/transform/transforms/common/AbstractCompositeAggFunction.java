/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.transform.transforms.Function;
import org.elasticsearch.xpack.transform.transforms.pivot.AggregationResultUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Basic abstract class for implementing a transform function that utilizes composite aggregations
 */
public abstract class AbstractCompositeAggFunction implements Function {

    public static final int TEST_QUERY_PAGE_SIZE = 50;
    public static final String COMPOSITE_AGGREGATION_NAME = "_transform";

    private final CompositeAggregationBuilder cachedCompositeAggregation;

    /**
     * Creates a composite aggregation from the passed {@GroupConfig} or throws if there are pasing errors.
     * @param config The configuration to parse into a composite aggregation
     * @param functionName the calling transform function name. Used in error messages
     * @return A parsed {@link CompositeAggregationBuilder} object
     */
    public static final CompositeAggregationBuilder createCompositeAggregationSources(GroupConfig config, String functionName) {
        CompositeAggregationBuilder compositeAggregation;

        try (XContentBuilder builder = jsonBuilder()) {
            config.toCompositeAggXContent(builder);
            XContentParser parser = builder.generator()
                .contentType()
                .xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
            compositeAggregation = CompositeAggregationBuilder.PARSER.parse(parser, COMPOSITE_AGGREGATION_NAME);
        } catch (IOException e) {
            throw new RuntimeException(
                TransformMessages.getMessage(TransformMessages.TRANSFORM_FAILED_TO_CREATE_COMPOSITE_AGGREGATION, functionName), e);
        }
        return compositeAggregation;
    }

    public AbstractCompositeAggFunction(CompositeAggregationBuilder compositeAggregationBuilder) {
        cachedCompositeAggregation = compositeAggregationBuilder;
    }

    @Override
    public SearchSourceBuilder buildSearchQuery(SearchSourceBuilder builder, Map<String, Object> position, int pageSize) {
        cachedCompositeAggregation.aggregateAfter(position);
        cachedCompositeAggregation.size(pageSize);
        return builder.size(0).aggregation(cachedCompositeAggregation);
    }

    @Override
    public void preview(
        Client client,
        Map<String, String> headers,
        SourceConfig sourceConfig,
        Map<String, String> fieldTypeMap,
        int numberOfBuckets,
        ActionListener<List<Map<String, Object>>> listener
    ) {
        ClientHelper.assertNoAuthorizationHeader(headers);
        ClientHelper.executeWithHeadersAsync(
            headers,
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            SearchAction.INSTANCE,
            buildSearchRequest(sourceConfig, null, numberOfBuckets),
            ActionListener.wrap(r -> {
                try {
                    final Aggregations aggregations = r.getAggregations();
                    if (aggregations == null) {
                        listener.onFailure(
                            new ElasticsearchStatusException("Source indices have been deleted or closed.", RestStatus.BAD_REQUEST));
                        return;
                    }
                    final CompositeAggregation agg = aggregations.get(COMPOSITE_AGGREGATION_NAME);
                    TransformIndexerStats stats = new TransformIndexerStats();

                    List<Map<String, Object>> docs = extractResults(agg, fieldTypeMap, stats)
                        .map(this::documentTransformationFunction)
                        .collect(Collectors.toList());

                    listener.onResponse(docs);
                } catch (AggregationResultUtils.AggregationExtractionException extractionException) {
                    listener.onFailure(new ElasticsearchStatusException(extractionException.getMessage(), RestStatus.BAD_REQUEST));
                }
            }, listener::onFailure)
        );
    }

    @Override
    public void validateQuery(Client client, SourceConfig sourceConfig, ActionListener<Boolean> listener) {
        SearchRequest searchRequest = buildSearchRequest(sourceConfig, null, TEST_QUERY_PAGE_SIZE);
        client.execute(SearchAction.INSTANCE, searchRequest, ActionListener.wrap(response -> {
            if (response == null) {
                listener.onFailure(
                    new ElasticsearchStatusException("Unexpected null response from test query", RestStatus.SERVICE_UNAVAILABLE)
                );
                return;
            }
            if (response.status() != RestStatus.OK) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Unexpected status from response of test query: {}", response.status(), response.status())
                );
                return;
            }
            listener.onResponse(true);
        }, e -> {
            Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
            RestStatus status = unwrapped instanceof ElasticsearchException
                ? ((ElasticsearchException) unwrapped).status()
                : RestStatus.SERVICE_UNAVAILABLE;
            listener.onFailure(new ElasticsearchStatusException("Failed to test query", status, unwrapped));
        }));
    }

    @Override
    public Tuple<Stream<IndexRequest>, Map<String, Object>> processSearchResponse(
        SearchResponse searchResponse,
        String destinationIndex,
        String destinationPipeline,
        Map<String, String> fieldTypeMap,
        TransformIndexerStats stats
    ) {
        Aggregations aggregations = searchResponse.getAggregations();

        // Treat this as a "we reached the end".
        // This should only happen when all underlying indices have gone away. Consequently, there is no more data to read.
        if (aggregations == null) {
            return null;
        }

        CompositeAggregation compositeAgg = aggregations.get(COMPOSITE_AGGREGATION_NAME);
        if (compositeAgg == null || compositeAgg.getBuckets().isEmpty()) {
            return null;
        }

        Stream<IndexRequest> indexRequestStream = extractResults(compositeAgg, fieldTypeMap, stats)
            .map(doc -> {
                String docId = (String)doc.remove(TransformField.DOCUMENT_ID_FIELD);
                return DocumentConversionUtils.convertDocumentToIndexRequest(
                    docId,
                    documentTransformationFunction(doc),
                    destinationIndex,
                    destinationPipeline
                );
            });

        return Tuple.tuple(indexRequestStream, compositeAgg.afterKey());
    }

    protected abstract Map<String, Object> documentTransformationFunction(Map<String, Object> document);

    protected abstract Stream<Map<String, Object>> extractResults(
        CompositeAggregation agg,
        Map<String, String> fieldTypeMap,
        TransformIndexerStats transformIndexerStats
    );

    private SearchRequest buildSearchRequest(SourceConfig sourceConfig, Map<String, Object> position, int pageSize) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(sourceConfig.getQueryConfig().getQuery())
            .runtimeMappings(sourceConfig.getRuntimeMappings());
        buildSearchQuery(sourceBuilder, null, pageSize);
        return new SearchRequest(sourceConfig.getIndex())
            .source(sourceBuilder)
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
    }

    @Override
    public void getInitialProgressFromResponse(SearchResponse response, ActionListener<TransformProgress> progressListener) {
        progressListener.onResponse(new TransformProgress(response.getHits().getTotalHits().value, 0L, 0L));
    }

}
