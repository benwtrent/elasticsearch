/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.categorization.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.categorization.CategorizationConfig;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class CategorizationProvider {

    private static final Logger logger = LogManager.getLogger(CategorizationProvider.class);
    private final Client client;
    private static final ToXContent.Params FOR_INTERNAL_STORAGE_PARAMS =
        new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));

    public CategorizationProvider(Client client) {
        this.client = client;
    }

    public void storeCategorizationConfig(CategorizationConfig categorizationConfig, ActionListener<Boolean> listener) {
        ActionListener<IndexResponse> indexResponseActionListener = ActionListener.wrap(
            r -> listener.onResponse(true),
            listener::onFailure
        );
        executeAsyncWithOrigin(client, ML_ORIGIN,
            IndexAction.INSTANCE,
            createRequest(CategorizationConfig.NAME + "_" + categorizationConfig.getCategorizationConfigId(),
                categorizationConfig),
            indexResponseActionListener);
    }

    public void getSingleCategorizationConfig(String categorizationId, ActionListener<CategorizationConfig> listener) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery(CategorizationConfig.ID.getPreferredName(), categorizationId)))
            .trackTotalHits(false)
            .size(1);
        SearchRequest searchRequest = new SearchRequest().indices(AnomalyDetectorsIndex.configIndexName()).source(sourceBuilder);
        executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    for (SearchHit hit : response.getHits().getHits()) {
                        BytesReference docSource = hit.getSourceRef();
                        try (InputStream stream = docSource.streamInput();
                             XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(
                                 NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
                            listener.onResponse(CategorizationConfig.fromXContent(parser, true).build());
                        } catch (IOException e) {
                            this.onFailure(e);
                        }
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            },
            client::search);
    }



    private IndexRequest createRequest(String docId, ToXContentObject body) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = body.toXContent(builder, FOR_INTERNAL_STORAGE_PARAMS);

            return new IndexRequest(AnomalyDetectorsIndex.configIndexName())
                .opType(DocWriteRequest.OpType.CREATE)
                .id(docId)
                .source(source);
        } catch (IOException ex) {
            // This should never happen. If we were able to deserialize the object (from Native or REST) and then fail to serialize it again
            // that is not the users fault. We did something wrong and should throw.
            throw ExceptionsHelper.serverError(
                new ParameterizedMessage("Unexpected serialization exception for [{}]", docId).getFormattedMessage(),
                ex);
        }
    }
}
