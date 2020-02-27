/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.rest.ltr;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.LtrFeatureExtractionAction;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestLtfFeatureExtractionAction extends BaseRestHandler {

    public static String ENDPOINT = MachineLearning.BASE_PATH + "ltr/_feature_creation";

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, ENDPOINT),
            new Route(POST, ENDPOINT),
            new Route(GET, ENDPOINT + "/{index}" ),
            new Route(POST, ENDPOINT + "/{index}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        XContentParser parser = restRequest.contentParser();
        LtrFeatureExtractionAction.Request request  = LtrFeatureExtractionAction.Request.parseRequest(parser);
        request.indices(Strings.splitStringByCommaToArray(restRequest.param("index")));
        request.indicesOptions(IndicesOptions.fromRequest(restRequest, request.indicesOptions()));
        if (restRequest.hasParam("search_type")) {
            request.searchType(SearchType.fromString(restRequest.param("search_type")));
        }
        return channel -> client.execute(LtrFeatureExtractionAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "ltr_feature_extraction";
    }
}
