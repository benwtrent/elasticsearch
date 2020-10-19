/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.categorization;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetCategorizationConfigsAction;
import org.elasticsearch.xpack.core.ml.categorization.CategorizationConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.core.ml.action.GetCategorizationConfigsAction.Request.ALLOW_NO_MATCH;

public class RestGetCategorizationConfigsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Arrays.asList(
            new Route(GET, MachineLearning.BASE_PATH + "categorization/{" + CategorizationConfig.ID.getPreferredName() + "}"),
            new Route(GET, MachineLearning.BASE_PATH + "categorization")
        );
    }

    @Override
    public String getName() {
        return "ml_get_categorization_configs_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String categorizationId = restRequest.param(CategorizationConfig.ID.getPreferredName());
        if (Strings.isNullOrEmpty(categorizationId)) {
            categorizationId = Metadata.ALL;
        }
        GetCategorizationConfigsAction.Request request = new GetCategorizationConfigsAction.Request(categorizationId);
        if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
            request.setPageParams(new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
        }
        request.setAllowNoResources(restRequest.paramAsBoolean(ALLOW_NO_MATCH.getPreferredName(), request.isAllowNoResources()));
        return channel -> client.execute(GetCategorizationConfigsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

}
