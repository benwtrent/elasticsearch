/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.categorization;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.CategorizeTextAction;
import org.elasticsearch.xpack.core.ml.categorization.CategorizationConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestCategorizeTextAction extends BaseRestHandler {

    public RestCategorizeTextAction(RestController controller) {
        controller.registerHandler(
            POST, MachineLearning.BASE_PATH + "categorization/{" + CategorizationConfig.ID.getPreferredName() + "}/_categorize", this);
    }

    @Override
    public String getName() {
        return "ml_categorize_text_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String categorizationConfigId = restRequest.param(CategorizationConfig.ID.getPreferredName());

        XContentParser parser = restRequest.contentParser();
        CategorizeTextAction.Request request = CategorizeTextAction.Request.parseRequest(categorizationConfigId, parser);
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        return channel -> client.execute(CategorizeTextAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

}
