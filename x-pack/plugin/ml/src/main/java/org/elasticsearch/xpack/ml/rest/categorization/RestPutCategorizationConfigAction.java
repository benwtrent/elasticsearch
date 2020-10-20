/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.categorization;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.PutCategorizationConfigAction;
import org.elasticsearch.xpack.core.ml.categorization.CategorizationConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RestPutCategorizationConfigAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Arrays.asList(
            new Route(RestRequest.Method.PUT,
                MachineLearning.BASE_PATH + "categorization/{" + CategorizationConfig.ID.getPreferredName() + "}")
        );
    }

    @Override
    public String getName() {
        return "xpack_ml_put_categorization_config_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(CategorizationConfig.ID.getPreferredName());
        XContentParser parser = restRequest.contentParser();
        PutCategorizationConfigAction.Request putRequest = PutCategorizationConfigAction.Request.parseRequest(id, parser);
        putRequest.timeout(restRequest.paramAsTime("timeout", putRequest.timeout()));

        return channel -> client.execute(PutCategorizationConfigAction.INSTANCE, putRequest, new RestToXContentListener<>(channel));
    }
}
