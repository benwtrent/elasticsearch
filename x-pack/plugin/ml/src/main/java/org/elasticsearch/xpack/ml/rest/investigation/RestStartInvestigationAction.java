/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.investigation;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.StartInvestigationAction;
import org.elasticsearch.xpack.core.ml.investigation.InvestigationConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

public class RestStartInvestigationAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, BASE_PATH + "investigation/_start")
        );
    }

    @Override
    public String getName() {
        return "ml_start_investigation_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        InvestigationConfig config = InvestigationConfig.PARSER.apply(restRequest.contentParser(), null);

        return channel -> client.execute(StartInvestigationAction.INSTANCE,
            new StartInvestigationAction.Request(config),
            new RestToXContentListener<>(channel));
    }
}
