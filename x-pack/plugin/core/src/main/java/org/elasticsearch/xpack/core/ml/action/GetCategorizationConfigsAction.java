/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesRequest;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.categorization.CategorizationConfig;

import java.io.IOException;

public class GetCategorizationConfigsAction extends ActionType<GetCategorizationConfigsAction.Response> {

    public static final GetCategorizationConfigsAction INSTANCE = new GetCategorizationConfigsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/categorization/get";

    private GetCategorizationConfigsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AbstractGetResourcesRequest {

        public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");

        public Request() {
            setAllowNoResources(true);
        }

        public Request(String id) {
            setResourceId(id);
            setAllowNoResources(true);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getResourceIdField() {
            return CategorizationConfig.ID.getPreferredName();
        }
    }

    public static class Response extends AbstractGetResourcesResponse<CategorizationConfig> {

        public static final ParseField RESULTS_FIELD = new ParseField("categorization_configs");

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(QueryPage<CategorizationConfig> configs) {
            super(configs);
        }

        @Override
        protected Reader<CategorizationConfig> getReader() {
            return CategorizationConfig::new;
        }
    }

}
