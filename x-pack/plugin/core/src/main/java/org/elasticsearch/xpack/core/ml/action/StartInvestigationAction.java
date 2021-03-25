/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class StartInvestigationAction extends ActionType<StartInvestigationAction.Response> {

    public static final StartInvestigationAction INSTANCE = new StartInvestigationAction();

    public static final String NAME = "cluster:monitor/xpack/ml/investigation/start";

    private StartInvestigationAction() {
        super(NAME, StartInvestigationAction.Response::new);
    }

    public static class Request extends ActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject  {

        private final List<Map<String, Object>> items;
        public Response(List<Map<String, Object>> items) {
            this.items = items;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            items = in.readList(StreamInput::readMap);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("results", items);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(items, StreamOutput::writeMap);
        }
    }
}
