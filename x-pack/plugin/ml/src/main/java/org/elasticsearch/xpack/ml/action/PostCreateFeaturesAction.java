/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class PostCreateFeaturesAction extends ActionType<PostCreateFeaturesAction.Response> {

    public static final PostCreateFeaturesAction INSTANCE = new PostCreateFeaturesAction();
    public static final String NAME = "cluster:admin/xpack/ml/inference/features";

    private PostCreateFeaturesAction() {
        super(NAME, PostCreateFeaturesAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<PostCreateFeaturesAction.Request> {

        public static Request parseRequest(XContentParser parser) {
            return new Request();
        }

        Request() {}

        Request(StreamInput input) {}

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        Response(StreamInput input) {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }

    public static class TransportPostCreateFeaturesAction extends HandledTransportAction<
        PostCreateFeaturesAction.Request,
        PostCreateFeaturesAction.Response> {

        protected TransportPostCreateFeaturesAction(TransportService transportService, ActionFilters actionFilters) {
            super(NAME, transportService, actionFilters, Request::new);
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {

        }
    }
}
