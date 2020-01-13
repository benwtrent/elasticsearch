/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.categorization.CategorizationConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class PutCategorizationConfigAction extends ActionType<PutCategorizationConfigAction.Response> {

    public static final PutCategorizationConfigAction INSTANCE = new PutCategorizationConfigAction();
    public static final String NAME = "cluster:admin/xpack/ml/categorization/put";

    private PutCategorizationConfigAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        public static Request parseRequest(String categorizationConfigId, XContentParser parser) {
            CategorizationConfig.Builder categorizationBuilder = CategorizationConfig.STRICT_PARSER.apply(parser, null);
            if (categorizationBuilder.getCategorizationConfigId() == null) {
                categorizationBuilder.setCategorizationConfigId(categorizationConfigId);
            } else if (!Strings.isNullOrEmpty(categorizationConfigId) && !categorizationConfigId.equals(categorizationBuilder.getCategorizationConfigId())) {
                // If we have both URI and body jobBuilder ID, they must be identical
                throw new IllegalArgumentException(Messages.getMessage(Messages.INCONSISTENT_ID, CategorizationConfig.ID.getPreferredName(),
                    categorizationBuilder.getCategorizationConfigId(), categorizationConfigId));
            }

            return new Request(categorizationBuilder.build());
        }

        private CategorizationConfig categorizationConfig;

        public Request(CategorizationConfig categorizationConfig) {
            this.categorizationConfig = categorizationConfig;
        }

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            categorizationConfig = new CategorizationConfig(in);
        }

        public CategorizationConfig getCategorizationConfig() {
            return categorizationConfig;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            categorizationConfig.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return categorizationConfig.toXContent(builder, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(categorizationConfig, request.categorizationConfig);
        }

        @Override
        public int hashCode() {
            return Objects.hash(categorizationConfig);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final CategorizationConfig categorizationConfig;

        public Response(CategorizationConfig categorizationConfig) {
            this.categorizationConfig = categorizationConfig;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            categorizationConfig = new CategorizationConfig(in);
        }

        public CategorizationConfig getResponse() {
            return categorizationConfig;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            categorizationConfig.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return categorizationConfig.toXContent(builder, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(categorizationConfig, response.categorizationConfig);
        }

        @Override
        public int hashCode() {
            return Objects.hash(categorizationConfig);
        }
    }
}
