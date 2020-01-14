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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.xpack.core.ml.categorization.CategorizationConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class CategorizeTextAction extends ActionType<CategorizeTextAction.Response> {

    public static final CategorizeTextAction INSTANCE = new CategorizeTextAction();
    public static final String NAME = "cluster:admin/xpack/ml/categorization/categorize";

    private CategorizeTextAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        public static final ParseField TEXT = new ParseField("text");
        private static ConstructingObjectParser<Request, Void> PARSER =
            new ConstructingObjectParser<>("categorize_text_action_request", false, a -> new Request((String)a[0], (String)a[1]));
        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TEXT);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), CategorizationConfig.ID);
        }
        public static Request parseRequest(String categorizationConfigId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (request.getCategorizationConfigId() == null) {
                request.setCategorizationConfigId(categorizationConfigId);
            } else if (!Strings.isNullOrEmpty(categorizationConfigId) && !categorizationConfigId.equals(request.getCategorizationConfigId())) {
                // If we have both URI and body jobBuilder ID, they must be identical
                throw new IllegalArgumentException(Messages.getMessage(Messages.INCONSISTENT_ID, CategorizationConfig.ID.getPreferredName(),
                    request.getCategorizationConfigId(), categorizationConfigId));
            }
            return request;
        }

        private String text;
        private String categorizationConfigId;
        //TODO remove
        private boolean cacheCategorization;

        public Request(String text, String categorizationConfigId) {
            this.text = text;
            this.categorizationConfigId = categorizationConfigId;
        }

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.text = in.readString();
            this.categorizationConfigId = in.readString();
            this.cacheCategorization = in.readBoolean();
        }

        public String getText() {
            return text;
        }

        public String getCategorizationConfigId() {
            return categorizationConfigId;
        }

        public void setCategorizationConfigId(String categorizationConfigId) {
            this.categorizationConfigId = categorizationConfigId;
        }

        public boolean isCacheCategorization() {
            return cacheCategorization;
        }

        public void setCacheCategorization(boolean cacheCategorization) {
            this.cacheCategorization = cacheCategorization;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(categorizationConfigId);
            out.writeString(text);
            out.writeBoolean(cacheCategorization);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TEXT.getPreferredName(), text);
            builder.field(CategorizationConfig.ID.getPreferredName(), categorizationConfigId);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(categorizationConfigId, request.categorizationConfigId) && Objects.equals(text, request.text);
        }

        @Override
        public int hashCode() {
            return Objects.hash(categorizationConfigId, text);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final CategoryDefinition categoryDefinition;

        public Response(CategoryDefinition categoryDefinition) {
            this.categoryDefinition = categoryDefinition;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            categoryDefinition = in.readBoolean() ? new CategoryDefinition(in) : null;
        }

        public CategoryDefinition getResponse() {
            return categoryDefinition;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(categoryDefinition != null);
            if (categoryDefinition != null) {
                categoryDefinition.writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (categoryDefinition != null) {
                categoryDefinition.toXContent(builder, params);
            }
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(categoryDefinition, response.categoryDefinition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(categoryDefinition);
        }

        public void writeToDoc(String fieldPrefix, IngestDocument document) {
            document.setFieldValue(fieldPrefix + ".category_id", categoryDefinition.getCategoryId());
            document.setFieldValue(fieldPrefix + ".grok", categoryDefinition.getGrokPattern());
            document.appendFieldValue(fieldPrefix + ".terms", Arrays.asList(categoryDefinition.getTerms().split(" ")));
        }
    }
}
