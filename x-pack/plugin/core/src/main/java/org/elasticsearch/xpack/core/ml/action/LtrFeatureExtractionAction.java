/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.ltr.RankEvalSpec;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class LtrFeatureExtractionAction extends ActionType<LtrFeatureExtractionAction.Response> {

    public static final LtrFeatureExtractionAction INSTANCE = new LtrFeatureExtractionAction();
    public static final String NAME = "cluster:admin/xpack/ml/ltr_feature_extraction";

    private LtrFeatureExtractionAction() {
        super(NAME, LtrFeatureExtractionAction.Response::new);
    }

    public static class Request extends ActionRequest implements IndicesRequest.Replaceable {

        public static final ParseField NUM_DOCS = new ParseField("num_docs");
        public static final ParseField DESTINATION = new ParseField("destination");
        public static final ParseField RANK_EVAL_DEF = new ParseField("rank_eval_def");

        private static final ConstructingObjectParser<Request, Void> PARSER =
            new ConstructingObjectParser<>("ltr_feature_request",
                (a) -> new Request((RankEvalSpec)a[0], (String)a[1], (Integer)a[2]));

        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> RankEvalSpec.parse(p), RANK_EVAL_DEF);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), DESTINATION);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), NUM_DOCS);
        }

        public static Request parseRequest(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private RankEvalSpec rankingEvaluationSpec;
        private IndicesOptions indicesOptions  = SearchRequest.DEFAULT_INDICES_OPTIONS;
        private String[] indices = Strings.EMPTY_ARRAY;
        private SearchType searchType = SearchType.DEFAULT;
        private String destinationIndex;
        private int numDocs = 10;

        public Request(RankEvalSpec rankingEvaluationSpec, String destinationIndex, Integer numDocs) {
            this.rankingEvaluationSpec = Objects.requireNonNull(rankingEvaluationSpec, "ranking evaluation specification must not be null");
            this.destinationIndex = Objects.requireNonNull(destinationIndex, "destination index must not be null");
            if (numDocs != null) {
                this.numDocs = numDocs;
            }
            indices(indices);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            rankingEvaluationSpec = new RankEvalSpec(in);
            indices = in.readStringArray();
            indicesOptions = IndicesOptions.readIndicesOptions(in);
            searchType = SearchType.fromId(in.readByte());
            destinationIndex = in.readString();
            numDocs = in.readVInt();
        }

        Request() {
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = null;
            if (rankingEvaluationSpec == null) {
                e = new ActionRequestValidationException();
                e.addValidationError("missing ranking evaluation specification");
            }
            if (destinationIndex == null) {
                e = new ActionRequestValidationException();
                e.addValidationError("missing destination parameter");
            }
            return e;
        }

        /**
         * Returns the specification of the ranking evaluation.
         */
        public RankEvalSpec getRankEvalSpec() {
            return rankingEvaluationSpec;
        }

        /**
         * Set the specification of the ranking evaluation.
         */
        public void setRankEvalSpec(RankEvalSpec task) {
            this.rankingEvaluationSpec = task;
        }

        public String getDestinationIndex() {
            return destinationIndex;
        }

        public int getNumDocs() {
            return numDocs;
        }

        /**
         * Sets the indices the search will be executed on.
         */
        @Override
        public Request indices(String... indices) {
            Objects.requireNonNull(indices, "indices must not be null");
            for (String index : indices) {
                Objects.requireNonNull(index, "index must not be null");
            }
            this.indices = indices;
            return this;
        }

        /**
         * @return the indices for this request
         */
        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        public void indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = Objects.requireNonNull(indicesOptions, "indicesOptions must not be null");
        }

        /**
         * The search type to execute, defaults to {@link SearchType#DEFAULT}.
         */
        public void searchType(SearchType searchType) {
            this.searchType = Objects.requireNonNull(searchType, "searchType must not be null");
        }

        /**
         * The type of search to execute.
         */
        public SearchType searchType() {
            return searchType;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            rankingEvaluationSpec.writeTo(out);
            out.writeStringArray(indices);
            indicesOptions.writeIndicesOptions(out);
            out.writeByte(searchType.id());
            out.writeString(destinationIndex);
            out.writeVInt(numDocs);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Request that = (Request) o;
            return Objects.equals(indicesOptions, that.indicesOptions) &&
                Arrays.equals(indices, that.indices) &&
                Objects.equals(rankingEvaluationSpec, that.rankingEvaluationSpec) &&
                Objects.equals(searchType, that.searchType) &&
                Objects.equals(destinationIndex, that.destinationIndex) &&
                Objects.equals(numDocs, that.numDocs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indicesOptions, Arrays.hashCode(indices), rankingEvaluationSpec, searchType, destinationIndex, numDocs);
        }
    }

    public static class Response extends ActionResponse implements StatusToXContentObject {

        private Map<String, ElasticsearchException> errors;
        public Response(Map<String, ElasticsearchException> errors) {
            this.errors = errors;
        }

        public Response(StreamInput in) throws IOException {
            errors = in.readMap(StreamInput::readString, StreamInput::readException);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(errors, StreamOutput::writeString, StreamOutput::writeException);
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("errors", errors);
            builder.endObject();
            return builder;
        }
    }
}
