/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class LtrConfig implements LenientlyParsedInferenceConfig, StrictlyParsedInferenceConfig {

    public static final ParseField NAME = new ParseField("ltr");
    private static final Version MIN_SUPPORTED_VERSION = Version.CURRENT;
    public static final ParseField RESULTS_FIELD = new ParseField("results_field");
    public static final ParseField NUM_TOP_FEATURE_IMPORTANCE_VALUES = new ParseField("num_top_feature_importance_values");
    public static final ParseField PARAMS = new ParseField("params");

    public static LtrConfig EMPTY_PARAMS = new LtrConfig(DEFAULT_RESULTS_FIELD, null, null);

    private static final ObjectParser<LtrConfig.Builder, Void> LENIENT_PARSER = createParser(true);
    private static final ObjectParser<LtrConfig.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<LtrConfig.Builder, Void> createParser(boolean lenient) {
        ObjectParser<LtrConfig.Builder, Void> parser = new ObjectParser<>(NAME.getPreferredName(), lenient, LtrConfig.Builder::new);
        parser.declareString(LtrConfig.Builder::setResultsField, RESULTS_FIELD);
        parser.declareInt(LtrConfig.Builder::setNumTopFeatureImportanceValues, NUM_TOP_FEATURE_IMPORTANCE_VALUES);
        parser.declareObject(LtrConfig.Builder::setParams, (p, c) -> p.map(), PARAMS);
        return parser;
    }

    public static LtrConfig fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    public static LtrConfig fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null).build();
    }

    private final String resultsField;
    private final int numTopFeatureImportanceValues;
    private final Map<String, Object> params;

    public LtrConfig(String resultsField) {
        this(resultsField, 0, null);
    }

    public LtrConfig(String resultsField, Integer numTopFeatureImportanceValues, Map<String, Object> params) {
        this.resultsField = resultsField == null ? DEFAULT_RESULTS_FIELD : resultsField;
        if (numTopFeatureImportanceValues != null && numTopFeatureImportanceValues < 0) {
            throw new IllegalArgumentException(
                "[" + NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName() + "] must be greater than or equal to 0"
            );
        }
        this.numTopFeatureImportanceValues = numTopFeatureImportanceValues == null ? 0 : numTopFeatureImportanceValues;
        this.params = params == null ? Map.of() : params;
    }

    public LtrConfig(StreamInput in) throws IOException {
        this.resultsField = in.readString();
        this.numTopFeatureImportanceValues = in.readVInt();
        this.params = in.readMap();
    }

    public int getNumTopFeatureImportanceValues() {
        return numTopFeatureImportanceValues;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public boolean requestingImportance() {
        return numTopFeatureImportanceValues > 0;
    }

    @Override
    public boolean isAllocateOnly() {
        return false;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(resultsField);
        out.writeVInt(numTopFeatureImportanceValues);
        out.writeGenericMap(params);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        builder.field(NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName(), numTopFeatureImportanceValues);
        builder.field(PARAMS.getPreferredName(), this.params);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LtrConfig that = (LtrConfig) o;
        return Objects.equals(this.resultsField, that.resultsField)
            && Objects.equals(this.numTopFeatureImportanceValues, that.numTopFeatureImportanceValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultsField, numTopFeatureImportanceValues);
    }

    @Override
    public boolean isTargetTypeSupported(TargetType targetType) {
        return TargetType.REGRESSION.equals(targetType);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return requestingImportance() ? Version.V_7_7_0 : MIN_SUPPORTED_VERSION;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String resultsField;
        private Integer numTopFeatureImportanceValues;
        private Map<String, Object> params;

        Builder() {}

        Builder(LtrConfig config) {
            this.resultsField = config.resultsField;
            this.numTopFeatureImportanceValues = config.numTopFeatureImportanceValues;
            this.params = config.params;
        }

        public Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public Builder setNumTopFeatureImportanceValues(Integer numTopFeatureImportanceValues) {
            this.numTopFeatureImportanceValues = numTopFeatureImportanceValues;
            return this;
        }

        public Builder setParams(Map<String, Object> params) {
            this.params = params;
            return this;
        }

        public LtrConfig build() {
            return new LtrConfig(resultsField, numTopFeatureImportanceValues, params);
        }
    }
}
