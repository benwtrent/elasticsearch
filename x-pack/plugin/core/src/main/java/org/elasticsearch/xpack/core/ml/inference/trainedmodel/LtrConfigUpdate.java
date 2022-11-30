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
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.LtrConfig.PARAMS;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig.NUM_TOP_FEATURE_IMPORTANCE_VALUES;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig.RESULTS_FIELD;

public class LtrConfigUpdate implements InferenceConfigUpdate, NamedXContentObject {

    public static final ParseField NAME = LtrConfig.NAME;

    public static LtrConfigUpdate EMPTY_PARAMS = new LtrConfigUpdate(null, null, null);

    @SuppressWarnings("unchecked")
    public static LtrConfigUpdate fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        String resultsField = (String) options.remove(RESULTS_FIELD.getPreferredName());
        Integer featureImportance = (Integer) options.remove(NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName());
        Map<String, Object> params = (Map<String, Object>) options.remove(PARAMS.getPreferredName());
        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", map.keySet());
        }
        return new LtrConfigUpdate(resultsField, featureImportance, params);
    }

    public static LtrConfigUpdate fromConfig(LtrConfig config) {
        return new LtrConfigUpdate(config.getResultsField(), config.getNumTopFeatureImportanceValues(), config.getParams());
    }

    private static final ObjectParser<LtrConfigUpdate.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<LtrConfigUpdate.Builder, Void> createParser(boolean lenient) {
        ObjectParser<LtrConfigUpdate.Builder, Void> parser = new ObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            LtrConfigUpdate.Builder::new
        );
        parser.declareString(LtrConfigUpdate.Builder::setResultsField, RESULTS_FIELD);
        parser.declareInt(LtrConfigUpdate.Builder::setNumTopFeatureImportanceValues, NUM_TOP_FEATURE_IMPORTANCE_VALUES);
        parser.declareObject(LtrConfigUpdate.Builder::setParams, (p, c) -> p.map(), PARAMS);
        return parser;
    }

    public static LtrConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    private final String resultsField;
    private final Integer numTopFeatureImportanceValues;
    private final Map<String, Object> params;

    public LtrConfigUpdate(String resultsField, Integer numTopFeatureImportanceValues, Map<String, Object> params) {
        this.resultsField = resultsField;
        if (numTopFeatureImportanceValues != null && numTopFeatureImportanceValues < 0) {
            throw new IllegalArgumentException(
                "[" + NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName() + "] must be greater than or equal to 0"
            );
        }
        this.numTopFeatureImportanceValues = numTopFeatureImportanceValues;
        InferenceConfigUpdate.checkFieldUniqueness(resultsField);
        this.params = params;
    }

    public LtrConfigUpdate(StreamInput in) throws IOException {
        this.resultsField = in.readOptionalString();
        this.numTopFeatureImportanceValues = in.readOptionalVInt();
        if (in.readBoolean()) {
            this.params = in.readMap();
        } else {
            this.params = null;
        }
    }

    public Integer getNumTopFeatureImportanceValues() {
        return numTopFeatureImportanceValues;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new Builder().setNumTopFeatureImportanceValues(numTopFeatureImportanceValues)
            .setResultsField(resultsField)
            .setParams(params);
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(resultsField);
        out.writeOptionalVInt(numTopFeatureImportanceValues);
        out.writeBoolean(params != null);
        if (params != null) {
            out.writeGenericMap(params);
        }
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_8_0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        if (numTopFeatureImportanceValues != null) {
            builder.field(NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName(), numTopFeatureImportanceValues);
        }
        if (params != null) {
            builder.field(PARAMS.getPreferredName(), this.params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LtrConfigUpdate that = (LtrConfigUpdate) o;
        return Objects.equals(this.resultsField, that.resultsField)
            && Objects.equals(this.numTopFeatureImportanceValues, that.numTopFeatureImportanceValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultsField, numTopFeatureImportanceValues);
    }

    @Override
    public InferenceConfig apply(InferenceConfig originalConfig) {
        if (originalConfig instanceof LtrConfig == false) {
            throw ExceptionsHelper.badRequestException(
                "Inference config of type [{}] can not be updated with a inference request of type [{}]",
                originalConfig.getName(),
                getName()
            );
        }

        LtrConfig ltrConfig = (LtrConfig) originalConfig;
        if (isNoop(ltrConfig)) {
            return originalConfig;
        }
        LtrConfig.Builder builder = new LtrConfig.Builder(ltrConfig);
        if (resultsField != null) {
            builder.setResultsField(resultsField);
        }
        if (numTopFeatureImportanceValues != null) {
            builder.setNumTopFeatureImportanceValues(numTopFeatureImportanceValues);
        }
        if (params != null) {
            builder.setParams(params);
        }
        return builder.build();
    }

    @Override
    public boolean isSupported(InferenceConfig inferenceConfig) {
        return inferenceConfig instanceof LtrConfig;
    }

    boolean isNoop(LtrConfig originalConfig) {
        return (resultsField == null || originalConfig.getResultsField().equals(resultsField))
            && (numTopFeatureImportanceValues == null || originalConfig.getNumTopFeatureImportanceValues() == numTopFeatureImportanceValues)
            && (params == null || params.equals(originalConfig.getParams()));
    }

    public static class Builder implements InferenceConfigUpdate.Builder<Builder, LtrConfigUpdate> {
        private String resultsField;
        private Integer numTopFeatureImportanceValues;
        private Map<String, Object> params;

        @Override
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

        @Override
        public LtrConfigUpdate build() {
            return new LtrConfigUpdate(resultsField, numTopFeatureImportanceValues, params);
        }
    }
}
