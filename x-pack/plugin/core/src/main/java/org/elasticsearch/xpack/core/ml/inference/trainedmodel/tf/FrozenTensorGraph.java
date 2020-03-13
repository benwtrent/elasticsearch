package org.elasticsearch.xpack.core.ml.inference.trainedmodel.tf;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FrozenTensorGraph implements LenientlyParsedTrainedModel, StrictlyParsedTrainedModel {

    public static final ParseField NAME = new ParseField("frozen_tensor_graph");
    public static final ParseField FEATURE_NAMES = new ParseField("feature_names");
    public static final ParseField GRAPH_STRUCTURE = new ParseField("graph_structure");
    public static final ParseField TARGET_TYPE = new ParseField("target_type");
    public static final ParseField CLASSIFICATION_LABELS = new ParseField("classification_labels");

    private static final ObjectParser<FrozenTensorGraph.Builder, Void> LENIENT_PARSER = createParser(true);
    private static final ObjectParser<FrozenTensorGraph.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<FrozenTensorGraph.Builder, Void> createParser(boolean lenient) {
        ObjectParser<FrozenTensorGraph.Builder, Void> parser = new ObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            FrozenTensorGraph.Builder::new);
        parser.declareStringArray(FrozenTensorGraph.Builder::setFeatureNames, FEATURE_NAMES);
        parser.declareString(FrozenTensorGraph.Builder::setGraphStructure, GRAPH_STRUCTURE);
        parser.declareString(FrozenTensorGraph.Builder::setTargetType, TARGET_TYPE);
        parser.declareStringArray(FrozenTensorGraph.Builder::setClassificationLabels, CLASSIFICATION_LABELS);
        return parser;
    }

    public static FrozenTensorGraph fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    public static FrozenTensorGraph fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null).build();
    }

    private final String graphStructure;
    private final List<String> featureNames;
    private final TargetType targetType;
    private final List<String> classificationLabels;

    FrozenTensorGraph(String graphStructure, List<String> featureNames, TargetType targetType, List<String> classificationLabels) {
        this.graphStructure = ExceptionsHelper.requireNonNull(graphStructure, GRAPH_STRUCTURE);
        this.featureNames = featureNames;
        this.targetType = targetType;
        this.classificationLabels = classificationLabels;
    }

    public FrozenTensorGraph(StreamInput in) throws IOException {
        this.featureNames = Collections.unmodifiableList(in.readStringList());
        this.graphStructure = in.readString();
        this.targetType = TargetType.fromStream(in);
        if (in.readBoolean()) {
            this.classificationLabels = Collections.unmodifiableList(in.readStringList());
        } else {
            this.classificationLabels = null;
        }
    }

    public String getGraphStructure() {
        return graphStructure;
    }

    public List<String> getFeatureNames() {
        return featureNames;
    }

    public TargetType getTargetType() {
        return targetType;
    }

    public List<String> getClassificationLabels() {
        return classificationLabels;
    }

    @Override
    public InferenceResults infer(Map<String, Object> fields, InferenceConfig config, Map<String, String> featureDecoderMap) {
        return null;
    }

    @Override
    public TargetType targetType() {
        return null;
    }

    @Override
    public void validate() {

    }

    @Override
    public long estimatedNumOperations() {
        return 0;
    }

    @Override
    public boolean supportsFeatureImportance() {
        return false;
    }

    @Override
    public Map<String, Double> featureImportance(Map<String, Object> fields, Map<String, String> featureDecoder) {
        return null;
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(featureNames);
        out.writeString(graphStructure);
        targetType.writeTo(out);
        out.writeBoolean(classificationLabels != null);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FEATURE_NAMES.getPreferredName(), featureNames);
        builder.field(GRAPH_STRUCTURE.getPreferredName(), graphStructure);
        builder.field(TARGET_TYPE.getPreferredName(), targetType.toString());
        if(classificationLabels != null) {
            builder.field(CLASSIFICATION_LABELS.getPreferredName(), classificationLabels);
        }
        builder.endObject();
        return  builder;
    }


    public static class Builder {

        private String graphStructure;
        private List<String> featureNames;
        private TargetType targetType;
        private List<String> classificationLabels;

        public Builder setGraphStructure(String graphStructure) {
            this.graphStructure = graphStructure;
            return this;
        }

        public Builder setFeatureNames(List<String> featureNames) {
            this.featureNames = featureNames;
            return this;
        }

        public Builder setTargetType(TargetType targetType) {
            this.targetType = targetType;
            return this;
        }

        private void setTargetType(String targetType) {
            this.targetType = TargetType.fromString(targetType);
        }

        public Builder setClassificationLabels(List<String> classificationLabels) {
            this.classificationLabels = classificationLabels;
            return this;
        }

        public FrozenTensorGraph build() {
            return new FrozenTensorGraph(graphStructure, featureNames, targetType, classificationLabels);
        }
    }
}
