/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.ltr;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.InferenceToXContentCompressor;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class LtrRescorerBuilder extends RescorerBuilder<LtrRescorerBuilder> {

    private static final ParseField MODEL = new ParseField("model_id");

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("ltr", false, Builder::new);
    static {
        PARSER.declareString(Builder::setModelId, MODEL);
    }

    public static LtrRescorerBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null).build();
    }

    private final String modelId;
    private final Supplier<InferenceDefinition> inferenceDefinitionSupplier;

    public LtrRescorerBuilder(String modelId) {
        this.modelId = modelId;
        this.inferenceDefinitionSupplier = null;
    }

    public LtrRescorerBuilder(Supplier<InferenceDefinition> inferenceDefinitionSupplier) {
        this.modelId = null;
        this.inferenceDefinitionSupplier = inferenceDefinitionSupplier;
    }

    public LtrRescorerBuilder(StreamInput input) throws IOException {
        super(input);
        this.modelId = input.readString();
        this.inferenceDefinitionSupplier = null;
    }

    @Override
    public String getWriteableName() {
        return "ltr";
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT;
    }

    @Override
    public RescorerBuilder<LtrRescorerBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
        if (inferenceDefinitionSupplier != null) {
            // probably need to make inference def serializable
            return this;
        }
        SetOnce<InferenceDefinition> inferenceDefinitionSetOnce = new SetOnce<>();
        ctx.registerAsyncAction((c, l) -> {
            // TODO get inference definition from cache if possible
            // internal action
            c.execute(
                GetTrainedModelsAction.INSTANCE,
                new GetTrainedModelsAction.Request(modelId, List.of(), Set.of(GetTrainedModelsAction.Includes.DEFINITION)),
                ActionListener.wrap(r -> {
                    TrainedModelConfig config = r.getResources().results().get(0);
                    inferenceDefinitionSetOnce.set(
                        InferenceToXContentCompressor.inflate(
                            config.getCompressedDefinition(),
                            InferenceDefinition::fromXContent,
                            ctx.getParserConfig().registry()
                        )
                    );
                    l.onResponse(null);
                }, l::onFailure)
            );
        });
        return new LtrRescorerBuilder(inferenceDefinitionSetOnce::get);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (inferenceDefinitionSupplier != null) {
            throw new IllegalStateException("supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeString(modelId);

    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("ltr");
        builder.field(MODEL.getPreferredName(), modelId);
        builder.endObject();
    }

    @Override
    protected RescoreContext innerBuildContext(int windowSize, SearchExecutionContext context) throws IOException {
        return new LtrRescoreContext(windowSize, new LtrRescorer(), inferenceDefinitionSupplier.get(), context);
    }

    private static class Builder {
        private String modelId;

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        LtrRescorerBuilder build() {
            return new LtrRescorerBuilder(modelId);
        }
    }
}
