/*
 * Copyright [2017] Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.xpack.ml.ltr;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LenientlyParsedPreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedInferenceConfig;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class CreateFeaturesSearchExtBuilder extends SearchExtBuilder {
    public static final String NAME = "ltr";
    private static final ParseField PROCESSORS = new ParseField("processors");
    private static final ParseField INFERENCE_CONFIG = new ParseField("inference_config");

    private static final ObjectParser<CreateFeaturesSearchExtBuilder, Void> PARSER = new ObjectParser<>(
        NAME,
        true,
        CreateFeaturesSearchExtBuilder::new
    );

    static {
        PARSER.declareNamedObjects(
            CreateFeaturesSearchExtBuilder::setProcessors,
            (p, c, n) -> p.namedObject(LenientlyParsedPreProcessor.class, n, PreProcessor.PreProcessorParseContext.DEFAULT),
            v -> {},
            PROCESSORS
        );
        PARSER.declareNamedObject(
            CreateFeaturesSearchExtBuilder::setInferenceConfig,
            (p, c, n) -> p.namedObject(LenientlyParsedInferenceConfig.class, n, true),
            INFERENCE_CONFIG
        );
    }

    public static CreateFeaturesSearchExtBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private List<PreProcessor> processors;
    private InferenceConfig inferenceConfig;

    CreateFeaturesSearchExtBuilder() {}

    public CreateFeaturesSearchExtBuilder(StreamInput input) throws IOException {
        processors = input.readNamedWriteableList(PreProcessor.class);
        inferenceConfig = input.readNamedWriteable(InferenceConfig.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(processors);
        out.writeNamedWriteable(inferenceConfig);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        NamedXContentObjectHelper.writeNamedObjects(builder, params, true, PROCESSORS.getPreferredName(), processors);
        return builder.endObject();
    }

    private void setProcessors(List<PreProcessor> processors) {
        this.processors = processors;
    }

    private void setInferenceConfig(InferenceConfig inferenceConfig) {
        this.inferenceConfig = inferenceConfig;
    }

    public List<PreProcessor> getProcessors() {
        return processors;
    }

    public InferenceConfig getInferenceConfig() {
        return inferenceConfig;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateFeaturesSearchExtBuilder that = (CreateFeaturesSearchExtBuilder) o;
        return Objects.equals(processors, that.processors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processors);
    }
}
