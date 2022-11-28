/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.ltr;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.ScriptScoreFunction;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LenientlyParsedPreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.StrictlyParsedPreProcessor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ScriptScoreProcessor implements LenientlyParsedPreProcessor, StrictlyParsedPreProcessor {

    public static final ParseField NAME = new ParseField("script_score_processor");
    private static final ParseField SCRIPT = new ParseField("script");
    private static final ParseField OUTPUT_FIELD = new ParseField("output_field");

    private static final ConstructingObjectParser<ScriptScoreProcessor, PreProcessorParseContext> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<ScriptScoreProcessor, PreProcessorParseContext> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<ScriptScoreProcessor, PreProcessorParseContext> createParser(boolean lenient) {
        ConstructingObjectParser<ScriptScoreProcessor, PreProcessorParseContext> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            (a, c) -> new ScriptScoreProcessor((String) a[0], (Script) a[1])
        );
        parser.declareString(ConstructingObjectParser.constructorArg(), OUTPUT_FIELD);
        parser.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> Script.parse(p),
            SCRIPT,
            ObjectParser.ValueType.OBJECT_OR_STRING
        );
        return parser;
    }

    public static ScriptScoreProcessor fromXContentStrict(XContentParser parser, PreProcessorParseContext context) {
        return STRICT_PARSER.apply(parser, context == null ? PreProcessorParseContext.DEFAULT : context);
    }

    public static ScriptScoreProcessor fromXContentLenient(XContentParser parser, PreProcessorParseContext context) {
        return LENIENT_PARSER.apply(parser, context == null ? PreProcessorParseContext.DEFAULT : context);
    }

    private final Script script;
    private final String outputField;

    public ScriptScoreProcessor(String outputField, Script script) {
        this.script = script;
        this.outputField = outputField;
    }

    public ScriptScoreProcessor(StreamInput input) throws IOException {
        this.script = new Script(input);
        this.outputField = input.readString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SCRIPT.getPreferredName(), script);
        builder.field(OUTPUT_FIELD.getPreferredName(), outputField);
        builder.endObject();
        return builder;
    }

    @Override
    public List<String> inputFields() {
        return null;
    }

    @Override
    public List<String> outputFields() {
        return List.of(outputField);
    }

    @Override
    public void process(Map<String, Object> fields) {}

    public Script getScript() {
        return script;
    }

    @Override
    public Map<String, String> reverseLookup() {
        return null;
    }

    @Override
    public boolean isCustom() {
        return true;
    }

    @Override
    public String getOutputFieldType(String outputField) {
        return NumberFieldMapper.NumberType.DOUBLE.typeName();
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
        script.writeTo(out);
        out.writeString(outputField);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    public ScriptScoreFunction compile(SearchExecutionContext executionContext) {
        ScoreScript.Factory factory = executionContext.compile(script, ScoreScript.CONTEXT);
        SearchLookup lookup = executionContext.lookup();
        ScoreScript.LeafFactory searchScript = factory.newFactory(script.getParams(), lookup);
        return new ScriptScoreFunction(script, searchScript, lookup, executionContext.index().getName(), executionContext.getShardId());
    }

}
