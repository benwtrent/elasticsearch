/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.cluster;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class TermValue extends ClusterValue {
    public static final String NAME = "term";

    public static final ParseField VALUE = new ParseField("value");

    static final ConstructingObjectParser<TermValue, Void> STRICT_PARSER = createParser(false);
    static final ConstructingObjectParser<TermValue, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<TermValue, Void> createParser(boolean lenient) {
        ConstructingObjectParser<TermValue, Void> parser = new ConstructingObjectParser<>(
            NAME,
            lenient,
            args -> new TermValue((String)args[0])
        );
        parser.declareString(constructorArg(), VALUE);
        return parser;
    }

    public static TermValue fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return ignoreUnknownFields ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private final String value;

    public TermValue(String value) {
        this.value = ExceptionsHelper.requireNonNull(value, VALUE.getPreferredName());
    }

    public TermValue(StreamInput in) throws IOException {
        this.value = in.readString();
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TermValue termValue = (TermValue) o;
        return Objects.equals(value, termValue.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VALUE.getPreferredName(), value);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(value);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Type getType() {
        return Type.TERMS;
    }

    @Override
    public Map<String, Object> asMap() {
        return MapBuilder.<String, Object>newMapBuilder()
            .put(VALUE.getPreferredName(), value)
            .map();
    }

    @Override
    void merge(ClusterValue clusterValue) {
        if (clusterValue instanceof TermValue) {
            // Right now, don't need to do anything
        }
    }

    @Override
    public String toString() {
        return "TermValue{" +
            "value='" + value + '\'' +
            '}';
    }
}
