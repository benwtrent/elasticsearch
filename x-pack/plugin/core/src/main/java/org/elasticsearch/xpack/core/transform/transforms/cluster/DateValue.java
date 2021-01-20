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

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class DateValue extends ClusterValue {

    public static final String NAME = "date_time";

    private final long min;
    private final long intervalMs;
    private long max;

    public static final ParseField MIN = new ParseField("min");
    public static final ParseField MAX = new ParseField("max");
    private static final ParseField INTERVAL_MS = new ParseField("interval_ms");

    static final ConstructingObjectParser<DateValue, Void> STRICT_PARSER = createParser(false);
    static final ConstructingObjectParser<DateValue, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<DateValue, Void> createParser(boolean lenient) {
        ConstructingObjectParser<DateValue, Void> parser = new ConstructingObjectParser<>(
            NAME,
            lenient,
            args -> new DateValue((long)args[0], (long)args[1], (long)args[2])
        );
        parser.declareLong(constructorArg(), MIN);
        parser.declareLong(constructorArg(), MAX);
        parser.declareLong(constructorArg(), INTERVAL_MS);
        return parser;
    }

    public static DateValue fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return ignoreUnknownFields ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    public DateValue(long min, long max, long intervalMs) {
        if (intervalMs <= 0) {
            throw new IllegalArgumentException(
                "interval for a date clustering value must be greater than 0, provided [" + intervalMs + "]"
            );
        }
        this.min = min;
        this.intervalMs = intervalMs;
        this.max = max;
    }

    public DateValue(StreamInput in) throws IOException {
        this.min = in.readLong();
        this.max = in.readLong();
        this.intervalMs = in.readVLong();
    }

    public long getMin() {
        return min;
    }

    public long getMax() {
        return max;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MIN.getPreferredName(), min);
        builder.field(MAX.getPreferredName(), max);
        builder.field(INTERVAL_MS.getPreferredName(), intervalMs);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(min);
        out.writeLong(max);
        out.writeVLong(intervalMs);
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
        return Type.DATE_HISTOGRAM;
    }

    @Override
    public Map<String, Object> asMap() {
        return MapBuilder.<String, Object>newMapBuilder()
            .put(MAX.getPreferredName(), max)
            .put(MIN.getPreferredName(), min)
            .map();
    }

    @Override
    void merge(ClusterValue clusterValue) {
        if (clusterValue instanceof DateValue) {
            DateValue dateValue = (DateValue) clusterValue;
            assert dateValue.intervalMs == this.intervalMs;
            this.max = Math.max(dateValue.max, max);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateValue dateValue = (DateValue) o;
        return min == dateValue.min && max == dateValue.max && intervalMs == dateValue.intervalMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max, intervalMs);
    }

    @Override
    public String toString() {
        return "DateValue{" +
            "min=" + min +
            ", max=" + max +
            ", interval_ms=" + intervalMs +
            '}';
    }

    @Override
    boolean shouldPrune(ClusterValue clusterValue) {
        assert clusterValue instanceof DateValue;
        if (clusterValue instanceof DateValue) {
            DateValue dateValue = (DateValue) clusterValue;
            assert dateValue.intervalMs == this.intervalMs;
            return this.max + intervalMs < dateValue.min;
        }
        return false;
    }

}
