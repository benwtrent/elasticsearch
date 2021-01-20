/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.cluster;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;



public class ClusterObject implements ToXContentObject, Writeable {

    public static final ParseField CLUSTER_ID = new ParseField("cluster_id");
    public static final ParseField COUNT = new ParseField("count");
    private static final ParseField FIELD_VALUES = new ParseField("field_values");

    private static final ObjectParser<ClusterObject.Builder, Void> STRICT_PARSER = createParser(false);
    private static final ObjectParser<ClusterObject.Builder, Void> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ObjectParser<Builder, Void> createParser(boolean lenient) {
        ObjectParser<ClusterObject.Builder, Void> parser = new ObjectParser<>(
            "transform_cluster_object",
            lenient,
            ClusterObject.Builder::new
        );
        parser.declareString(Builder::setId, CLUSTER_ID);
        parser.declareLong(Builder::setCount, COUNT);
        parser.declareNamedObjects(
            Builder::setClusterValues,
            (p, c, n) -> p.namedObject(ClusterValue.class, n, lenient),
            (c) -> c.setInOrder(true),
            FIELD_VALUES
        );
        return parser;
    }

    public static ClusterObject fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return ignoreUnknownFields ? LENIENT_PARSER.apply(parser, null).build() : STRICT_PARSER.apply(parser, null).build();
    }

    private final String id;
    private final ClusterValue[] clusterValues;
    private long count;

    ClusterObject(String id, long count, ClusterValue[] clusterValues) {
        this.id = ExceptionsHelper.requireNonNull(id, CLUSTER_ID.getPreferredName());
        if (count < 0) {
            throw new IllegalArgumentException("[count] must be greater than or equal to 0");
        }
        this.count = count;
        this.clusterValues = clusterValues;
    }

    public ClusterObject(StreamInput in) throws  IOException {
        this.id = in.readString();
        this.count = in.readVLong();
        this.clusterValues = in.readNamedWriteableList(ClusterValue.class).toArray(ClusterValue[]::new);
    }

    void addValues(ClusterValue[] value, long count) {
        assert count > 0: "count can only be incremented by a non-negative value";
        assert value.length == clusterValues.length : "newly clustered value must have same dimensions as cluster";
        this.count += count;
        IntStream.range(0, value.length).forEach(i -> {
            assert value[i].getType() == clusterValues[i].getType() : "clustered value type mismatch";
            this.clusterValues[i].merge(value[i]);
        });
    }

    boolean shouldPrune(ClusterValue[] value) {
        assert value.length == this.clusterValues.length;
        for (int i = 0; i < value.length; i++) {
            assert value[i].getType() == clusterValues[i].getType() : "clustered value type mismatch";
            if (this.clusterValues[i].shouldPrune(value[i])) {
                return true;
            }
        }
        return false;
    }

    ClusterValue[] getClusterValues() {
        return clusterValues;
    }

    public String getId() {
        return id;
    }

    long getCount() {
        return count;
    }

    Map<String, Object> asMap(ClusterFunctionState.ClusterFieldTypeAndName[] fieldNames) {
        assert fieldNames.length == clusterValues.length;
        Map<String, Object> map = new HashMap<>();
        map.put(CLUSTER_ID.getPreferredName(), id);
        map.put(COUNT.getPreferredName(), count);
        IntStream.range(0, fieldNames.length)
            .forEach(i -> map.put(fieldNames[i].getFieldName(), clusterValues[i].asMap()));
        return map;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLUSTER_ID.getPreferredName(), id);
        builder.field(COUNT.getPreferredName(), count);
        builder.startArray(FIELD_VALUES.getPreferredName());
        for (ClusterValue clusterValue : clusterValues) {
            builder.startObject();
            builder.field(clusterValue.getName(), clusterValue);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeVLong(count);
        out.writeNamedWriteableList(Arrays.asList(clusterValues));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterObject cluster = (ClusterObject) o;
        return count == cluster.count && Objects.equals(id, cluster.id) && Arrays.equals(clusterValues, cluster.clusterValues);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, count);
        result = 31 * result + Arrays.hashCode(clusterValues);
        return result;
    }

    @Override
    public String toString() {
        return "Cluster{" +
            "id='" + id + '\'' +
            ", count=" + count +
            ", clusterValues=" + Arrays.toString(clusterValues) +
            '}';
    }

    static class Builder {
        private String id;
        private long count;
        private List<ClusterValue> clusterValues;
        private boolean inOrder = false;

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        public Builder setCount(long count) {
            this.count = count;
            return this;
        }

        public Builder setClusterValues(List<ClusterValue> clusterValues) {
            this.clusterValues = clusterValues;
            return this;
        }

        public Builder setInOrder(boolean inOrder) {
            this.inOrder = inOrder;
            return this;
        }

        ClusterObject build() {
            if (inOrder == false) {
                throw new IllegalArgumentException("cluster values must have guaranteed order");
            }
            return new ClusterObject(id, count, clusterValues.toArray(new ClusterValue[0]));
        }
    }
}
