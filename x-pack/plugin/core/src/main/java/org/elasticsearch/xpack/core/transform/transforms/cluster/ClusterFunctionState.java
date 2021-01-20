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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.transforms.FunctionState;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class ClusterFunctionState implements FunctionState {

    public static final String NAME = "cluster_transform_function_state";
    private static final ParseField CLUSTERS = new ParseField("clusters");
    private static final ParseField CLUSTERING_FIELD_NAMES = new ParseField("clustering_field_names");

    private static final ConstructingObjectParser<ClusterFunctionState, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<ClusterFunctionState, Void> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<ClusterFunctionState, Void> createParser(boolean lenient) {
        ConstructingObjectParser<ClusterFunctionState, Void> parser = new ConstructingObjectParser<>(
            "transform_cluster",
            lenient,
            args -> new ClusterFunctionState(
                (List<ClusterObject>) args[0],
                ((List<ClusterFieldTypeAndName>) args[1]).toArray(ClusterFieldTypeAndName[]::new)
            )
        );
        parser.declareObjectArray(constructorArg(), (p, c) -> ClusterObject.fromXContent(p, lenient), CLUSTERS);
        parser.declareObjectArray(constructorArg(), (p, c) -> ClusterFieldTypeAndName.fromXContent(p, lenient), CLUSTERING_FIELD_NAMES);
        return parser;
    }

    public static ClusterFunctionState fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return ignoreUnknownFields ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    public static ClusterFunctionState empty(ClusterFieldTypeAndName... clusterFieldNames) {
        return new ClusterFunctionState(new LinkedHashMap<>(), clusterFieldNames);
    }

    private final Map<String, ClusterObject> clusters;
    private final ClusterFieldTypeAndName[] clusterFieldNames;

    // TODO, better cluster data structure
    private final Map<Integer, String> termsToClusterId = new HashMap<>();

    private ClusterFunctionState(List<ClusterObject> clusters, ClusterFieldTypeAndName[] clusterFieldNames) {
        this.clusters = clusters.stream().collect(Collectors.toMap(ClusterObject::getId, c -> c, (v, v1) -> v, LinkedHashMap::new));
        this.clusterFieldNames = clusterFieldNames;
        for (ClusterObject clusterObject : this.clusters.values()) {
            termsToClusterId.put(termHash(clusterObject.getClusterValues()), clusterObject.getId());
        }
    }

    ClusterFunctionState(Map<String, ClusterObject> clusterIdMap, ClusterFieldTypeAndName[] clusterFieldNames) {
        this.clusters = clusterIdMap;
        if (Arrays
            .stream(clusterFieldNames)
            .map(ClusterFieldTypeAndName::getFieldName)
            .collect(Collectors.toSet())
            .size() < clusterFieldNames.length) {
            throw new IllegalArgumentException("The field names on which to cluster must be unique");
        }
        this.clusterFieldNames = clusterFieldNames;
        for (ClusterObject clusterObject : this.clusters.values()) {
            termsToClusterId.put(termHash(clusterObject.getClusterValues()), clusterObject.getId());
        }
    }

    public ClusterFunctionState(StreamInput in) throws IOException {
        this.clusters = in.readList(ClusterObject::new)
            .stream()
            .collect(Collectors.toMap(ClusterObject::getId, c -> c, (v, v1) -> v, LinkedHashMap::new));
        this.clusterFieldNames = in.readList(ClusterFieldTypeAndName::new).toArray(ClusterFieldTypeAndName[]::new);
        for (ClusterObject clusterObject : this.clusters.values()) {
            termsToClusterId.put(termHash(clusterObject.getClusterValues()), clusterObject.getId());
        }
    }

    /**
     * This adds new fields to the appropriate cluster. Creating a new one if necessary.
     *
     * @param fields The cluster value fields
     * @param count The count of values that match the fields
     * @param idCreator A function to create a unique cluster ID given the provided fields
     * @return The id of the new or updated cluster.
     */
    public String putOrUpdateCluster(Map<String, ClusterValue> fields, long count, Function<Map<String, ClusterValue>, String> idCreator) {
        ClusterValue[] clusterValues = Arrays.stream(clusterFieldNames).map(cfn -> fields.get(cfn.fieldName)).toArray(ClusterValue[]::new);
        int termsHash = termHash(clusterValues);
        String clusterId = termsToClusterId.get(termsHash);
        if (clusterId != null) {
            clusters.get(clusterId).addValues(clusterValues, count);
            return clusterId;
        } else {
            ClusterObject newObject = new ClusterObject(idCreator.apply(fields), count, clusterValues);
            clusters.put(newObject.getId(), newObject);
            termsToClusterId.put(termsHash, newObject.getId());
            return newObject.getId();
        }
    }

    /**
     * This gets the current cluster as a string object map that works for indexing.
     * @param clusterId The cluster ID to get
     * @param dateAsEpochMilli whether or not the date values should be epoch milli or not
     * @return An index ready string object map
     */
    public Map<String, Object> clusterAsMap(String clusterId, boolean dateAsEpochMilli) {
        return clusters.get(clusterId).asMap(clusterFieldNames);
    }

    /**
     * This prunes down the current clusters to only those that will be applicable to future value sets.
     *
     * @param lastValueSet The value from which to determine if future values will be able to use current clusters in memory
     */
    public void pruneClusters(Map<String, ClusterValue> lastValueSet) {
        ClusterValue[] clusterValues = Arrays.stream(clusterFieldNames)
            .map(cfn -> lastValueSet.get(cfn.fieldName))
            .toArray(ClusterValue[]::new);
        List<String> toPrune = new ArrayList<>();
        for (ClusterObject clusterObject : clusters.values()) {
            if (clusterObject.shouldPrune(clusterValues)) {
                toPrune.add(clusterObject.getId());
            }
        }
        for (String clusterId : toPrune) {
            ClusterObject clusterObject = clusters.remove(clusterId);
            termsToClusterId.remove(termHash(clusterObject.getClusterValues()));
        }
    }

    ClusterObject getCluster(String clusterId) {
        return clusters.get(clusterId);
    }

    ClusterFieldTypeAndName[] getClusterFieldNames() {
        return clusterFieldNames;
    }

    private static int termHash(ClusterValue[] values) {
        int result = 1;
        for (ClusterValue clusterValue : values) {
            if (clusterValue instanceof TermValue) {
                TermValue termValue = (TermValue) clusterValue;
                result = 31 * result + (termValue.getValue() == null ? 0 : termValue.getValue().hashCode());
            }
        }
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLUSTERS.getPreferredName(), clusters.values());
        builder.field(CLUSTERING_FIELD_NAMES.getPreferredName(), clusterFieldNames);
        builder.endObject();
        return null;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(clusters.values());
        out.writeList(Arrays.asList(clusterFieldNames));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterFunctionState that = (ClusterFunctionState) o;
        return Objects.equals(clusters, that.clusters) && Arrays.equals(clusterFieldNames, that.clusterFieldNames);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(clusters);
        result = 31 * result + Arrays.hashCode(clusterFieldNames);
        return result;
    }

    public static class ClusterFieldTypeAndName implements ToXContentObject, Writeable {

        private static final ParseField FIELD_TYPE = new ParseField("field_type");
        private static final ParseField FIELD_NAME = new ParseField("field_name");
        private static final ConstructingObjectParser<ClusterFieldTypeAndName, Void> STRICT_PARSER = createParser(false);
        private static final ConstructingObjectParser<ClusterFieldTypeAndName, Void> LENIENT_PARSER = createParser(true);

        private static ConstructingObjectParser<ClusterFieldTypeAndName, Void> createParser(boolean lenient) {
            ConstructingObjectParser<ClusterFieldTypeAndName, Void> parser = new ConstructingObjectParser<>(
                "transform_cluster_field_name_type",
                lenient,
                args -> new ClusterFieldTypeAndName(
                    (String) args[0],
                    ClusterValue.Type.fromString((String)args[1])
                )
            );
            parser.declareString(constructorArg(), FIELD_NAME);
            parser.declareString(constructorArg(), FIELD_TYPE);
            return parser;
        }

        public static ClusterFieldTypeAndName fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
            return ignoreUnknownFields ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
        }

        private final String fieldName;
        private final ClusterValue.Type type;

        public ClusterFieldTypeAndName(String fieldName, ClusterValue.Type type) {
            this.fieldName = ExceptionsHelper.requireNonNull(fieldName, FIELD_NAME.getPreferredName());
            this.type = ExceptionsHelper.requireNonNull(type, FIELD_TYPE.getPreferredName());
        }

        ClusterFieldTypeAndName(StreamInput input) throws IOException {
            this.fieldName = input.readString();
            this.type = input.readEnum(ClusterValue.Type.class);
        }

        public String getFieldName() {
            return fieldName;
        }

        public ClusterValue.Type getType() {
            return type;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FIELD_NAME.getPreferredName(), fieldName);
            builder.field(FIELD_TYPE.getPreferredName(), type.value());
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(fieldName);
            out.writeEnum(type);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClusterFieldTypeAndName that = (ClusterFieldTypeAndName) o;
            return Objects.equals(fieldName, that.fieldName) && type == that.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, type);
        }


    }
}
