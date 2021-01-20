/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.cluster;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.core.transform.utils.TransformStrings.detectDuplicateDotDelimitedPaths;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

public class ClusterConfig implements Writeable, ToXContentObject {

    private static final String NAME = "cluster_config";

    private final GroupConfig groups;

    private static final ConstructingObjectParser<ClusterConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<ClusterConfig, Void> LENIENT_PARSER = createParser(true);

    // We need guaranteed composite agg order, that way we know once we reach certain buckets
    // clusters can be aged out
    private static GroupConfig sortGroups(GroupConfig groupConfig) {
        Map<String, SingleGroupSource> orderedGroups = new LinkedHashMap<>();
        List<Entry<String, SingleGroupSource>> dateHistogramGroups = new ArrayList<>();
        List<Entry<String, SingleGroupSource>> termGroups = new ArrayList<>();
        for (Entry<String, SingleGroupSource> groupSourceEntry : groupConfig.getGroups().entrySet()) {
            switch (groupSourceEntry.getValue().getType()) {
                case TERMS:
                    termGroups.add(groupSourceEntry);
                    break;
                case DATE_HISTOGRAM:
                    dateHistogramGroups.add(groupSourceEntry);
                    break;
                default:
                    throw new IllegalArgumentException(
                        "clustering does not support group by type [" + groupSourceEntry.getValue().getType().value() +"]"
                    );
            }
        }
        dateHistogramGroups.sort(Entry.comparingByKey());
        termGroups.sort(Entry.comparingByKey());
        dateHistogramGroups.forEach(e -> orderedGroups.put(e.getKey(), e.getValue()));
        termGroups.forEach(e -> orderedGroups.put(e.getKey(), e.getValue()));
        return GroupConfig.fromGroups(orderedGroups);
    }

    private static ConstructingObjectParser<ClusterConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<ClusterConfig, Void> parser = new ConstructingObjectParser<>(
            NAME,
            lenient,
            args -> new ClusterConfig((GroupConfig) args[0])
        );
        parser.declareObject(constructorArg(), (p, c) -> (GroupConfig.fromXContent(p, lenient)), TransformField.GROUP_BY);
        return parser;
    }

    public ClusterConfig(final GroupConfig groups) {
        if (groups.getGroups().size() < 2) {
            throw new IllegalArgumentException("group_by must contain at least 2 groupings for clustering");
        }
        int date_histogram_count = 0;
        for (SingleGroupSource singleGroupSource : groups.getGroups().values()) {
            if (singleGroupSource instanceof DateHistogramGroupSource) {
                date_histogram_count++;
                if (singleGroupSource.getMissingBucket()) {
                    throw new IllegalArgumentException("cluster does not support missing_bucket in date_histogram group_by");
                }
            }
        }
        if (date_histogram_count != 1) {
            throw new IllegalArgumentException("group_by must contain exactly 1 date_histogram grouping for clustering");
        }
        this.groups = sortGroups(ExceptionsHelper.requireNonNull(groups, TransformField.GROUP_BY.getPreferredName()));
    }

    public ClusterConfig(StreamInput in) throws IOException {
        this.groups = sortGroups(new GroupConfig(in));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TransformField.GROUP_BY.getPreferredName(), groups);
        builder.endObject();
        return builder;
    }

    public void toCompositeAggXContent(XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(CompositeAggregationBuilder.SOURCES_FIELD_NAME.getPreferredName());
        builder.startArray();

        for (Entry<String, SingleGroupSource> groupBy : groups.getGroups().entrySet()) {
            builder.startObject();
            builder.startObject(groupBy.getKey());
            builder.field(groupBy.getValue().getType().value(), groupBy.getValue());
            builder.endObject();
            builder.endObject();
        }

        builder.endArray();
        builder.endObject(); // sources
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        groups.writeTo(out);
    }

    public GroupConfig getGroupConfig() {
        return groups;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final ClusterConfig that = (ClusterConfig) other;

        return Objects.equals(this.groups, that.groups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groups);
    }

    public boolean isValid() {
        return groups.isValid();
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        for (String failure : aggFieldValidation()) {
            validationException = addValidationError(failure, validationException);
        }

        return validationException;
    }

    public List<String> aggFieldValidation() {
        if (groups.isValid() == false) {
            return Collections.emptyList();
        }
        List<String> usedNames = new ArrayList<>(groups.getGroups().keySet());
        return detectDuplicateDotDelimitedPaths(usedNames);
    }

    public static ClusterConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

}
