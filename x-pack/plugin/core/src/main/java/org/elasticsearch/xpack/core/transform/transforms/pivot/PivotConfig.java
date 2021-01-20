/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.transform.utils.TransformStrings.detectDuplicateDotDelimitedPaths;

public class PivotConfig implements Writeable, ToXContentObject {

    private static final String NAME = "data_frame_transform_pivot";
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(PivotConfig.class);

    private final GroupConfig groups;
    private final AggregationConfig aggregationConfig;
    private final Integer maxPageSearchSize;

    private static final ConstructingObjectParser<PivotConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<PivotConfig, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<PivotConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<PivotConfig, Void> parser = new ConstructingObjectParser<>(NAME, lenient, args -> {
            GroupConfig groups = (GroupConfig) args[0];

            // allow "aggs" and "aggregations" but require one to be specified
            // if somebody specifies both: throw
            AggregationConfig aggregationConfig = null;
            if (args[1] != null) {
                aggregationConfig = (AggregationConfig) args[1];
            }

            if (args[2] != null) {
                if (aggregationConfig != null) {
                    throw new IllegalArgumentException("Found two aggregation definitions: [aggs] and [aggregations]");
                }
                aggregationConfig = (AggregationConfig) args[2];
            }
            if (aggregationConfig == null) {
                throw new IllegalArgumentException("Required [aggregations]");
            }

            return new PivotConfig(groups, aggregationConfig, (Integer) args[3]);
        });

        parser.declareObject(constructorArg(), (p, c) -> (GroupConfig.fromXContent(p, lenient)), TransformField.GROUP_BY);

        parser.declareObject(optionalConstructorArg(), (p, c) -> AggregationConfig.fromXContent(p, lenient), TransformField.AGGREGATIONS);
        parser.declareObject(optionalConstructorArg(), (p, c) -> AggregationConfig.fromXContent(p, lenient), TransformField.AGGS);
        parser.declareInt(optionalConstructorArg(), TransformField.MAX_PAGE_SEARCH_SIZE);

        return parser;
    }

    public PivotConfig(final GroupConfig groups, final AggregationConfig aggregationConfig, Integer maxPageSearchSize) {
        this.groups = ExceptionsHelper.requireNonNull(groups, TransformField.GROUP_BY.getPreferredName());
        this.aggregationConfig = ExceptionsHelper.requireNonNull(aggregationConfig, TransformField.AGGREGATIONS.getPreferredName());
        this.maxPageSearchSize = maxPageSearchSize;

        if (maxPageSearchSize != null) {
            deprecationLogger.deprecate(
                DeprecationCategory.API,
                TransformField.MAX_PAGE_SEARCH_SIZE.getPreferredName(),
                "[max_page_search_size] is deprecated inside pivot please use settings instead"
            );
        }
    }

    public PivotConfig(StreamInput in) throws IOException {
        this.groups = new GroupConfig(in);
        this.aggregationConfig = new AggregationConfig(in);
        this.maxPageSearchSize = in.readOptionalInt();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TransformField.GROUP_BY.getPreferredName(), groups);
        builder.field(TransformField.AGGREGATIONS.getPreferredName(), aggregationConfig);
        if (maxPageSearchSize != null) {
            builder.field(TransformField.MAX_PAGE_SEARCH_SIZE.getPreferredName(), maxPageSearchSize);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        groups.writeTo(out);
        aggregationConfig.writeTo(out);
        out.writeOptionalInt(maxPageSearchSize);
    }

    public AggregationConfig getAggregationConfig() {
        return aggregationConfig;
    }

    public GroupConfig getGroupConfig() {
        return groups;
    }

    @Nullable
    public Integer getMaxPageSearchSize() {
        return maxPageSearchSize;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final PivotConfig that = (PivotConfig) other;

        return Objects.equals(this.groups, that.groups)
            && Objects.equals(this.aggregationConfig, that.aggregationConfig)
            && Objects.equals(this.maxPageSearchSize, that.maxPageSearchSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groups, aggregationConfig, maxPageSearchSize);
    }

    public boolean isValid() {
        return groups.isValid() && aggregationConfig.isValid();
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (maxPageSearchSize != null && (maxPageSearchSize < 10 || maxPageSearchSize > 10_000)) {
            validationException = addValidationError(
                "pivot.max_page_search_size [" + maxPageSearchSize + "] must be greater than 10 and less than 10,000",
                validationException
            );
        }

        for (String failure : aggFieldValidation()) {
            validationException = addValidationError(failure, validationException);
        }

        return validationException;
    }

    public List<String> aggFieldValidation() {
        if ((aggregationConfig.isValid() && groups.isValid()) == false) {
            return Collections.emptyList();
        }
        List<String> usedNames = new ArrayList<>();
        aggregationConfig.getAggregatorFactories().forEach(agg -> addAggNames("", agg, usedNames));
        aggregationConfig.getPipelineAggregatorFactories().forEach(agg -> addAggNames("", agg, usedNames));
        usedNames.addAll(groups.getGroups().keySet());
        return detectDuplicateDotDelimitedPaths(usedNames);
    }

    public static PivotConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private static void addAggNames(String namePrefix, AggregationBuilder aggregationBuilder, Collection<String> names) {
        if (aggregationBuilder.getSubAggregations().isEmpty() && aggregationBuilder.getPipelineAggregations().isEmpty()) {
            names.add(namePrefix + aggregationBuilder.getName());
            return;
        }

        String newNamePrefix = namePrefix + aggregationBuilder.getName() + ".";
        aggregationBuilder.getSubAggregations().forEach(agg -> addAggNames(newNamePrefix, agg, names));
        aggregationBuilder.getPipelineAggregations().forEach(agg -> addAggNames(newNamePrefix, agg, names));
    }

    private static void addAggNames(String namePrefix, PipelineAggregationBuilder pipelineAggregationBuilder, Collection<String> names) {
        names.add(namePrefix + pipelineAggregationBuilder.getName());
    }
}
