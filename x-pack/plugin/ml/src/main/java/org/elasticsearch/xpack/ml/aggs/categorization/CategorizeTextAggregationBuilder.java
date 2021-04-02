/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CategorizeTextAggregationBuilder extends AbstractAggregationBuilder<CategorizeTextAggregationBuilder> {

    public static final String NAME = "categorize_text";

    static final ParseField FIELD_NAME = new ParseField("field");
    static final ParseField MAX_CHILDREN = new ParseField("max_children");
    static final ParseField SIMILARITY_THRESHOLD = new ParseField("similarity_threshold");
    static final ParseField MAX_DEPTH = new ParseField("max_depth");
    static final ParseField CATEGORIZATION_FILTERS = new ParseField("categorization_filters");

    public static final ObjectParser<CategorizeTextAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        CategorizeTextAggregationBuilder.NAME,
        CategorizeTextAggregationBuilder::new
    );
    static {
        PARSER.declareString(CategorizeTextAggregationBuilder::setFieldName, FIELD_NAME);
        PARSER.declareInt(CategorizeTextAggregationBuilder::setMaxChildren, MAX_CHILDREN);
        PARSER.declareInt(CategorizeTextAggregationBuilder::setMaxDepth, MAX_DEPTH);
        PARSER.declareDouble(CategorizeTextAggregationBuilder::setSimilarityThreshold, SIMILARITY_THRESHOLD);
        PARSER.declareStringArray(CategorizeTextAggregationBuilder::setCategorizationFilters, CATEGORIZATION_FILTERS);
    }
    public static CategorizeTextAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new CategorizeTextAggregationBuilder(aggregationName), null);
    }

    private List<String> categorizationFilters = new ArrayList<>();
    private String fieldName;
    private int maxChildren = 100;
    private double similarityThreshold = 0.5;
    private int maxDepth = 5;

    public CategorizeTextAggregationBuilder(String name) {
        super(name);
    }

    public String getFieldName() {
        return fieldName;
    }

    public CategorizeTextAggregationBuilder setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public CategorizeTextAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.maxChildren = in.readVInt();
        this.maxDepth = in.readVInt();
        this.similarityThreshold = in.readDouble();
        this.categorizationFilters = in.readStringList();
    }

    public int getMaxChildren() {
        return maxChildren;
    }

    public CategorizeTextAggregationBuilder setMaxChildren(int maxChildren) {
        this.maxChildren = maxChildren;
        return this;
    }

    public double getSimilarityThreshold() {
        return similarityThreshold;
    }

    public CategorizeTextAggregationBuilder setSimilarityThreshold(double similarityThreshold) {
        this.similarityThreshold = similarityThreshold;
        return this;
    }

    public List<String> getCategorizationFilters() {
        return categorizationFilters;
    }

    public CategorizeTextAggregationBuilder setCategorizationFilters(List<String> categorizationFilters) {
        this.categorizationFilters = ExceptionsHelper.requireNonNull(categorizationFilters, CATEGORIZATION_FILTERS);
        return this;
    }

    public int getMaxDepth() {
        return maxDepth;
    }

    public CategorizeTextAggregationBuilder setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
        return this;
    }

    protected CategorizeTextAggregationBuilder(CategorizeTextAggregationBuilder clone,
                                               AggregatorFactories.Builder factoriesBuilder,
                                               Map<String, Object> metadata)  {
        super(clone, factoriesBuilder, metadata);
        this.fieldName = clone.fieldName;
        this.maxChildren = clone.maxChildren;
        this.maxDepth = clone.maxDepth;
        this.similarityThreshold = clone.similarityThreshold;
        this.categorizationFilters = clone.categorizationFilters;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeVInt(maxChildren);
        out.writeVInt(maxDepth);
        out.writeDouble(similarityThreshold);
        out.writeStringCollection(categorizationFilters);
    }

    @Override
    protected AggregatorFactory doBuild(AggregationContext context,
                                        AggregatorFactory parent,
                                        AggregatorFactories.Builder subfactoriesBuilder) throws IOException {
        return new CategorizeTextAggregatorFactory(
            name,
            fieldName,
            maxChildren,
            maxDepth,
            similarityThreshold,
            categorizationFilters,
            context,
            parent,
            subfactoriesBuilder,
            metadata
        );
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_NAME.getPreferredName(), fieldName);
        builder.field(MAX_CHILDREN.getPreferredName(), maxChildren);
        builder.field(MAX_DEPTH.getPreferredName(), maxDepth);
        builder.field(SIMILARITY_THRESHOLD.getPreferredName(), similarityThreshold);
        if (categorizationFilters.isEmpty() == false) {
            builder.field(CATEGORIZATION_FILTERS.getPreferredName(), categorizationFilters);
        }
        builder.endObject();
        return null;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new CategorizeTextAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    public String getType() {
        return NAME;
    }
}
