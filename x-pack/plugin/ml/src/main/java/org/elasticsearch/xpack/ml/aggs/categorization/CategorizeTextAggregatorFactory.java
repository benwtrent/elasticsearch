/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CategorizeTextAggregatorFactory extends AggregatorFactory {

    private final MappedFieldType fieldType;
    private final String indexedFieldName;
    private final int maxChildren;
    private final int maxDepth;
    private final double similarityThreshold;
    private final List<String> categorizationFilters;

    public CategorizeTextAggregatorFactory(String name,
                                           String fieldName,
                                           int maxChildren,
                                           int maxDepth,
                                           double similarityThreshold,
                                           List<String> categorizationFilters,
                                           AggregationContext context,
                                           AggregatorFactory parent,
                                           AggregatorFactories.Builder subFactoriesBuilder,
                                           Map<String, Object> metadata) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metadata);
        this.fieldType = context.getFieldType(fieldName);
        if (fieldType != null) {
            this.indexedFieldName = fieldType.name();
        } else {
            throw new IllegalArgumentException("Only works on indexed fields, cannot find field [" + fieldName + "]");
        }
        this.maxChildren = maxChildren;
        this.maxDepth = maxDepth;
        this.similarityThreshold = similarityThreshold;
        this.categorizationFilters = categorizationFilters == null ? Collections.emptyList() : categorizationFilters;
    }

    @Override
    protected Aggregator createInternal(Aggregator parent,
                                        CardinalityUpperBound cardinality,
                                        Map<String, Object> metadata) throws IOException {

        return new CategorizeTextAggregator(
            name,
            factories,
            context,
            parent,
            indexedFieldName,
            fieldType,
            maxChildren,
            maxDepth,
            similarityThreshold,
            categorizationFilters,
            metadata
        );
    }

}
