/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.cluster;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.core.transform.transforms.cluster.ClusterValue;
import org.elasticsearch.xpack.core.transform.transforms.cluster.DateValue;

import java.util.Map;

import static org.elasticsearch.xpack.core.transform.transforms.cluster.DateValue.MAX;
import static org.elasticsearch.xpack.core.transform.transforms.cluster.DateValue.MIN;

class DateHistogramClusterValueFactory implements ClusterValueFactory {

    private final long intervalMs;

    DateHistogramClusterValueFactory(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    @Override
    public ClusterValue build(Object value) {
        // TODO do we allow missing??
        if (value == null) {
            return new DateValue(0L, 0L, 1L);
        }
        if (value instanceof Number) {
            return new DateValue((long) value, (long) value, intervalMs);
        }
        throw new IllegalArgumentException(
            "Unexpected type [" + value.getClass() + "] when parsing date histogram cluster value from aggregation"
        );
    }

    @Override
    public ClusterValue.Type factoryType() {
        return ClusterValue.Type.DATE_HISTOGRAM;
    }

    @Override
    public Map<String, String> getIndexMapping(String fieldName) {
        return MapBuilder.<String, String>newMapBuilder()
            .put(fieldName + "." + MAX.getPreferredName(), DateFieldMapper.CONTENT_TYPE)
            .put(fieldName + "." + MIN.getPreferredName(), DateFieldMapper.CONTENT_TYPE)
            .map();
    }
}
