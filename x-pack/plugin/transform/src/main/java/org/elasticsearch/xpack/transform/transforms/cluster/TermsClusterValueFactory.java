/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.cluster;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.xpack.core.transform.transforms.cluster.ClusterValue;
import org.elasticsearch.xpack.core.transform.transforms.cluster.TermValue;

import java.util.Map;

import static org.elasticsearch.xpack.core.transform.transforms.cluster.TermValue.VALUE;

class TermsClusterValueFactory implements ClusterValueFactory {
    @Override
    public ClusterValue build(Object value) {
        return new TermValue(value.toString());
    }

    @Override
    public ClusterValue.Type factoryType() {
        return ClusterValue.Type.TERMS;
    }

    @Override
    public Map<String, String> getIndexMapping(String fieldName) {
        return MapBuilder.<String, String>newMapBuilder()
            .put(fieldName + "." + VALUE.getPreferredName(), KeywordFieldMapper.CONTENT_TYPE)
            .map();
    }
}
