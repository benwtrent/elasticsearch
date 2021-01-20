/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.cluster;

import org.elasticsearch.xpack.core.transform.transforms.cluster.ClusterValue;
import org.elasticsearch.xpack.core.transform.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSource;

import java.util.Map;

interface ClusterValueFactory {
    TermsClusterValueFactory TERMS_CLUSTER_VALUE = new TermsClusterValueFactory();

    ClusterValue build(Object value);

    ClusterValue.Type factoryType();

    Map<String, String> getIndexMapping(String fieldName);

    static ClusterValueFactory factoryFrom(SingleGroupSource singleGroupSource) {
        if (singleGroupSource instanceof TermsGroupSource) {
            return TERMS_CLUSTER_VALUE;
        }
        if (singleGroupSource instanceof DateHistogramGroupSource) {
            return new DateHistogramClusterValueFactory(
                ((DateHistogramGroupSource)singleGroupSource).getInterval().getInterval().estimateMillis()
            );
        }
        throw new IllegalArgumentException("Unable to cluster on group by source [" + singleGroupSource.getType().value() + "]");
    }

}
