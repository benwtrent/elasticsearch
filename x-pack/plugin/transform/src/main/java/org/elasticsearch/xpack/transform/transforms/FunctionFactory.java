/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.core.transform.transforms.FunctionState;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.transform.transforms.cluster.ClusterFunction;
import org.elasticsearch.xpack.transform.transforms.latest.Latest;
import org.elasticsearch.xpack.transform.transforms.pivot.Pivot;

/**
 * Factory for creating the runtime instance for a function given the configuration
 */
public final class FunctionFactory {

    private FunctionFactory() {}

    /**
     * Creates the function instance given the transform configuration
     *
     * @param config the transform configuration
     * @param functionState the state to pass to the created function. Can be null.
     * @return the instance of the function
     */
    public static Function create(TransformConfig config, @Nullable FunctionState functionState) {
        if (config.getPivotConfig() != null) {
            return new Pivot(config.getPivotConfig(), config.getSettings(), config.getVersion());
        } else if (config.getLatestConfig() != null) {
            return new Latest(config.getLatestConfig());
        } else if (config.getClusterConfig() != null) {
            return new ClusterFunction(config.getClusterConfig(), config.getSettings(), config.getVersion(), functionState);
        } else {
            throw new IllegalArgumentException("unknown transform function");
        }
    }

}
