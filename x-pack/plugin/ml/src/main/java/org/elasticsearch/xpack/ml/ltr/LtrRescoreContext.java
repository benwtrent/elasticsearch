/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.ltr;

import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;

public class LtrRescoreContext extends RescoreContext {

    final SearchExecutionContext executionContext;
    final InferenceDefinition inferenceDefinition;

    /**
     * Build the context.
     *
     * @param windowSize
     * @param rescorer   the rescorer actually performing the rescore.
     */
    public LtrRescoreContext(
        int windowSize,
        Rescorer rescorer,
        InferenceDefinition inferenceDefinition,
        SearchExecutionContext executionContext
    ) {
        super(windowSize, rescorer);
        this.executionContext = executionContext;
        this.inferenceDefinition = inferenceDefinition;
    }
}
