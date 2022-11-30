/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;

public interface LeafReaderContextExtractorFactory {
    LeafReaderContextExtractor createLeafReaderExtractor(SearchExecutionContext executionContext) throws IOException;
}
