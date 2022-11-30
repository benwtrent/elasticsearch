/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.Map;

public interface LeafReaderContextExtractor {
    void resetContext(LeafReaderContext context) throws IOException;

    void extract(int docId) throws IOException;

    void populate(int docId, Map<String, Object> output);

    int expectedFieldOutputSize();
}
