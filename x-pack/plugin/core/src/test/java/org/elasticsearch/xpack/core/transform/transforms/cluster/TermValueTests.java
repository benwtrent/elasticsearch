/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.cluster;

import java.io.IOException;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.transforms.AbstractSerializingTransformTestCase;

public class TermValueTests extends AbstractSerializingTransformTestCase<TermValue> {

    static TermValue randomTermValue() {
        return new TermValue(randomAlphaOfLength(10));
    }

    @Override
    protected TermValue doParseInstance(XContentParser parser) throws IOException {
        return TermValue.fromXContent(parser, false);
    }

    @Override
    protected Writeable.Reader<TermValue> instanceReader() {
        return TermValue::new;
    }

    @Override
    protected TermValue createTestInstance() {
        return randomTermValue();
    }
}
