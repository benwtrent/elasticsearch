/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.cluster;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.transforms.AbstractSerializingTransformTestCase;

import java.io.IOException;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.transform.transforms.cluster.DateValueTests.randomDateValue;
import static org.elasticsearch.xpack.core.transform.transforms.cluster.TermValueTests.randomTermValue;

public class ClusterObjectTests extends AbstractSerializingTransformTestCase<ClusterObject> {

    static ClusterObject randomInstance() {
        return randomInstance(
            randomAlphaOfLength(10),
            Stream.generate(() -> randomFrom(randomTermValue(), randomDateValue()))
                .limit(randomLongBetween(2, 10))
                .toArray(ClusterValue[]::new));
    }

    static ClusterObject randomInstance(String id, ClusterValue[] values) {
        return new ClusterObject(
            id,
            randomLongBetween(0L, 100_000_000_000L),
            values);
    }

    @Override
    protected ClusterObject doParseInstance(XContentParser parser) throws IOException {
        return ClusterObject.fromXContent(parser, false);
    }

    @Override
    protected Writeable.Reader<ClusterObject> instanceReader() {
        return ClusterObject::new;
    }

    @Override
    protected ClusterObject createTestInstance() {
        return randomInstance();
    }
}
