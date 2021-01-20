/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.FunctionState;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.cluster.ClusterConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;
import org.elasticsearch.xpack.transform.transforms.cluster.ClusterFunction;
import org.elasticsearch.xpack.transform.transforms.latest.Latest;
import org.elasticsearch.xpack.transform.transforms.pivot.Pivot;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class FunctionFactoryTests extends ESTestCase {

    public void testCreatePivotFunction() {
        TransformConfig config =
            TransformConfigTests.randomTransformConfig(
                randomAlphaOfLengthBetween(1, 10),
                PivotConfigTests.randomPivotConfig(),
                null,
                null);
        Function function = FunctionFactory.create(config, randomBoolean() ? null : new TestFunctionState());
        assertThat(function, is(instanceOf(Pivot.class)));
    }

    public void testCreateLatestFunction() {
        TransformConfig config =
            TransformConfigTests.randomTransformConfig(
                randomAlphaOfLengthBetween(1, 10),
                null,
                LatestConfigTests.randomLatestConfig(),
                null);
        Function function = FunctionFactory.create(config, randomBoolean() ? null : new TestFunctionState());
        assertThat(function, is(instanceOf(Latest.class)));
    }

    public void testCreateClusterFunction() {
        TransformConfig config =
            TransformConfigTests.randomTransformConfig(
                randomAlphaOfLengthBetween(1, 10),
                null,
                null,
                ClusterConfigTests.randomInstance());
        Function function = FunctionFactory.create(config, randomBoolean() ? null : new TestFunctionState());
        assertThat(function, is(instanceOf(ClusterFunction.class)));
    }

    private static class TestFunctionState implements FunctionState {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }

        @Override
        public String getWriteableName() {
            return "test";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }
    }
}
