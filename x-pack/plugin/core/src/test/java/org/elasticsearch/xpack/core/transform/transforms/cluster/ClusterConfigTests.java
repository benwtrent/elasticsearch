/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.cluster;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.transforms.AbstractSerializingTransformTestCase;
import org.elasticsearch.xpack.core.transform.transforms.pivot.DateHistogramGroupSourceTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSourceTests;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;
import static org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfigTests.getSource;
import static org.hamcrest.Matchers.equalTo;

public class ClusterConfigTests extends AbstractSerializingTransformTestCase<ClusterConfig> {

    public static ClusterConfig randomInstance() {
        return new ClusterConfig(randomGroupConfig());
    }

    private static GroupConfig randomGroupConfig() {
        Map<String, Object> source = new LinkedHashMap<>();
        Map<String, SingleGroupSource> groups = new LinkedHashMap<>();

        // ensure that the unlikely does not happen: 2 group_by's share the same name
        Set<String> names = new HashSet<>();
        String dateHistogramTargetFieldName = randomAlphaOfLengthBetween(1, 20);
        names.add(dateHistogramTargetFieldName);
        SingleGroupSource dateHistogramGroupSource = DateHistogramGroupSourceTests.randomDateHistogramGroupSource(Version.CURRENT, false);
        source.put(dateHistogramTargetFieldName,
            Collections.singletonMap(dateHistogramGroupSource.getType().value(), getSource(dateHistogramGroupSource)));
        groups.put(dateHistogramTargetFieldName, dateHistogramGroupSource);

        for (int i = 0; i < randomIntBetween(1, 20); ++i) {
            String targetFieldName = randomAlphaOfLengthBetween(1, 20);
            if (names.add(targetFieldName)) {
                SingleGroupSource groupBy = TermsGroupSourceTests.randomTermsGroupSource(Version.CURRENT);
                source.put(targetFieldName, Collections.singletonMap(groupBy.getType().value(), getSource(groupBy)));
                groups.put(targetFieldName, groupBy);
            }
        }

        return new GroupConfig(source, groups);
    }

    public void testEnsureGroupByOrder() throws IOException {
        xContentTester(this::createParser, this::createXContextTestInstance, getToXContentParams(), this::doParseInstance)
            .numberOfTestRuns(NUMBER_OF_TEST_RUNS)
            .supportsUnknownFields(false)
            .shuffleFieldsExceptions(getShuffleFieldsExceptions())
            .assertEqualsConsumer((original, serialized) -> {
                GroupConfig originalGroupConfig = original.getGroupConfig();
                GroupConfig serializedGroupConfig = serialized.getGroupConfig();
                assertThat(originalGroupConfig.getGroups().size(), equalTo(serializedGroupConfig.getGroups().size()));
                Iterator<Map.Entry<String, SingleGroupSource>> iteratorA = originalGroupConfig.getGroups().entrySet().iterator();
                Iterator<Map.Entry<String, SingleGroupSource>> iteratorB = serializedGroupConfig.getGroups().entrySet().iterator();
                while (iteratorA.hasNext() && iteratorB.hasNext()) {
                    Map.Entry<String, SingleGroupSource> a = iteratorA.next();
                    Map.Entry<String, SingleGroupSource> b = iteratorB.next();
                    assertThat(a.getKey(), equalTo(b.getKey()));
                    assertThat(a.getValue(), equalTo(b.getValue()));
                }
            })
            .assertToXContentEquivalence(false)
            .test();
    }

    @Override
    protected ClusterConfig doParseInstance(XContentParser parser) throws IOException {
        return ClusterConfig.fromXContent(parser, true);
    }

    @Override
    protected Reader<ClusterConfig> instanceReader() {
        return ClusterConfig::new;
    }

    @Override
    protected ClusterConfig createTestInstance() {
        return randomInstance();
    }
}
