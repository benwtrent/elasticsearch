/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.cluster;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.transforms.AbstractSerializingTransformTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;


public class ClusterFunctionStateTests extends AbstractSerializingTransformTestCase<ClusterFunctionState> {

    private static final Function<Map<String, ClusterValue>, String> SIMPLE_ID_GENERATOR = (obj) -> {
        int result = 1;
        for (ClusterValue clusterValue : obj.values()) {
            if (clusterValue instanceof TermValue) {
                TermValue termValue = (TermValue) clusterValue;
                result = 31 * result + (termValue.getValue() == null ? 0 : termValue.getValue().hashCode());
            }
            if (clusterValue instanceof DateValue) {
                DateValue dateValue = (DateValue) clusterValue;
                result = 31 * result + Long.hashCode(dateValue.getMin());
            }
        }
        return String.valueOf(result);
    };

    public static ClusterFunctionState randomInstance() {
        String[] fieldNames = Arrays.stream(generateRandomStringArray(10, 10, false, false))
            .distinct()
            .toArray(String[]::new);
        List<Supplier<ClusterValue>> randomValues = Stream
            .generate(() -> ClusterFunctionStateTests.randomClusterValueSupplier(
                randomFrom(ClusterValue.Type.TERMS, ClusterValue.Type.DATE_HISTOGRAM)
            ))
            .limit(fieldNames.length)
            .collect(Collectors.toList());
        ClusterFunctionState.ClusterFieldTypeAndName[] namesAndTypes = IntStream.range(0, fieldNames.length)
            .mapToObj(i -> new ClusterFunctionState.ClusterFieldTypeAndName(fieldNames[i], randomValues.get(i).get().getType()))
            .toArray(ClusterFunctionState.ClusterFieldTypeAndName[]::new);
        Map<String, ClusterObject> clusters = Arrays.stream(generateRandomStringArray(10, 10, false, false))
            .collect(Collectors.toMap(
                Function.identity(),
                id -> ClusterObjectTests.randomInstance(id, randomValues.stream().map(Supplier::get).toArray(ClusterValue[]::new)),
                (k, v) -> v,
                LinkedHashMap::new
            ));
        return new ClusterFunctionState(clusters, namesAndTypes);
    }

    private static Supplier<ClusterValue> randomClusterValueSupplier(ClusterValue.Type type) {
        switch (type) {
            case TERMS:
                return TermValueTests::randomTermValue;
            case DATE_HISTOGRAM:
                return DateValueTests::randomDateValue;
            default:
                throw new IllegalArgumentException("unknown type");
        }
    }

    public void testClusterAsMap() {
        ClusterFunctionState clusterFunctionState = randomInstance();
        String id = clusterFunctionState.putOrUpdateCluster(
            Arrays
                .stream(clusterFunctionState.getClusterFieldNames())
                .collect(Collectors.toMap(
                    ClusterFunctionState.ClusterFieldTypeAndName::getFieldName,
                    cfs -> randomClusterValueSupplier(cfs.getType()).get())
                ),
            10L,
            (unused) -> "foo");
        Map<String, Object> map = clusterFunctionState.clusterAsMap("foo", false);
        assertThat(map, hasEntry("cluster_id", "foo"));
        assertThat(map, hasEntry("count", 10L));
    }

    public void testPutOrUpdateCluster_newCluster() {
        ClusterFunctionState clusterFunctionState = randomInstance();
        String id = clusterFunctionState.putOrUpdateCluster(
            Arrays
                .stream(clusterFunctionState.getClusterFieldNames())
                .collect(Collectors.toMap(
                    ClusterFunctionState.ClusterFieldTypeAndName::getFieldName,
                    cfs -> randomClusterValueSupplier(cfs.getType()).get())
                ),
            10L,
            (unused) -> "foo");
        assertThat(id, equalTo("foo"));
        ClusterObject clusterObject = clusterFunctionState.getCluster("foo");
        assertThat(clusterObject, is(notNullValue()));
        assertThat(clusterObject.getId(), equalTo("foo"));
        assertThat(clusterObject.getCount(), equalTo(10L));
    }

    public void testPutOrUpdateCluster_updates() {
        final long interval = 3600;
        ClusterFunctionState clusterFunctionState = ClusterFunctionState.empty(
            new ClusterFunctionState.ClusterFieldTypeAndName("date1", ClusterValue.Type.DATE_HISTOGRAM),
            new ClusterFunctionState.ClusterFieldTypeAndName("term1", ClusterValue.Type.TERMS),
            new ClusterFunctionState.ClusterFieldTypeAndName("term2", ClusterValue.Type.TERMS)
        );
        String id = clusterFunctionState.putOrUpdateCluster(
            MapBuilder.<String, ClusterValue>newMapBuilder()
                .put("date1", new DateValue(0L, 0L, interval))
                .put("term1", new TermValue("foo"))
                .put("term2", new TermValue("foo"))
                .map(),
            1,
            SIMPLE_ID_GENERATOR);
        assertThat(clusterFunctionState.putOrUpdateCluster(
            MapBuilder.<String, ClusterValue>newMapBuilder()
                .put("date1", new DateValue(interval, interval, interval))
                .put("term1", new TermValue("foo"))
                .put("term2", new TermValue("foo"))
                .map(),
            1,
            SIMPLE_ID_GENERATOR), equalTo(id));
        assertThat(clusterFunctionState.putOrUpdateCluster(
            MapBuilder.<String, ClusterValue>newMapBuilder()
                .put("date1", new DateValue(interval * 2, interval * 2, interval))
                .put("term1", new TermValue("foo"))
                .put("term2", new TermValue("foo"))
                .map(),
            3,
            SIMPLE_ID_GENERATOR), equalTo(id));
        assertThat(clusterFunctionState.getCluster(id).getCount(), equalTo(5L));
        DateValue dateValue = ((DateValue)clusterFunctionState.getCluster(id).getClusterValues()[0]);
        assertThat(dateValue.getMin(), equalTo(0L));
        assertThat(dateValue.getMax(), equalTo(interval * 2L));

        String otherClusterId = clusterFunctionState.putOrUpdateCluster(
            MapBuilder.<String, ClusterValue>newMapBuilder()
                .put("date1", new DateValue(interval * 2, interval * 2, interval))
                .put("term1", new TermValue("bar"))
                .put("term2", new TermValue("foo"))
                .map(),
            1,
            SIMPLE_ID_GENERATOR);
        assertThat(otherClusterId, not(equalTo(id)));
        dateValue = ((DateValue)clusterFunctionState.getCluster(otherClusterId).getClusterValues()[0]);
        assertThat(dateValue.getMin(), equalTo(interval * 2L));
        assertThat(dateValue.getMax(), equalTo(interval * 2L));
    }

    public void testPrune() {
        final long interval = 3600;
        ClusterFunctionState clusterFunctionState = ClusterFunctionState.empty(
            new ClusterFunctionState.ClusterFieldTypeAndName("date1", ClusterValue.Type.DATE_HISTOGRAM),
            new ClusterFunctionState.ClusterFieldTypeAndName("term1", ClusterValue.Type.TERMS),
            new ClusterFunctionState.ClusterFieldTypeAndName("term2", ClusterValue.Type.TERMS)
        );
        String id = clusterFunctionState.putOrUpdateCluster(
            MapBuilder.<String, ClusterValue>newMapBuilder()
                .put("date1", new DateValue(0L, 0L, interval))
                .put("term1", new TermValue("foo"))
                .put("term2", new TermValue("foo"))
                .map(),
            1,
            SIMPLE_ID_GENERATOR);
        assertThat(clusterFunctionState.getCluster(id), is(notNullValue()));
        clusterFunctionState.pruneClusters(MapBuilder.<String, ClusterValue>newMapBuilder()
            .put("date1", new DateValue(interval, interval, interval))
            .put("term1", new TermValue(randomAlphaOfLength(10)))
            .put("term2", new TermValue(randomAlphaOfLength(10)))
            .map());

        clusterFunctionState.pruneClusters(MapBuilder.<String, ClusterValue>newMapBuilder()
            .put("date1", new DateValue(interval * 2, interval * 2, interval))
            .put("term1", new TermValue(randomAlphaOfLength(10)))
            .put("term2", new TermValue(randomAlphaOfLength(10)))
            .map());
        assertThat(clusterFunctionState.getCluster(id), is(nullValue()));
    }

    @Override
    protected ClusterFunctionState doParseInstance(XContentParser parser) throws IOException {
        return ClusterFunctionState.fromXContent(parser, false);
    }

    @Override
    protected Writeable.Reader<ClusterFunctionState> instanceReader() {
        return ClusterFunctionState::new;
    }

    @Override
    protected ClusterFunctionState createTestInstance() {
        return randomInstance();
    }
}
