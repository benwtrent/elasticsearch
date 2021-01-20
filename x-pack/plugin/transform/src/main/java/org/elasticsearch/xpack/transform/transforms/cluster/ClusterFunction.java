/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.mapper.DocCountFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.FunctionState;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.cluster.ClusterConfig;
import org.elasticsearch.xpack.core.transform.transforms.cluster.ClusterFunctionState;
import org.elasticsearch.xpack.core.transform.transforms.cluster.ClusterObject;
import org.elasticsearch.xpack.core.transform.transforms.cluster.ClusterValue;
import org.elasticsearch.xpack.core.transform.transforms.cluster.DateValue;
import org.elasticsearch.xpack.core.transform.transforms.cluster.TermValue;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.transform.Transform;
import org.elasticsearch.xpack.transform.transforms.IDGenerator;
import org.elasticsearch.xpack.transform.transforms.common.AbstractCompositeAggFunction;
import org.elasticsearch.xpack.transform.transforms.pivot.CompositeBucketsChangeCollector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.transform.transforms.common.DocumentConversionUtils.removeFields;

public class ClusterFunction extends AbstractCompositeAggFunction {

    private static final Logger logger = LogManager.getLogger(ClusterFunction.class);

    private final ClusterConfig config;
    private final SettingsConfig settings;
    private final Version version;
    private final ClusterFunctionState clusterFunctionState;
    private final Map<String, ClusterValueFactory> clusterValueFactoryMap;

    public ClusterFunction(ClusterConfig config, SettingsConfig settings, Version version, FunctionState functionState) {
        super(createCompositeAggregationSources(config.getGroupConfig(), "cluster"));
        this.config = config;
        this.settings = settings;
        this.version = version == null ? Version.CURRENT : version;
        List<ClusterFunctionState.ClusterFieldTypeAndName> featureNames = new ArrayList<>(config.getGroupConfig().getGroups().size());
        this.clusterValueFactoryMap = new HashMap<>();

        for (Map.Entry<String, SingleGroupSource> singleGroupSource : config.getGroupConfig().getGroups().entrySet()) {
            ClusterValueFactory clusterValueFactory = ClusterValueFactory.factoryFrom(singleGroupSource.getValue());
            featureNames.add(new ClusterFunctionState.ClusterFieldTypeAndName(
                singleGroupSource.getKey(),
                clusterValueFactory.factoryType()
            ));
            clusterValueFactoryMap.put(singleGroupSource.getKey(), clusterValueFactory);
        }
        this.clusterFunctionState = functionState instanceof ClusterFunctionState ?
            (ClusterFunctionState) functionState :
            ClusterFunctionState.empty(featureNames.toArray(new ClusterFunctionState.ClusterFieldTypeAndName[0]));
    }

    @Override
    public void validateConfig(ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    @Override
    public List<String> getPerformanceCriticalFields() {
        return config.getGroupConfig().getGroups().values().stream().map(SingleGroupSource::getField).collect(toList());
    }

    @Override
    public ChangeCollector buildChangeCollector(String synchronizationField) {
        return CompositeBucketsChangeCollector.buildChangeCollector(config.getGroupConfig().getGroups(), synchronizationField);
    }

    @Override
    public void deduceMappings(Client client, SourceConfig sourceConfig, final ActionListener<Map<String, String>> listener) {
        // one day we may have to get the source value mappings (for histogram clustering, etc.)
        // But for now, mappings are consistent no matter the source.
        HashMap<String, String> mappings = new HashMap<>();
        mappings.put(ClusterObject.CLUSTER_ID.getPreferredName(), KeywordFieldMapper.CONTENT_TYPE);
        mappings.put(ClusterObject.COUNT.getPreferredName(), NumberFieldMapper.NumberType.LONG.typeName());
        clusterValueFactoryMap.forEach((fieldName, factory) -> mappings.putAll(factory.getIndexMapping(fieldName)));
        listener.onResponse(mappings);
    }

    @Override
    protected Map<String, Object> documentTransformationFunction(Map<String, Object> document) {
        return removeFields(document, f -> f != null
            && f.startsWith("_")
            && ((f.equals(DocCountFieldMapper.NAME) || f.equals("cluster_id")) == false));
    }

    @Override
    public SearchSourceBuilder buildSearchQueryForInitialProgress(SearchSourceBuilder searchSourceBuilder) {
        BoolQueryBuilder existsClauses = QueryBuilders.boolQuery();

        config.getGroupConfig().getGroups().values().forEach(src -> {
            if (src.getMissingBucket() == false && src.getField() != null) {
                existsClauses.must(QueryBuilders.existsQuery(src.getField()));
            }
        });

        return searchSourceBuilder.query(existsClauses).size(0).trackTotalHits(true);
    }

    /**
     * Get the initial page size for this pivot.
     *
     * The page size is the main parameter for adjusting memory consumption. Memory consumption mainly depends on
     * the page size, the type of aggregations and the data. As the page size is the number of buckets we return
     * per page the page size is a multiplier for the costs of aggregating bucket.
     *
     * The user may set a maximum in the {@link PivotConfig#getMaxPageSearchSize()}, but if that is not provided,
     *    the default {@link Transform#DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE} is used.
     *
     * In future we might inspect the configuration and base the initial size on the aggregations used.
     *
     * @return the page size
     */
    @Override
    public int getInitialPageSize() {
        return Transform.DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE;
    }

    @Override
    protected Stream<Map<String, Object>> extractResults(
        CompositeAggregation agg,
        Map<String, String> fieldTypeMap,
        TransformIndexerStats transformIndexerStats
    ) {
        // defines how dates are written, if not specified in settings
        // < 7.11 as epoch millis
        // >= 7.11 as string
        // note: it depends on the version when the transform has been created, not the version of the code
        boolean datesAsEpoch = settings.getDatesAsEpochMillis() != null ? settings.getDatesAsEpochMillis()
            : version.onOrAfter(Version.V_7_11_0) ? false
            : true;

        List<Tuple<Map<String, ClusterValue>, Long>> aggResults = aggResultsToClusterValueAndCount(agg, transformIndexerStats)
            .collect(Collectors.toList());
        return mergeClusters(aggResults, datesAsEpoch).stream();
    }

    private List<Map<String, Object>> mergeClusters(List<Tuple<Map<String, ClusterValue>, Long>> aggResults, boolean datesAsEpoch) {
        Set<String> updatedClusters = new HashSet<>();
        for (Tuple<Map<String, ClusterValue>, Long> aggResultAndCount : aggResults) {
            final Map<String, ClusterValue> fields = aggResultAndCount.v1();
            final long count = aggResultAndCount.v2();
            String clusterId = clusterFunctionState.putOrUpdateCluster(fields, count, ClusterFunction::generateClusterId);
            logger.trace(() -> new ParameterizedMessage("cluster [{}] found for [{}]", clusterId, fields));
            updatedClusters.add(clusterId);
        }
        List<Map<String, Object>> clusters = updatedClusters.stream().map(id -> {
            Map<String, Object> map = clusterFunctionState.clusterAsMap(id, datesAsEpoch);
            map.put(TransformField.DOCUMENT_ID_FIELD, id);
            return map;
        }).collect(Collectors.toList());
        clusterFunctionState.pruneClusters(aggResults.get(aggResults.size() - 1).v1());
        return clusters;
    }

    Stream<Tuple<Map<String, ClusterValue>, Long>> aggResultsToClusterValueAndCount(
        CompositeAggregation agg,
        TransformIndexerStats stats
    ) {
        return agg.getBuckets().stream().map(bucket -> {
            final long docCount = bucket.getDocCount();
            stats.incrementNumDocuments(docCount);
            Map<String, ClusterValue> document = new HashMap<>();
            final Map<String, Object> bucketKey = bucket.getKey();
            config.getGroupConfig().getGroups().forEach((destinationFieldName, singleGroupSource) ->
                document.put(
                    destinationFieldName,
                    clusterValueFactoryMap.get(destinationFieldName).build(bucketKey.get(destinationFieldName))
                )
            );
            return Tuple.tuple(document, docCount);
        });
    }

    @Override
    public FunctionState getFunctionState() {
        return clusterFunctionState;
    }

    private static String generateClusterId(Map<String, ClusterValue> clusterValues) {
        IDGenerator idGenerator = new IDGenerator();
        for (Map.Entry<String, ClusterValue> fieldNameAndValue : clusterValues.entrySet()) {
            final ClusterValue clusterValue = fieldNameAndValue.getValue();
            final String fieldName = fieldNameAndValue.getKey();
            if (clusterValue == null) {
                idGenerator.add(fieldName, null);
            } else if (clusterValue instanceof DateValue) {
                idGenerator.add(fieldName, ((DateValue)clusterValue).getMin());
            } else if (clusterValue instanceof TermValue) {
                idGenerator.add(fieldName, ((TermValue)clusterValue).getValue());
            } else {
                throw new IllegalArgumentException("Cluster value of type [" + clusterValue.getClass() + "] is not supported");
            }
        }
        return idGenerator.getID();
    }
}
