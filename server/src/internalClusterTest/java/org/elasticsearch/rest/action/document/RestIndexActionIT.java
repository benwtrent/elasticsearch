/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class RestIndexActionIT extends ESIntegTestCase {
    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testIndexWithSourceOnErrorDisabled() throws Exception {
        var source = "{\"field\": \"value}";
        var sourceEscaped = "{\\\"field\\\": \\\"value}";

        var request = new Request("POST", "/test_index/_doc/1");
        request.setJsonEntity(source);

        var exception = assertThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString(sourceEscaped));

        // disable source on error
        request.addParameter(RestUtils.INCLUDE_SOURCE_ON_ERROR_PARAMETER, "false");
        exception = assertThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(
            response,
            both(not(containsString(sourceEscaped))).and(
                containsString("REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled)")
            )
        );
    }

    public void testSliceEndToEndWhenEnabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        final String index = "test-slice-enabled";
        var create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              },
              "mappings": {
                "properties": { "field": { "type": "keyword" } }
              }
            }""");
        getRestClient().performRequest(create);

        var indexReq = new Request("POST", "/" + index + "/_doc/1");
        indexReq.addParameter("_slice", "s1");
        indexReq.addParameter("refresh", "true");
        indexReq.setJsonEntity("{\"field\":\"value\"}");
        getRestClient().performRequest(indexReq);

        var search = new Request("GET", "/" + index + "/_search");
        search.addParameter("_slice", "s1");
        search.addParameter("filter_path", "hits.total.value,hits.hits.fields");
        search.setJsonEntity("""
            {
              "query": { "match_all": {} },
              "_source": false,
              "fields": ["_slice"]
            }""");
        var response = ObjectPath.createFromResponse(getRestClient().performRequest(search));
        assertThat(response.evaluate("hits.total.value"), equalTo(1));
        assertThat(response.evaluate("hits.hits.0.fields._slice.0"), equalTo("s1"));
    }

    public void testSliceMissingWhenEnabledFails() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        final String index = "test-slice-required";
        var create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              },
              "mappings": {
                "properties": { "field": { "type": "keyword" } }
              }
            }""");
        getRestClient().performRequest(create);

        var indexReq = new Request("POST", "/" + index + "/_doc/1");
        indexReq.addParameter("refresh", "true");
        indexReq.setJsonEntity("{\"field\":\"value\"}");

        var exception = assertThrows(ResponseException.class, () -> getRestClient().performRequest(indexReq));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[_slice] is required"));
    }

    public void testSearchRequiresSliceWhenSliceEnabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        final String index = "test-slice-search-required";
        var create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              },
              "mappings": {
                "properties": { "field": { "type": "keyword" } }
              }
            }""");
        getRestClient().performRequest(create);

        var indexReq = new Request("POST", "/" + index + "/_doc/1");
        indexReq.addParameter("_slice", "s1");
        indexReq.addParameter("refresh", "true");
        indexReq.setJsonEntity("{\"field\":\"value\"}");
        getRestClient().performRequest(indexReq);

        var searchMissingSlice = new Request("GET", "/" + index + "/_search");
        var exception = assertThrows(ResponseException.class, () -> getRestClient().performRequest(searchMissingSlice));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[_slice] is required"));

        var searchWithSlice = new Request("GET", "/" + index + "/_search");
        searchWithSlice.addParameter("_slice", "s1");
        searchWithSlice.addParameter("filter_path", "hits.total.value");
        var ok = ObjectPath.createFromResponse(getRestClient().performRequest(searchWithSlice));
        assertThat(ok.evaluate("hits.total.value"), equalTo(1));
    }

    public void testSearchSliceAllAndMultipleSlices() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        final String index = "test-slice-search-all";
        var create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              },
              "mappings": {
                "properties": { "field": { "type": "keyword" } }
              }
            }""");
        getRestClient().performRequest(create);

        var indexS1 = new Request("POST", "/" + index + "/_doc/1");
        indexS1.addParameter("_slice", "s1");
        indexS1.addParameter("refresh", "true");
        indexS1.setJsonEntity("{\"field\":\"v1\"}");
        getRestClient().performRequest(indexS1);

        var indexS2 = new Request("POST", "/" + index + "/_doc/2");
        indexS2.addParameter("_slice", "s2");
        indexS2.addParameter("refresh", "true");
        indexS2.setJsonEntity("{\"field\":\"v2\"}");
        getRestClient().performRequest(indexS2);

        var searchAll = new Request("GET", "/" + index + "/_search");
        searchAll.addParameter("_slice", "_all");
        searchAll.addParameter("filter_path", "hits.total.value");
        var all = ObjectPath.createFromResponse(getRestClient().performRequest(searchAll));
        assertThat(all.evaluate("hits.total.value"), equalTo(2));

        var searchMulti = new Request("GET", "/" + index + "/_search");
        searchMulti.addParameter("_slice", "s1,s2");
        searchMulti.addParameter("filter_path", "hits.total.value");
        var multi = ObjectPath.createFromResponse(getRestClient().performRequest(searchMulti));
        assertThat(multi.evaluate("hits.total.value"), equalTo(2));
    }

    public void testTermsAggregationOnSliceAlias() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        final String index = "test-slice-terms-agg";
        var create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              },
              "mappings": {
                "properties": { "field": { "type": "keyword" } }
              }
            }""");
        getRestClient().performRequest(create);

        var indexS1Doc1 = new Request("POST", "/" + index + "/_doc/1");
        indexS1Doc1.addParameter("_slice", "s1");
        indexS1Doc1.setJsonEntity("{\"field\":\"v1\"}");
        getRestClient().performRequest(indexS1Doc1);

        var indexS1Doc2 = new Request("POST", "/" + index + "/_doc/2");
        indexS1Doc2.addParameter("_slice", "s1");
        indexS1Doc2.setJsonEntity("{\"field\":\"v2\"}");
        getRestClient().performRequest(indexS1Doc2);

        var indexS2Doc = new Request("POST", "/" + index + "/_doc/3");
        indexS2Doc.addParameter("_slice", "s2");
        indexS2Doc.setJsonEntity("{\"field\":\"v3\"}");
        getRestClient().performRequest(indexS2Doc);

        var refresh = new Request("POST", "/" + index + "/_refresh");
        getRestClient().performRequest(refresh);

        var searchAgg = new Request("GET", "/" + index + "/_search");
        searchAgg.addParameter("_slice", "_all");
        searchAgg.setJsonEntity("""
            {
              "size": 0,
              "aggs": {
                "by_slice": {
                  "terms": {
                    "field": "_slice",
                    "order": { "_key": "asc" }
                  }
                }
              }
            }""");
        var aggResponse = ObjectPath.createFromResponse(getRestClient().performRequest(searchAgg));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> buckets = aggResponse.evaluate("aggregations.by_slice.buckets");
        assertThat(buckets.size(), equalTo(2));
        assertThat(buckets.get(0).get("key"), equalTo("s1"));
        assertThat(((Number) buckets.get(0).get("doc_count")).intValue(), equalTo(2));
        assertThat(buckets.get(1).get("key"), equalTo("s2"));
        assertThat(((Number) buckets.get(1).get("doc_count")).intValue(), equalTo(1));
    }

    public void testSliceProvidedWhenDisabledFails() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        final String index = "test-slice-disabled";
        var create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": false
              },
              "mappings": {
                "properties": { "field": { "type": "keyword" } }
              }
            }""");
        getRestClient().performRequest(create);

        var indexReq = new Request("POST", "/" + index + "/_doc/1");
        indexReq.addParameter("_slice", "s1");
        indexReq.addParameter("refresh", "true");
        indexReq.setJsonEntity("{\"field\":\"value\"}");

        var exception = assertThrows(ResponseException.class, () -> getRestClient().performRequest(indexReq));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[_slice] is not allowed"));
    }

    public void testSliceParamRejectedWhenFeatureDisabled() throws Exception {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        final String index = "test-slice-feature-disabled";
        var create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "mappings": {
                "properties": { "field": { "type": "keyword" } }
              }
            }""");
        getRestClient().performRequest(create);

        var indexReq = new Request("POST", "/" + index + "/_doc/1");
        indexReq.addParameter("_slice", "s1");
        indexReq.setJsonEntity("{\"field\":\"value\"}");

        var exception = assertThrows(ResponseException.class, () -> getRestClient().performRequest(indexReq));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("request does not support [_slice]"));
    }

    public void testSliceNotAllowedInTimeSeriesMode() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        final String index = "test-slice-tsdb";
        var create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.mode": "time_series",
                "index.slice.enabled": true,
                "index.routing_path": ["dim"]
              },
              "mappings": {
                "properties": {
                  "@timestamp": { "type": "date" },
                  "dim": { "type": "keyword", "time_series_dimension": true }
                }
              }
            }""");

        var exception = assertThrows(ResponseException.class, () -> getRestClient().performRequest(create));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("index.slice.enabled"));
        assertThat(response, containsString("index.mode"));
        assertThat(response, containsString("time_series"));
    }
}
