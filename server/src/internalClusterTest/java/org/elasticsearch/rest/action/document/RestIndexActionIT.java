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
import org.elasticsearch.index.mapper.SliceFieldMapper;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.InputStreamReader;

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
        assumeTrue("slice mapper feature flag must be enabled", SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled());

        final String index = "test-slice-enabled";
        var create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.sort.field": ["_slice"]
              },
              "mappings": {
                "_slice": { "enabled": true },
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
        search.addParameter("filter_path", "hits.hits.fields");
        search.setJsonEntity("""
            {
              "query": { "match_all": {} },
              "_source": false,
              "fields": ["_slice"]
            }""");
        var response = ObjectPath.createFromResponse(getRestClient().performRequest(search));
        assertThat(response.evaluate("hits.hits.0.fields._slice.0"), equalTo("s1"));
    }

    public void testSliceMissingWhenEnabledFails() throws Exception {
        assumeTrue("slice mapper feature flag must be enabled", SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled());

        final String index = "test-slice-required";
        var create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.sort.field": ["_slice"]
              },
              "mappings": {
                "_slice": { "enabled": true },
                "properties": { "field": { "type": "keyword" } }
              }
            }""");
        getRestClient().performRequest(create);

        var indexReq = new Request("POST", "/" + index + "/_doc/1");
        indexReq.addParameter("refresh", "true");
        indexReq.setJsonEntity("{\"field\":\"value\"}");

        var exception = assertThrows(ResponseException.class, () -> getRestClient().performRequest(indexReq));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("Slice is required"));
    }

    public void testSliceProvidedWhenDisabledFails() throws Exception {
        assumeTrue("slice mapper feature flag must be enabled", SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled());

        final String index = "test-slice-disabled";
        var create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "mappings": {
                "_slice": { "enabled": false },
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
        assertThat(response, containsString("Cannot provide [_slice]"));
    }

    public void testSliceParamRejectedWhenFeatureDisabled() throws Exception {
        assumeFalse("slice mapper feature flag must be disabled", SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled());

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
        assumeTrue("slice mapper feature flag must be enabled", SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled());

        final String index = "test-slice-tsdb";
        var create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.mode": "time_series",
                "index.routing_path": ["dim"]
              },
              "mappings": {
                "_slice": { "enabled": true },
                "properties": {
                  "@timestamp": { "type": "date" },
                  "dim": { "type": "keyword", "time_series_dimension": true }
                }
              }
            }""");

        var exception = assertThrows(ResponseException.class, () -> getRestClient().performRequest(create));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[_slice] is not supported in [index.mode=time_series]"));
    }
}
