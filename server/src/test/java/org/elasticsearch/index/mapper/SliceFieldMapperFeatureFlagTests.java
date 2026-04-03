/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.cluster.metadata.IndexMetadata;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;

public class SliceFieldMapperFeatureFlagTests extends MapperServiceTestCase {

    public void testSliceIsRegisteredWhenFeatureEnabled() {
        assumeTrue("slice mapper feature flag must be enabled", SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled());
        IndicesModule module = new IndicesModule(Collections.emptyList());
        assertTrue(module.getMapperRegistry().getMetadataMapperParsers(getVersion()).containsKey(SliceFieldMapper.NAME));
    }

    public void testSliceIsNotRegisteredWhenFeatureDisabled() {
        assumeFalse("slice mapper feature flag must be disabled", SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled());
        IndicesModule module = new IndicesModule(Collections.emptyList());
        assertFalse(module.getMapperRegistry().getMetadataMapperParsers(getVersion()).containsKey(SliceFieldMapper.NAME));
    }

    public void testMappingWithSliceIsRejectedWhenFeatureDisabled() throws IOException {
        assumeFalse("slice mapper feature flag must be disabled", SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled());
        MapperService mapperService = createMapperService(Settings.EMPTY, mapping(b -> {}));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            merge(mapperService, topMapping(b -> b.startObject(SliceFieldMapper.NAME).field("enabled", true).endObject()));
        });
        assertThat(e.getMessage(), containsString("Root mapping definition has unsupported parameters"));
        assertThat(e.getMessage(), containsString("_slice"));
    }

    public void testSliceIsRejectedInTimeSeriesIndexMode() throws IOException {
        assumeTrue("slice mapper feature flag must be enabled", SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-10-29T00:00:00Z")
            .build();
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            MapperService mapperService = createMapperService(settings, mapping(b -> {}));
            merge(mapperService, topMapping(b -> b.startObject(SliceFieldMapper.NAME).field("enabled", true).endObject()));
        });
        assertThat(e.getMessage(), containsString("[_slice] is not supported in [index.mode=time_series]"));
    }

    public void testSliceEnabledRequiresPrimaryIndexSortOnSlice() throws IOException {
        assumeTrue("slice mapper feature flag must be enabled", SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled());
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(Settings.EMPTY, topMapping(b -> {
            b.startObject(SliceFieldMapper.NAME).field("enabled", true).endObject();
        })));
        assertThat(e.getMessage(), containsString("[_slice] requires primary index sort field [_slice] when enabled"));
    }
}
