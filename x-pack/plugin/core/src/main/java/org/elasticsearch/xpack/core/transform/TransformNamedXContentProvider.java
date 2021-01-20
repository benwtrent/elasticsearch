/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.xpack.core.transform.transforms.FunctionState;
import org.elasticsearch.xpack.core.transform.transforms.SyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.cluster.ClusterFunctionState;
import org.elasticsearch.xpack.core.transform.transforms.cluster.ClusterValue;
import org.elasticsearch.xpack.core.transform.transforms.cluster.DateValue;
import org.elasticsearch.xpack.core.transform.transforms.cluster.TermValue;

import java.util.Arrays;
import java.util.List;

public class TransformNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return Arrays.asList(
            new NamedXContentRegistry.Entry(SyncConfig.class,
                TransformField.TIME_BASED_SYNC,
                TimeSyncConfig::parse),
            new NamedXContentRegistry.Entry(FunctionState.class,
                new ParseField(ClusterFunctionState.NAME),
                (p, c) -> {
                    boolean ignoreUnknownFields = (boolean) c;
                    return ClusterFunctionState.fromXContent(p, ignoreUnknownFields);
                }),
            new NamedXContentRegistry.Entry(ClusterValue.class,
                new ParseField(DateValue.NAME),
                (p, c) -> {
                    boolean ignoreUnknownFields = (boolean) c;
                    return DateValue.fromXContent(p, ignoreUnknownFields);
                }),
            new NamedXContentRegistry.Entry(ClusterValue.class,
                new ParseField(TermValue.NAME),
                (p, c) -> {
                    boolean ignoreUnknownFields = (boolean) c;
                    return TermValue.fromXContent(p, ignoreUnknownFields);
                })
            );
    }
}
