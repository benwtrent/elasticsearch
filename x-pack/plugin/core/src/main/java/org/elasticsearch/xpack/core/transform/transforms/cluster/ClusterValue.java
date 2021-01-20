/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.cluster;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.util.Locale;
import java.util.Map;

/**
 * A value that is included in a cluster
 */
public abstract class ClusterValue implements ToXContentObject, NamedWriteable {

    public enum Type {
        TERMS(0),
        DATE_HISTOGRAM(1);

        private final byte id;

        Type(int id) {
            this.id = (byte) id;
        }

        public byte getId() {
            return id;
        }

        public static ClusterValue.Type fromString(String string) {
            switch (string.toLowerCase(Locale.ROOT)) {
                case "terms":
                    return TERMS;
                case "date_histogram":
                    return DATE_HISTOGRAM;
                default:
                    throw new IllegalArgumentException("unknown type [" + string + "]");
            }
        }

        public static ClusterValue.Type fromId(byte id) {
            switch (id) {
                case 0:
                    return TERMS;
                case 2:
                    return DATE_HISTOGRAM;
                default:
                    throw new IllegalArgumentException("unknown type");
            }
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public abstract String getName();

    public abstract Type getType();

    abstract Map<String, Object> asMap();

    abstract void merge(ClusterValue clusterValue);

    boolean shouldPrune(ClusterValue clusterValue) {
        return false;
    }
}
