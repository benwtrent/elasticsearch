/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Stores wall-clock nanosecond timing buckets for IVF vector query phases.
 */
public final class VectorQueryPhaseTimings {

    /**
     * Named timing buckets used by IVF + DiskBBQ vector search instrumentation.
     */
    public enum Phase {
        LEAF_SEARCH,
        CENTROID_FILTER,
        CENTROID_ITERATOR_BUILD,
        CENTROID_QUERY_QUANTIZE,
        CENTROID_BULK_SCORE,
        CENTROID_APPLY_CORRECTIONS,
        POSTING_RESET,
        POSTING_VISIT,
        DOCID_READ,
        QUERY_QUANTIZE,
        SCORE_BULK,
        SCORE_BULK_OFFSETS,
        SCORE_INDIVIDUAL,
        APPLY_CORRECTIONS,
        COLLECT_BULK,
        FILTERED_FOLLOWUP,
        RESCORE_VECTOR_SCORE
    }

    private static final ThreadLocal<VectorQueryPhaseTimings> CURRENT = new ThreadLocal<>();

    private final boolean enabled;
    private final LongAdder[] nanosByPhase;

    public VectorQueryPhaseTimings(boolean enabled) {
        this.enabled = enabled;
        this.nanosByPhase = new LongAdder[Phase.values().length];
        for (int i = 0; i < nanosByPhase.length; i++) {
            nanosByPhase[i] = new LongAdder();
        }
    }

    public static VectorQueryPhaseTimings current() {
        return CURRENT.get();
    }

    public static VectorQueryPhaseTimings setCurrent(VectorQueryPhaseTimings timings) {
        VectorQueryPhaseTimings previous = CURRENT.get();
        if (timings == null) {
            CURRENT.remove();
        } else {
            CURRENT.set(timings);
        }
        return previous;
    }

    public static void restore(VectorQueryPhaseTimings previous) {
        if (previous == null) {
            CURRENT.remove();
        } else {
            CURRENT.set(previous);
        }
    }

    public boolean enabled() {
        return enabled;
    }

    public void addNanos(Phase phase, long nanos) {
        if (enabled == false || nanos <= 0) {
            return;
        }
        nanosByPhase[phase.ordinal()].add(nanos);
    }

    public long start() {
        return enabled ? System.nanoTime() : 0L;
    }

    public void stop(Phase phase, long startNanos) {
        if (enabled == false) {
            return;
        }
        addNanos(phase, System.nanoTime() - startNanos);
    }

    public Map<String, Long> snapshot() {
        if (enabled == false) {
            return Collections.emptyMap();
        }
        Map<String, Long> values = new LinkedHashMap<>();
        for (Phase phase : Phase.values()) {
            long nanos = nanosByPhase[phase.ordinal()].sum();
            if (nanos > 0) {
                values.put(phase.name().toLowerCase(), nanos);
            }
        }
        return values;
    }
}
