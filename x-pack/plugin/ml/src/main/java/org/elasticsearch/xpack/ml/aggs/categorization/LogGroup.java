/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Tuple;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.aggs.categorization.CategorizationTokenTree.WILD_CARD;

class LogGroup implements ToXContentObject, Accountable {

    private final long id;
    private final BytesRef[] logEvent;
    private final long[] tokenCounts;
    private long count;

    @Override
    public String toString() {
        return "LogGroup{" +
            "id=" + id +
            ", logEvent=" + Arrays.stream(logEvent).map(BytesRef::utf8ToString).collect(Collectors.joining(", ", "[", "]")) +
            ", count=" + count +
            '}';
    }

    LogGroup(BytesRef[] logTokens, long count, long id) {
        this.id = id;
        this.logEvent = logTokens;
        this.count = count;
        this.tokenCounts = new long[logTokens.length];
        Arrays.fill(this.tokenCounts, count);
    }

    public long getId() {
        return id;
    }

    BytesRef[] getLogEvent() {
        return logEvent;
    }

    public long getCount() {
        return count;
    }

    Tuple<Double, Integer> calculateSimilarity(BytesRef[] logEvent) {
        assert logEvent.length == this.logEvent.length;
        int eqParams = 0;
        long tokensRemoved = 0;
        long tokensKept = 0;
        for (int i = 0; i < logEvent.length; i++) {
            if (logEvent[i].equals(this.logEvent[i])) {
                tokensKept += tokenCounts[i];
            } else if (this.logEvent[i].equals(WILD_CARD)) {
                eqParams++;
            } else {
                tokensRemoved += tokenCounts[i];
            }
        }
        return new Tuple<>(tokensRemoved == 0 ? 1.0 : (double)tokensKept/tokensRemoved, eqParams);
    }

    void addLog(BytesRef[] logEvent, long docCount) {
        assert logEvent.length == this.logEvent.length;
        for (int i = 0; i < logEvent.length; i++) {
            if (logEvent[i].equals(this.logEvent[i]) == false) {
                this.logEvent[i] = WILD_CARD;
            } else {
                tokenCounts[i] += docCount;
            }
        }
        this.count += docCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("log_event", Arrays.stream(logEvent).map(BytesRef::utf8ToString).collect(Collectors.toList()));
        builder.field("count", this.count);
        builder.endObject();
        return builder;
    }

    @Override
    public long ramBytesUsed() {
        return Long.BYTES //id
            + (long)logEvent.length * RamUsageEstimator.NUM_BYTES_ARRAY_HEADER // logEvent
            + ((long)logEvent.length * Long.BYTES) + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER //tokenCounts
            + Long.BYTES //count
            + Long.BYTES; // bytesUsed;
    }

}
