/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.aggs.categorization.CategorizationTokenTree.WILD_CARD;

/**
 * How to have dynamically mergeable buckets?
 */
public class InternalCategorizationAggregation extends InternalMultiBucketAggregation<
    InternalCategorizationAggregation,
    InternalCategorizationAggregation.Bucket> {

    private static final Logger LOGGER = LogManager.getLogger(InternalCategorizationAggregation.class);

    static class BucketKey implements ToXContentFragment, Writeable, Comparable<BucketKey>  {

        private final BytesRef[] key;

        BucketKey(BytesRef[] key) {
            this.key = key;
        }

        BucketKey(StreamInput in) throws IOException {
            key = in.readArray(StreamInput::readBytesRef, BytesRef[]::new);
        }


        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.value(asString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeArray(StreamOutput::writeBytesRef, key);
        }

        public String asString() {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < key.length - 1; i++) {
                builder.append(key[i].utf8ToString()).append(" ");
            }
            builder.append(key[key.length - 1].utf8ToString());
            return builder.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BucketKey bucketKey = (BucketKey) o;
            return Arrays.equals(key, bucketKey.key);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(key);
        }

        public BytesRef[] keyAsTokens() {
            return key;
        }

        @Override
        public int compareTo(BucketKey o) {
            return Arrays.compare(key, o.key);
        }

        private BucketKey collapseWildCards() {
            if (key.length <= 1) {
                return this;
            }
            List<BytesRef> collapsedWildCards = new ArrayList<>();
            boolean previousTokenWildCard = false;
            for (BytesRef token : key) {
                if (token.equals(WILD_CARD)) {
                    if (previousTokenWildCard == false) {
                        previousTokenWildCard = true;
                        collapsedWildCards.add(WILD_CARD);
                    }
                } else {
                    previousTokenWildCard = false;
                    collapsedWildCards.add(token);
                }
            }
            if (collapsedWildCards.size() == key.length) {
                return this;
            }
            return new BucketKey(collapsedWildCards.toArray(BytesRef[]::new));
        }
    }

    public static class Bucket extends InternalBucket implements MultiBucketsAggregation.Bucket, Comparable<Bucket> {

        final BucketKey key;
        final long docCount;
        final InternalAggregations aggregations;

        public Bucket(BucketKey key, long docCount, InternalAggregations aggregations) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        public Bucket(StreamInput in) throws IOException {
            key = new BucketKey(in);
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            builder.field(CommonFields.KEY.getPreferredName());
            key.toXContent(builder, params);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return key.asString();
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            key.writeTo(out);
            out.writeVLong(getDocCount());
            aggregations.writeTo(out);
        }

        @Override
        public String toString() {
            return "Bucket{" +
                "key=" + getKeyAsString() +
                ", docCount=" + docCount +
                ", aggregations=" + aggregations.asMap() +
                "}\n";
        }

        public BytesRef[] keyAsTokens() {
            return key.keyAsTokens();
        }

        @Override
        public int compareTo(Bucket o) {
            return key.compareTo(o.key);
        }

        private BucketKey collapseWildCards() {
            return key.collapseWildCards();
        }
    }

    private final List<Bucket> buckets;
    private final int maxChildren;
    private final double similarityThreshold;
    private final int maxDepth;

    protected InternalCategorizationAggregation(String name,
                                                int maxChildren,
                                                int maxDepth,
                                                double similarityThreshold,
                                                Map<String, Object> metadata) {
        this(name, maxChildren, maxDepth, similarityThreshold, metadata, new ArrayList<>());
    }

    protected InternalCategorizationAggregation(String name,
                                                int maxChildren,
                                                int maxDepth,
                                                double similarityThreshold,
                                                Map<String, Object> metadata,
                                                List<Bucket> buckets) {
        super(name, metadata);
        this.buckets = buckets;
        this.maxChildren = maxChildren;
        this.maxDepth = maxDepth;
        this.similarityThreshold = similarityThreshold;
    }

    public InternalCategorizationAggregation(StreamInput in) throws IOException {
        super(in);
        this.maxChildren = in.readVInt();
        this.maxDepth = in.readVInt();
        this.similarityThreshold = in.readDouble();
        this.buckets = in.readList(Bucket::new);
    }

    @Override
    public InternalCategorizationAggregation create(List<Bucket> buckets) {
        return new InternalCategorizationAggregation(name, maxChildren, maxDepth, similarityThreshold, super.metadata, buckets);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.key, prototype.docCount, aggregations);
    }

    @Override
    // This is for buckets that share the same key
    // So, it should be pretty simple to filter down the tree, incrementing the counts and returning the new bucket with the new count
    protected Bucket reduceBucket(List<Bucket> buckets, ReduceContext context) {
        assert buckets.size() > 0;
        List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
        long docCount = 0;
        for (Bucket bucket : buckets) {
            docCount += bucket.docCount;
            aggregations.add((InternalAggregations) bucket.getAggregations());
        }
        InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
        return new Bucket(buckets.get(0).key, docCount, aggs);
    }

    @Override
    public List<Bucket> getBuckets() {
        return buckets;
    }

    @Override
    public String getWriteableName() {
        return CategorizeTextAggregationBuilder.NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(maxChildren);
        out.writeVInt(maxDepth);
        out.writeDouble(similarityThreshold);
        out.writeList(buckets);
    }

    // Reduce buckets down by the same key to get accurate counts
    private List<Bucket> reduceBuckets(List<InternalAggregation> aggregations, ReduceContext reduceContext) {

        final PriorityQueue<IteratorAndCurrent<Bucket>> pq = new PriorityQueue<>(aggregations.size()) {
            @Override
            protected boolean lessThan(IteratorAndCurrent<Bucket> a, IteratorAndCurrent<Bucket> b) {
                return a.current().compareTo(b.current()) < 0;
            }
        };
        for (InternalAggregation aggregation : aggregations) {
            InternalCategorizationAggregation categorizationAggregation = (InternalCategorizationAggregation) aggregation;
            if (categorizationAggregation.buckets.isEmpty() == false) {
                pq.add(new IteratorAndCurrent<>(categorizationAggregation.buckets.iterator()));
            }
        }

        List<Bucket> reducedBuckets = new ArrayList<>();
        if (pq.size() > 0) {
            // list of buckets coming from different shards that have the same key
            List<Bucket> currentBuckets = new ArrayList<>();
            BucketKey key = pq.top().current().key;

            do {
                final IteratorAndCurrent<Bucket> top = pq.top();

                if (top.current().key.compareTo(key) != 0) {
                    final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                    reducedBuckets.add(reduced);
                    currentBuckets.clear();
                    key = top.current().key;
                }

                currentBuckets.add(top.current());

                if (top.hasNext()) {
                    top.next();
                    assert top.current().key.compareTo(key) > 0 : "shards must return data sorted by key";
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            } while (pq.size() > 0);

            if (currentBuckets.isEmpty() == false) {
                final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                reducedBuckets.add(reduced);
            }
        }

        return reducedBuckets;
    }

    @Override
    //TODO this is woefully inefficient
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        CategorizationTokenTree categorizationTokenTree = new CategorizationTokenTree(maxChildren, maxDepth, similarityThreshold);
        // First, make sure we have all the counts for equal log groups
        List<Bucket> reduced = reduceBuckets(aggregations, reduceContext);

        // Now, sort by largest groups so they get precedence when merged into the same tree
        reduced.sort(Comparator.comparing(Bucket::getDocCount).reversed());
        // Native long iterator??? Or just make the hash for the log group static and not based on the log tokens
        for(Bucket bucket : reduced) {
            categorizationTokenTree.parseLogLine(bucket.keyAsTokens(), bucket.docCount);
        }
        categorizationTokenTree.mergeSmallestChildren();
        Map<LogGroup, List<Bucket>> mergedBuckets = new HashMap<>();
        for(Bucket bucket : reduced) {
            LogGroup group = categorizationTokenTree.parseLogLineConst(bucket.keyAsTokens());
            if (group == null) {
                throw new AggregationExecutionException(
                    "Unexpected null categorization group for bucket [" + bucket.getKeyAsString() + "]"
                );
            }
            mergedBuckets.computeIfAbsent(group, k -> new ArrayList<>()).add(bucket);
        }

        List<Bucket> finallyMergedBuckets = new ArrayList<>();
        for (Map.Entry<LogGroup, List<Bucket>> groupAndBuckets : mergedBuckets.entrySet()) {
            LogGroup group = groupAndBuckets.getKey();
            List<Bucket> buckets = groupAndBuckets.getValue();
            List<InternalAggregations> bucketAggs = new ArrayList<>(buckets.size());
            long docCount = 0;
            for (Bucket bucket : buckets) {
                docCount += bucket.docCount;
                bucketAggs.add((InternalAggregations) bucket.getAggregations());
            }
            InternalAggregations aggs = InternalAggregations.reduce(bucketAggs, reduceContext);
            BytesRef[] key = group.getLogEvent();
            finallyMergedBuckets.add(new Bucket(new BucketKey(key), docCount, aggs));
        }
        Collections.sort(finallyMergedBuckets);
        if (reduceContext.isFinalReduce() == false || finallyMergedBuckets.isEmpty()) {
            return new InternalCategorizationAggregation(name, maxChildren, maxDepth, similarityThreshold, metadata, finallyMergedBuckets);
        }
        return new InternalCategorizationAggregation(
            name,
            maxChildren,
            maxDepth,
            similarityThreshold,
            metadata,
            finallyMergedBuckets.stream()
                .collect(
                    Collectors.groupingBy(
                        Bucket::collapseWildCards,
                        LinkedHashMap::new,
                        Collectors.mapping(Function.identity(), Collectors.toList())
                    )
                ).entrySet()
                .stream()
                .map(entry -> {
                    long docCount = 0;
                    List<InternalAggregations> toMerge = new ArrayList<>();
                    for (Bucket bucket : entry.getValue()) {
                        toMerge.add(bucket.aggregations);
                        docCount += bucket.docCount;
                    }
                    InternalAggregations aggs = toMerge.size() == 1 ? toMerge.get(0) : InternalAggregations.reduce(toMerge, reduceContext);
                    return new Bucket(entry.getKey(), docCount, aggs);
                })
                .collect(Collectors.toList())
        );
    }

    @Override
    public Object getProperty(List<String> path) {
        //TODO anything special?
        return super.getProperty(path);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }
}
