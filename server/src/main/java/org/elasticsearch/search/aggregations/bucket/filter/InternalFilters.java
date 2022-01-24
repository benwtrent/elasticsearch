/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.support.LongToLongFunction;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

public class InternalFilters extends InternalMultiBucketAggregation<InternalFilters, InternalFilters.InternalBucket> implements Filters {
    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket implements Filters.Bucket {

        private final boolean keyed;
        private final String key;
        private long docCount;
        InternalAggregations aggregations;

        public InternalBucket(String key, long docCount, InternalAggregations aggregations, boolean keyed) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.keyed = keyed;
        }

        /**
         * Read from a stream.
         */
        public InternalBucket(StreamInput in, boolean keyed) throws IOException {
            this.keyed = keyed;
            key = in.readOptionalString();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return key;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (keyed) {
                builder.startObject(key);
            } else {
                builder.startObject();
            }
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            InternalBucket that = (InternalBucket) other;
            return Objects.equals(key, that.key)
                && Objects.equals(keyed, that.keyed)
                && Objects.equals(docCount, that.docCount)
                && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), key, keyed, docCount, aggregations);
        }
    }

    private final List<InternalBucket> buckets;
    private final boolean keyed;
    // bucketMap gets lazily initialized from buckets in getBucketByKey()
    private transient Map<String, InternalBucket> bucketMap;

    public InternalFilters(String name, List<InternalBucket> buckets, boolean keyed, Map<String, Object> metadata) {
        super(name, metadata);
        this.buckets = buckets;
        this.keyed = keyed;
    }

    /**
     * Read from a stream.
     */
    public InternalFilters(StreamInput in) throws IOException {
        super(in);
        keyed = in.readBoolean();
        int size = in.readVInt();
        List<InternalBucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new InternalBucket(in, keyed));
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(keyed);
        out.writeVInt(buckets.size());
        for (InternalBucket bucket : buckets) {
            bucket.writeTo(out);
        }
    }

    @Override
    public String getWriteableName() {
        return FiltersAggregationBuilder.NAME;
    }

    @Override
    public InternalFilters create(List<InternalBucket> buckets) {
        return new InternalFilters(name, buckets, keyed, metadata);
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(prototype.key, prototype.docCount, aggregations, prototype.keyed);
    }

    @Override
    public List<InternalBucket> getBuckets() {
        return buckets;
    }

    @Override
    public InternalBucket getBucketByKey(String key) {
        if (bucketMap == null) {
            bucketMap = Maps.newMapWithExpectedSize(buckets.size());
            for (InternalBucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(key);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        return innerReduce(aggregations, reduceContext, this::reduceBucket);
    }

    @Override
    public InternalAggregation reduceSampled(
        List<InternalAggregation> aggregations,
        AggregationReduceContext reduceContext,
        SamplingContext samplingContext
    ) {
        return innerReduce(
            aggregations,
            reduceContext,
            (buckets, context) -> this.reduceSampledBucket(buckets, reduceContext, samplingContext)
        );
    }

    private InternalAggregation innerReduce(
        List<InternalAggregation> aggregations,
        AggregationReduceContext reduceContext,
        BiFunction<List<InternalBucket>, AggregationReduceContext, InternalBucket> bucketReducer
    ) {
        List<List<InternalBucket>> bucketsList = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalFilters filters = (InternalFilters) aggregation;
            if (bucketsList == null) {
                bucketsList = new ArrayList<>(filters.buckets.size());
                for (InternalBucket bucket : filters.buckets) {
                    List<InternalBucket> sameRangeList = new ArrayList<>(aggregations.size());
                    sameRangeList.add(bucket);
                    bucketsList.add(sameRangeList);
                }
            } else {
                int i = 0;
                for (InternalBucket bucket : filters.buckets) {
                    bucketsList.get(i++).add(bucket);
                }
            }
        }

        reduceContext.consumeBucketsAndMaybeBreak(bucketsList.size());
        InternalFilters reduced = new InternalFilters(name, new ArrayList<>(bucketsList.size()), keyed, getMetadata());
        for (List<InternalBucket> sameRangeList : bucketsList) {
            reduced.buckets.add(bucketReducer.apply(sameRangeList, reduceContext));
        }
        return reduced;
    }

    @Override
    protected InternalBucket reduceBucket(List<InternalBucket> buckets, AggregationReduceContext context) {
        return innerReduceBucket(buckets, aggregationsList -> InternalAggregations.reduce(aggregationsList, context), l -> l);
    }

    @Override
    protected InternalBucket reduceSampledBucket(
        List<InternalBucket> buckets,
        AggregationReduceContext context,
        SamplingContext samplingContext
    ) {
        return innerReduceBucket(
            buckets,
            aggregationsList -> InternalAggregations.reduceSampled(aggregationsList, context, samplingContext),
            context.isFinalReduce() ? samplingContext::inverseScale : l -> l
        );
    }

    private InternalBucket innerReduceBucket(
        List<InternalBucket> buckets,
        Function<List<InternalAggregations>, InternalAggregations> aggReducer,
        LongToLongFunction countScaler
    ) {
        assert buckets.size() > 0;
        long docCount = 0;
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        for (InternalBucket bucket : buckets) {
            docCount += bucket.docCount;
            aggregationsList.add(bucket.aggregations);
        }
        InternalAggregations aggregations = aggReducer.apply(aggregationsList);
        return new InternalBucket(buckets.get(0).key, countScaler.apply(docCount), aggregations, buckets.get(0).keyed);

    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (InternalBucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, keyed);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalFilters that = (InternalFilters) obj;
        return Objects.equals(buckets, that.buckets) && Objects.equals(keyed, that.keyed);
    }

}
