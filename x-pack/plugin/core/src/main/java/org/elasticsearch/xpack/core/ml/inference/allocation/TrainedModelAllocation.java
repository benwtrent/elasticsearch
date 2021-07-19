/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.allocation;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

// TODO implement better diffable logic so that whole diff does not need to be serialized if only one part changes
/**
 * Trained model allocation object that contains allocation options and the allocation routing table
 */
public class TrainedModelAllocation extends AbstractDiffable<TrainedModelAllocation> implements
    Diffable<TrainedModelAllocation>,
    ToXContentObject {

    private static final ParseField ROUTING_TABLE = new ParseField("routing_table");
    private static final ParseField INDEX_NAME = new ParseField("index_name");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<TrainedModelAllocation, Void> PARSER = new ConstructingObjectParser<>(
        "trained_model_allocation",
        true,
        a -> new TrainedModelAllocation((String)a[0], (Map<String, RoutingStateAndReason>)a[1])
    );
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_NAME);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapOrdered(), ROUTING_TABLE);
    }

    private final String indexName;
    private final Map<String, RoutingStateAndReason> nodeRoutingTable;

    public static TrainedModelAllocation fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    TrainedModelAllocation(String indexName, Map<String, RoutingStateAndReason> nodeRoutingTable) {
        this.indexName = ExceptionsHelper.requireNonNull(indexName, INDEX_NAME);
        this.nodeRoutingTable = ExceptionsHelper.requireNonNull(nodeRoutingTable, ROUTING_TABLE);
    }

    public TrainedModelAllocation(StreamInput in) throws IOException {
        this.indexName = in.readString();
        nodeRoutingTable = in.readOrderedMap(StreamInput::readString, RoutingStateAndReason::new);
    }

    public boolean routedToNode(String nodeId) {
        return nodeRoutingTable.containsKey(nodeId);
    }

    public Map<String, RoutingStateAndReason> getNodeRoutingTable() {
        return Collections.unmodifiableMap(nodeRoutingTable);
    }

    public String getIndexName() {
        return indexName;
    }

    // TODO add support for other roles?
    // NOTE, whatever determines allocation should not be dynamically setable on the node
    // Otherwise allocation logic might fail
    public boolean canAllocateToNode(DiscoveryNode node) {
        return node.getRoles().contains(DiscoveryNodeRole.ML_ROLE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelAllocation that = (TrainedModelAllocation) o;
        return Objects.equals(nodeRoutingTable, that.nodeRoutingTable) && Objects.equals(indexName, that.indexName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeRoutingTable, indexName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_NAME.getPreferredName(), indexName);
        builder.field(ROUTING_TABLE.getPreferredName(), nodeRoutingTable);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeMap(nodeRoutingTable, StreamOutput::writeString, (o, w) -> w.writeTo(o));
    }

    public static class Builder {
        private final Map<String, RoutingStateAndReason> nodeRoutingTable;
        private final String indexName;
        private boolean isChanged;

        public static Builder fromAllocation(TrainedModelAllocation allocation) {
            return new Builder(allocation.indexName, allocation.nodeRoutingTable);
        }

        public static Builder empty(String indexName) {
            return new Builder(indexName);
        }

        private Builder(String indexName, Map<String, RoutingStateAndReason> nodeRoutingTable) {
            this.indexName = indexName;
            this.nodeRoutingTable = new LinkedHashMap<>(nodeRoutingTable);
        }

        private Builder(String indexName) {
            this.nodeRoutingTable = new LinkedHashMap<>();
            this.indexName = indexName;
        }

        public Builder addNewRoutingEntry(String nodeId) {
            if (nodeRoutingTable.containsKey(nodeId)) {
                return this;
            }
            isChanged = true;
            nodeRoutingTable.put(nodeId, RoutingStateAndReason.NEW_ROUTE);
            return this;
        }

        public Builder updateExistingRoutingEntry(String nodeId, RoutingStateAndReason state) {
            RoutingStateAndReason stateAndReason = nodeRoutingTable.get(nodeId);
            if (stateAndReason == null) {
                return this;
            }
            if (stateAndReason.equals(state)) {
                return this;
            }
            nodeRoutingTable.put(nodeId, state);
            isChanged = true;
            return this;
        }

        public Builder removeRoutingEntry(String nodeId) {
            if(nodeRoutingTable.remove(nodeId) != null) {
                isChanged = true;
            }
            return this;
        }

        // TODO add support for other roles?
        // NOTE, whatever determines allocation should not be dynamically setable on the node
        // Otherwise allocation logic might fail
        public boolean canAllocateToNode(DiscoveryNode node) {
            return node.getRoles().contains(DiscoveryNodeRole.ML_ROLE);
        }

        public boolean isChanged() {
            return isChanged;
        }

        public TrainedModelAllocation build() {
            return new TrainedModelAllocation(indexName, nodeRoutingTable);
        }
    }

    public enum RoutingState {
        INITIALIZING, STARTED, FAILED;

        public static RoutingState fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static class RoutingStateAndReason implements ToXContentObject, Writeable {

        public static RoutingStateAndReason NEW_ROUTE = new RoutingStateAndReason(
            RoutingState.INITIALIZING,
            "new allocation initializing"
        );

        private static final ParseField REASON = new ParseField("reason");
        private static final ParseField ROUTING_STATE = new ParseField("routing_state");

        private static final ConstructingObjectParser<RoutingStateAndReason, Void> PARSER = new ConstructingObjectParser<>(
            "trained_model_routing_state",
            a -> new RoutingStateAndReason(RoutingState.fromString((String)a[0]), (String)a[1])
        );
        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), ROUTING_STATE);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REASON);
        }

        private final String reason;
        private final RoutingState state;

        public RoutingStateAndReason(RoutingState state, String reason) {
            this.state = ExceptionsHelper.requireNonNull(state, ROUTING_STATE);
            this.reason = reason;
        }

        public RoutingStateAndReason(StreamInput in) throws IOException {
            this.state = in.readEnum(RoutingState.class);
            this.reason = in.readOptionalString();
        }

        public String getReason() {
            return reason;
        }

        public RoutingState getState() {
            return state;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(state);
            out.writeOptionalString(reason);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ROUTING_STATE.getPreferredName(), state);
            if (reason != null) {
                builder.field(REASON.getPreferredName(), reason);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RoutingStateAndReason that = (RoutingStateAndReason) o;
            return Objects.equals(reason, that.reason) && state == that.state;
        }

        @Override
        public int hashCode() {
            return Objects.hash(reason, state);
        }

    }

}
