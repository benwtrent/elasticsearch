/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class CategorizationTokenTree implements ToXContentObject, Accountable, TreeNodeFactory {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(CategorizationTokenTree.class);

    static final BytesRef WILD_CARD = new BytesRef("*");
    private static final Logger LOGGER = LogManager.getLogger(CategorizationTokenTree.class);

    private final int maxDepth;
    private final int maxChildren;
    private final double similarityThreshold;
    private final AtomicLong idGen = new AtomicLong();
    // TODO statically allocate an array like DuplicateByteSequenceSpotter ???
    private final Map<Integer, TreeNode> root = new HashMap<>();
    private long sizeInBytes;

    public CategorizationTokenTree(int maxChildren, int maxDepth, double similarityThreshold) {
        assert maxChildren > 0 && maxDepth >= 0;
        this.maxChildren = maxChildren;
        this.maxDepth = maxDepth;
        this.similarityThreshold = similarityThreshold;
        this.sizeInBytes = SHALLOW_SIZE;
    }

    public List<InternalCategorizationAggregation.Bucket> toBuckets(Map<Long, InternalAggregations> internalAggregations) {
        return root.values()
            .stream()
            .flatMap(c -> c.getAllChildrenLogGroups().stream())
            .map(lg -> new InternalCategorizationAggregation.Bucket(
                new InternalCategorizationAggregation.BucketKey(lg.getLogEvent()),
                lg.getCount(),
                internalAggregations.get(lg.getId())
            ))
            .sorted()
            .collect(Collectors.toList());
    }

    void mergeSmallestChildren() {
        root.values().forEach(TreeNode::collapseTinyChildren);
    }

    public LogGroup parseLogLine(final BytesRef[] logTokens) {
        return parseLogLine(logTokens, 1);
    }

    public LogGroup parseLogLineConst(final BytesRef[] logTokens) {
        TreeNode currentNode = this.root.get(logTokens.length);
        if (currentNode == null) { // we are missing an entire sub tree. New log length found
            return null;
        }
        return currentNode.getLogGroup(logTokens);
    }

    public LogGroup parseLogLine(final BytesRef[] logTokens, long docCount) {
        LOGGER.trace("parsing tokens [{}]", Strings.arrayToDelimitedString(logTokens, " "));
        TreeNode currentNode = this.root.get(logTokens.length);
        if (currentNode == null) { // we are missing an entire sub tree. New log length found
            currentNode = newNode(docCount, 0, logTokens);
            this.root.put(logTokens.length, currentNode);
        } else {
            currentNode.incCount(docCount);
        }
        return currentNode.addLog(logTokens, docCount, this);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<Integer, TreeNode> entry : root.entrySet()) {
            builder.field(entry.getKey().toString(), entry.getValue());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public TreeNode newNode(long docCount, int tokenPos, BytesRef[] tokens) {
        TreeNode node = tokenPos < maxDepth - 1 && tokenPos < tokens.length ?
            new TreeNode.InnerTreeNode(docCount, tokenPos, maxChildren) :
            new TreeNode.LeafTreeNode(docCount, similarityThreshold);
        // The size of the node + entry (since it is a map entry) + extra reference for priority queue
        sizeInBytes += node.ramBytesUsed() + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        return node;
    }

    @Override
    public LogGroup newGroup(long docCount, BytesRef[] logTokens) {
        LogGroup group = new LogGroup(logTokens, docCount, idGen.incrementAndGet());
        // Get the regular size bytes from the LogGroup and how much it costs to reference it
        sizeInBytes += group.ramBytesUsed() + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        return group;
    }

    @Override
    public long ramBytesUsed() {
        return sizeInBytes;
    }

}
