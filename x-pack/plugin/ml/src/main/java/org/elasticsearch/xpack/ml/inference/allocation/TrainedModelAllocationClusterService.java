/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAllocationStateAction;
import org.elasticsearch.xpack.core.ml.inference.allocation.TrainedModelAllocation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TrainedModelAllocationClusterService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(TrainedModelAllocationClusterService.class);

    private final ClusterService clusterService;

    public TrainedModelAllocationClusterService(Settings settings, ClusterService clusterService) {
        this.clusterService = clusterService;
        // Only nodes that can possibly be master nodes really need this service running
        if (DiscoveryNode.isMasterNode(settings)) {
            clusterService.addListener(this);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() && shouldAllocateModels(event)) {
            clusterService.submitStateUpdateTask("allocating models to nodes", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    TrainedModelAllocationMetadata previousState = TrainedModelAllocationMetadata.metadata(currentState);
                    TrainedModelAllocationMetadata.Builder builder = TrainedModelAllocationMetadata.builder(currentState);
                    Map<String, List<String>> removedNodeModelLookUp = new HashMap<>();
                    // TODO: make more efficient, right now this is O(nm) where n = sizeof(models) and m = sizeof(nodes)
                    // It could probably be O(max(n, m))
                    // Add nodes and keep track of currently routed nodes
                    for (Map.Entry<String, TrainedModelAllocation> modelAllocationEntry : previousState.modelAllocations().entrySet()) {
                        for (DiscoveryNode node : currentState.getNodes()) {
                            // Only add the route if the node is NOT shutting down, this would be a weird case of the node
                            // just being added to the cluster and immediately shutting down...
                            if (isNodeShuttingDown(currentState, node.getId()) == false
                                && modelAllocationEntry.getValue().canAllocateToNode(node)) {
                                builder.addNode(modelAllocationEntry.getKey(), node.getId());
                            }
                        }
                        for (String nodeId : modelAllocationEntry.getValue().getNodeRoutingTable().keySet()) {
                            removedNodeModelLookUp.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(modelAllocationEntry.getKey());
                        }
                    }

                    // Remove nodes
                    currentState.getNodes().forEach(d -> {
                        // If a node is NOT shutting down, we should NOT remove its route
                        if (isNodeShuttingDown(currentState, d.getId()) == false) {
                            removedNodeModelLookUp.remove(d.getId());
                        }
                    });
                    for (Map.Entry<String, List<String>> nodeToModels : removedNodeModelLookUp.entrySet()) {
                        final String nodeId = nodeToModels.getKey();
                        for (String modelId : nodeToModels.getValue()) {
                            builder.removeNode(modelId, nodeId);
                        }
                    }
                    return update(event.state(), builder);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn("failed to allocate models", e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.trace("updated model allocations based on node changes in the cluster");
                }
            });
        }
    }

    public void updateModelRoutingTable(
        UpdateTrainedModelAllocationStateAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final String modelId = request.getModelId();
        final String nodeId = request.getNodeId();
        clusterService.submitStateUpdateTask("updating model routing for node allocation", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                TrainedModelAllocationMetadata metadata = TrainedModelAllocationMetadata.metadata(currentState);
                TrainedModelAllocation existingAllocation = metadata.getModelAllocation(modelId);
                if (existingAllocation == null) {
                    throw new ResourceNotFoundException("allocation for model with id [" + modelId + "] not found");
                }
                if (existingAllocation.routedToNode(nodeId)) {
                    throw new ResourceNotFoundException(
                        "allocation for model with id [" + modelId + "] is not routed to node [" + nodeId + "]"
                    );
                }
                TrainedModelAllocationMetadata.Builder builder = TrainedModelAllocationMetadata.builder(currentState);
                builder.updateAllocation(modelId, nodeId, request.getRoutingState());
                return update(currentState, builder);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            }
        });
    }

    public void createNewModelAllocation(
        StartTrainedModelDeploymentAction.TaskParams params,
        ActionListener<TrainedModelAllocation> listener
    ) {
        clusterService.submitStateUpdateTask("create model allocation", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                TrainedModelAllocationMetadata.Builder builder = TrainedModelAllocationMetadata.builder(currentState);
                if (builder.hasModel(params.getModelId())) {
                    throw new ResourceAlreadyExistsException("allocation for model with id [" + params.getModelId() + "] already exist");
                }
                builder.addNewAllocation(params.getModelId(), params.getIndex());
                //TODO here is where to handle allocation options if possible
                TrainedModelAllocation.Builder allocationBuilder = TrainedModelAllocation.Builder.empty(params.getIndex());
                currentState.getNodes()
                    .getAllNodes()
                    .stream()
                    .filter(allocationBuilder::canAllocateToNode)
                    .map(DiscoveryNode::getId)
                    .forEach(nodeId -> builder.addNode(params.getModelId(), nodeId));
                return update(currentState, builder);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(TrainedModelAllocationMetadata.metadata(newState).getModelAllocation(params.getModelId()));
            }
        });
    }

    public void removeModelAllocation(String modelId, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("delete model allocation", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                TrainedModelAllocationMetadata.Builder builder = TrainedModelAllocationMetadata.builder(currentState);
                if (builder.hasModel(modelId) == false) {
                    throw new ResourceNotFoundException("allocation for model with id [" + modelId + "] not found");
                }
                return update(currentState, builder.removeAllocation(modelId));
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            }
        });
    }

    private static ClusterState update(ClusterState currentState, TrainedModelAllocationMetadata.Builder modelAllocations) {
        if (modelAllocations.isChanged()) {
            return ClusterState.builder(currentState).metadata(
                Metadata.builder(currentState.metadata()).putCustom(TrainedModelAllocationMetadata.NAME, modelAllocations.build())
            ).build();
        } else {
            return currentState;
        }
    }

    boolean shouldAllocateModels(final ClusterChangedEvent event) {
        // If there are no allocations created at all, there is nothing to update
        final TrainedModelAllocationMetadata allocationMetadata = event.state().getMetadata().custom(TrainedModelAllocationMetadata.NAME);
        if (allocationMetadata == null) {
            return false;
        }
        boolean masterChanged = event.previousState().nodes().isLocalNodeElectedMaster() == false;
        if(event.nodesChanged() || masterChanged) {
            DiscoveryNodes.Delta nodesDelta = event.nodesDelta();
            TrainedModelAllocationMetadata previousState = TrainedModelAllocationMetadata.metadata(event.previousState());
            for (Map.Entry<String, TrainedModelAllocation> modelAllocationEntry : previousState.modelAllocations().entrySet()) {
                for (DiscoveryNode removed : nodesDelta.removedNodes()) {
                    if (modelAllocationEntry.getValue().routedToNode(removed.getId())) {
                        return true;
                    }
                }
                for (DiscoveryNode added : nodesDelta.addedNodes()) {
                    if (modelAllocationEntry.getValue().canAllocateToNode(added)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Returns true if the given node is marked as shutting down with any
     * shutdown type.
     */
    static boolean isNodeShuttingDown(final ClusterState state, final String nodeId) {
        // Right now we make no distinction between the type of shutdown, but maybe in the future we might?
        return NodesShutdownMetadata.getShutdowns(state)
            .map(NodesShutdownMetadata::getAllNodeMetadataMap)
            .map(allNodes -> allNodes.get(nodeId))
            .isPresent();
    }

}
