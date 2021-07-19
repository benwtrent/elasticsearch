/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAllocationAction;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAllocationAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAllocationStateAction;
import org.elasticsearch.xpack.core.ml.inference.allocation.TrainedModelAllocation;

import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TrainedModelAllocationService {

    private static final Logger logger = LogManager.getLogger(TrainedModelAllocationService.class);

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public TrainedModelAllocationService(Client client, ClusterService clusterService, ThreadPool threadPool) {
        this.client = new OriginSettingClient(client, ML_ORIGIN);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    public void updateModelAllocationState(
        UpdateTrainedModelAllocationStateAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        client.execute(UpdateTrainedModelAllocationStateAction.INSTANCE, request, listener);
    }

    public void createNewModelAllocation(
        StartTrainedModelDeploymentAction.TaskParams taskParams,
        ActionListener<CreateTrainedModelAllocationAction.Response> listener
    ) {
        client.execute(CreateTrainedModelAllocationAction.INSTANCE, new CreateTrainedModelAllocationAction.Request(taskParams), listener);
    }

    public void deleteModelAllocation(
        String modelId,
        ActionListener<AcknowledgedResponse> listener
    ) {
        client.execute(DeleteTrainedModelAllocationAction.INSTANCE, new DeleteTrainedModelAllocationAction.Request(modelId), listener);
    }

    public void waitForAllocationCondition(
        final String modelId,
        final Predicate<TrainedModelAllocation> predicate,
        final @Nullable TimeValue timeout,
        final WaitForAllocationListener listener
    ) {
        final Predicate<ClusterState> clusterStatePredicate = clusterState ->
            predicate.test(TrainedModelAllocationMetadata.allocationForModelId(clusterState, modelId).orElse(null));

        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, timeout, logger, threadPool.getThreadContext());
        final ClusterState clusterState = observer.setAndGetObservedState();
        if (clusterStatePredicate.test(clusterState)) {
            listener.onResponse(TrainedModelAllocationMetadata.allocationForModelId(clusterState, modelId).orElse(null));
        } else {
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    listener.onResponse(TrainedModelAllocationMetadata.allocationForModelId(clusterState, modelId).orElse(null));
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onTimeout(timeout);
                }
            }, clusterStatePredicate);
        }
    }

    public interface WaitForAllocationListener extends ActionListener<TrainedModelAllocation> {
        default void onTimeout(TimeValue timeout) {
            onFailure(new IllegalStateException("Timed out when waiting for trained model allocation after " + timeout));
        }
    }

}
