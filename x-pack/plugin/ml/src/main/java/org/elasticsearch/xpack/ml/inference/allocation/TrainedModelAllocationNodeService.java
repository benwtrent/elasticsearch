/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.allocation.TrainedModelAllocation;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAllocationStateAction;
import org.elasticsearch.xpack.ml.inference.deployment.DeploymentManager;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

public class TrainedModelAllocationNodeService implements ClusterStateListener {

    private static final String TASK_NAME = "trained_model_allocation";
    private static final Logger logger = LogManager.getLogger(TrainedModelAllocationNodeService.class);
    private final TrainedModelAllocationService trainedModelAllocationService;
    private final String currentNode;
    private final AtomicLong taskIdGenerator = new AtomicLong();
    private final DeploymentManager deploymentManager;
    private final TaskManager taskManager;
    private final Map<String, TrainedModelDeploymentTask> modelIdToTask;
    private final Deque<String> loadingModels;

    public TrainedModelAllocationNodeService (
        TrainedModelAllocationService trainedModelAllocationService,
        ClusterService clusterService,
        DeploymentManager deploymentManager,
        TaskManager taskManager
    ) {
        this.trainedModelAllocationService = trainedModelAllocationService;
        this.currentNode = clusterService.localNode().getId();
        this.deploymentManager = deploymentManager;
        this.taskManager = taskManager;
        this.modelIdToTask = new ConcurrentHashMap<>();
        this.loadingModels = new ConcurrentLinkedDeque<>();
        clusterService.addListener(this);
    }

    //TODO should this take a listener????
    public void stopDeployment(TrainedModelDeploymentTask task) {
        deploymentManager.stopDeployment(task);
        taskManager.unregister(task);
        modelIdToTask.remove(task.getModelId());
    }

    public void infer(
        TrainedModelDeploymentTask task,
        String input,
        TimeValue timeout,
        ActionListener<InferenceResults> listener
    ) {
        deploymentManager.infer(task, input, timeout, listener);
    }

    private TaskAwareRequest taskAwareRequest(StartTrainedModelDeploymentAction.TaskParams params) {
        final TrainedModelAllocationNodeService trainedModelAllocationNodeService = this;
        return new TaskAwareRequest() {
            final TaskId parentTaskId = new TaskId(TASK_NAME + "-" + params.getModelId(), taskIdGenerator.incrementAndGet());
            @Override
            public void setParentTask(TaskId taskId) {
                throw new UnsupportedOperationException("parent task id for model allocation tasks shouldn't change");
            }

            @Override
            public TaskId getParentTask() {
                return parentTaskId;
            }

            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                return new TrainedModelDeploymentTask(
                    id,
                    type,
                    action,
                    parentTaskId,
                    headers,
                    params,
                    trainedModelAllocationNodeService
                );
            }
        };
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.metadataChanged()) {
            // TODO allocate!!!!
            TrainedModelAllocationMetadata modelAllocationMetadata = TrainedModelAllocationMetadata.metadata(event.state());
        }
    }

    private void loadMode(StartTrainedModelDeploymentAction.TaskParams taskParams, ActionListener<Void> listener) {

    }

    private void handleLoadSuccess(String modelId) {
        logger.debug(
            () -> new ParameterizedMessage("[{}] model successfully loaded and ready for inference. Notifying master node", modelId)
        );
        trainedModelAllocationService.updateModelAllocationState(
            new UpdateTrainedModelAllocationStateAction.Request(
                modelId,
                currentNode,
                new TrainedModelAllocation.RoutingStateAndReason(TrainedModelAllocation.RoutingState.STARTED, "")
            ),
            ActionListener.wrap(
                success -> logger.debug(() -> new ParameterizedMessage("[{}] model is loaded and master notified", modelId)),
                // TODO: error could be that the model is not allocated to this node any longer or that the master is
                // temporarily out of reach
                // TODO: Re-do on master notification failure if its due to master not elected yet
                error -> logger.warn(() -> new ParameterizedMessage("[{}] model is loaded but failed to notify master", modelId), error)
            )
        );
    }

    private void handleLoadFailure(String modelId, Exception ex) {
        logger.error(() -> new ParameterizedMessage("[{}] model failed to load", modelId), ex);
        Task task = modelIdToTask.get(modelId);
        if (task != null) {
            taskManager.unregister(task);
        }
        // TODO determine if a retry is possible and add periodic retries
        trainedModelAllocationService.updateModelAllocationState(
            new UpdateTrainedModelAllocationStateAction.Request(
                modelId,
                currentNode,
                new TrainedModelAllocation.RoutingStateAndReason(
                    TrainedModelAllocation.RoutingState.FAILED,
                    ExceptionsHelper.unwrapCause(ex).getMessage()
                )
            ),
            ActionListener.wrap(
                success -> logger.debug(() -> new ParameterizedMessage("[{}] notified master of load failure", modelId)),
                // TODO: error could be that the model is not allocated to this node any longer or that the master is
                // temporarily out of reach
                // TODO: Re-do on master notification failure if its due to master not elected yet
                error -> logger.warn(() -> new ParameterizedMessage("[{}] failed to notify master of load failure", modelId), error)
            )
        );

    }
}
