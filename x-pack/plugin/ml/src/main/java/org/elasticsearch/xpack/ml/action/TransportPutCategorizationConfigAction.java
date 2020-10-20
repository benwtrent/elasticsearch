/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.action.PutCategorizationConfigAction;
import org.elasticsearch.xpack.core.ml.action.PutCategorizationConfigAction.Request;
import org.elasticsearch.xpack.core.ml.action.PutCategorizationConfigAction.Response;
import org.elasticsearch.xpack.core.ml.categorization.CategorizationConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.ml.categorization.persistence.CategorizationProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class TransportPutCategorizationConfigAction extends TransportMasterNodeAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutCategorizationConfigAction.class);

    private final XPackLicenseState licenseState;
    private final CategorizationProvider configProvider;
    private final JobConfigProvider jobConfigProvider;
    private final JobResultsProvider jobResultsProvider;
    private final Client client;

    @Inject
    public TransportPutCategorizationConfigAction(TransportService transportService, ActionFilters actionFilters,
                                                  XPackLicenseState licenseState, ThreadPool threadPool,
                                                  ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver,
                                                  CategorizationProvider configProvider, JobConfigProvider jobConfigProvider,
                                                  JobResultsProvider jobResultsProvider, Client client) {
        super(PutCategorizationConfigAction.NAME, transportService, clusterService, threadPool, actionFilters,
            Request::new, indexNameExpressionResolver, Response::new, ThreadPool.Names.SAME);
        this.licenseState = licenseState;
        this.configProvider = configProvider;
        this.jobConfigProvider = jobConfigProvider;
        this.jobResultsProvider = jobResultsProvider;
        this.client = client;
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {

        final CategorizationConfig.Builder builder = new CategorizationConfig.Builder(request.getCategorizationConfig());
        ActionListener<Boolean> storeConfigListener = ActionListener.wrap(
            r -> listener.onResponse(new Response(builder.build())),
            listener::onFailure
        );

        ActionListener<List<CategoryDefinition>> getCategoriesListener = ActionListener.wrap(
            categories -> {
                Set<Long> deadCategories = categories.stream()
                    .flatMapToLong(c -> Arrays.stream(c.getPreferredToCategories()))
                    .boxed()
                    .collect(Collectors.toSet());
                builder.setCategories(categories.stream()
                    .filter(c -> !deadCategories.contains(c.getCategoryId()))
                    .sorted(Comparator.comparing(CategoryDefinition::getNumMatches).reversed())
                    .collect(Collectors.toList()));
                configProvider.storeCategorizationConfig(builder.build(Instant.now()), storeConfigListener);
            },
            listener::onFailure
        );

        ActionListener<Job.Builder> getJobListener = ActionListener.wrap(
            jobBuilder -> {
                Job job = jobBuilder.build();
                if(job.getAnalysisConfig().getCategorizationFieldName() != null) {
                    builder.setCategorizationAnalyzerConfig(job.getAnalysisConfig().getCategorizationAnalyzerConfig());
                    builder.setCategorizationFilters(job.getAnalysisConfig().getCategorizationFilters());
                }
                jobResultsProvider.categoryDefinitions(job.getId(),
                    null,
                    null,
                    false,
                    0,
                    10_000,
                    (qp) -> getCategoriesListener.onResponse(qp.results()),
                    listener::onFailure,
                    client);
            },
            listener::onFailure
        );
        // TODO handle custom categories and analysis (allow JobID to be null)
        jobConfigProvider.getJob(request.getCategorizationConfig().getJobId(), getJobListener);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        if (licenseState.checkFeature(XPackLicenseState.Feature.MACHINE_LEARNING)) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
    }
}
