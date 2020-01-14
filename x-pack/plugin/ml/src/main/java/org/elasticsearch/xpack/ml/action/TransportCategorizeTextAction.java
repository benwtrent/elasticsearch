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
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.action.CategorizeTextAction;
import org.elasticsearch.xpack.core.ml.action.GetCategorizationConfigsAction;
import org.elasticsearch.xpack.core.ml.action.CategorizeTextAction.Request;
import org.elasticsearch.xpack.core.ml.action.CategorizeTextAction.Response;
import org.elasticsearch.xpack.core.ml.categorization.CategorizationConfig;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.ml.categorization.Categorizer;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportCategorizeTextAction extends HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportCategorizeTextAction.class);

    private final XPackLicenseState licenseState;
    private final Client client;
    private final AnalysisRegistry analysisRegistry;

    private final Map<String, Categorizer> dumbCache = new HashMap<>();

    @Inject
    public TransportCategorizeTextAction(TransportService transportService, ActionFilters actionFilters,
                                         XPackLicenseState licenseState, Client client, AnalysisRegistry analysisRegistry) {
        super(CategorizeTextAction.NAME, transportService, actionFilters, Request::new);
        this.licenseState = licenseState;
        this.client = client;
        this.analysisRegistry = analysisRegistry;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        if (licenseState.isMachineLearningAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
        if (request.isCacheCategorization() && dumbCache.containsKey(request.getCategorizationConfigId())) {
            listener.onResponse(new Response(dumbCache.get(request.getCategorizationConfigId()).getCategory(request.getText())));
            return;
        }
        ActionListener<GetCategorizationConfigsAction.Response> categorizationConfigListener = ActionListener.wrap(
            categorizationConfig -> {
                CategorizationConfig config = categorizationConfig.getResources().results().get(0);
                Categorizer categorizer = new Categorizer(config.getCategories(), createCategorizationAnalyzer(config, analysisRegistry));
                if (request.isCacheCategorization()) {
                    dumbCache.put(request.getCategorizationConfigId(), categorizer);
                }
                listener.onResponse(new Response(categorizer.getCategory(request.getText())));
            },
            listener::onFailure
        );

        executeAsyncWithOrigin(client, ML_ORIGIN, GetCategorizationConfigsAction.INSTANCE, new GetCategorizationConfigsAction.Request(request.getCategorizationConfigId()), categorizationConfigListener);
    }

    private static CategorizationAnalyzer createCategorizationAnalyzer(CategorizationConfig config, AnalysisRegistry analysisRegistry) throws IOException {
        CategorizationAnalyzerConfig categorizationAnalyzerConfig = config.getCategorizationAnalyzerConfig();
        if (categorizationAnalyzerConfig == null) {
            categorizationAnalyzerConfig =
                CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(config.getCategorizationFilters());
        }
        return new CategorizationAnalyzer(analysisRegistry, categorizationAnalyzerConfig);
    }
}
