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
import org.elasticsearch.xpack.ml.categorization.CategorizerLoadingService;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportCategorizeTextAction extends HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportCategorizeTextAction.class);

    private final XPackLicenseState licenseState;
    private final AnalysisRegistry analysisRegistry;
    private final CategorizerLoadingService categorizerLoadingService;

    @Inject
    public TransportCategorizeTextAction(TransportService transportService, ActionFilters actionFilters,
                                         XPackLicenseState licenseState, AnalysisRegistry analysisRegistry,
                                         CategorizerLoadingService categorizerLoadingService) {
        super(CategorizeTextAction.NAME, transportService, actionFilters, Request::new);
        this.licenseState = licenseState;
        this.analysisRegistry = analysisRegistry;
        this.categorizerLoadingService = categorizerLoadingService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        if (licenseState.isMachineLearningAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
        ActionListener<Categorizer> getCategorizer = ActionListener.wrap(
            categorizer -> listener.onResponse(categorizer.getCategory(request.getText(), request.isIncludeGrok())),
            listener::onFailure
        );

        this.categorizerLoadingService.getCategorizer(request.getCategorizationConfigId(), analysisRegistry, getCategorizer);
    }

}
