/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.AbstractTransportGetResourcesAction;
import org.elasticsearch.xpack.core.ml.action.GetCategorizationConfigsAction;
import org.elasticsearch.xpack.core.ml.categorization.CategorizationConfig;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TransportGetCategorizationConfigsAction extends AbstractTransportGetResourcesAction<CategorizationConfig,
    GetCategorizationConfigsAction.Request, GetCategorizationConfigsAction.Response> {

    @Inject
    public TransportGetCategorizationConfigsAction(TransportService transportService, ActionFilters actionFilters, Client client,
                                                   NamedXContentRegistry xContentRegistry) {
        super(GetCategorizationConfigsAction.NAME, transportService, actionFilters, GetCategorizationConfigsAction.Request::new, client,
            xContentRegistry);
    }

    @Override
    protected ParseField getResultsField() {
        return GetCategorizationConfigsAction.Response.RESULTS_FIELD;
    }

    @Override
    protected String[] getIndices() {
        return new String[] { AnomalyDetectorsIndex.configIndexName() };
    }

    @Override
    protected CategorizationConfig parse(XContentParser parser) {
        return CategorizationConfig.LENIENT_PARSER.apply(parser, null).build();
    }

    @Override
    protected ResourceNotFoundException notFoundException(String resourceId) {
        return ExceptionsHelper.missingCategorizationConfig(resourceId);
    }

    @Override
    protected void doExecute(Task task, GetCategorizationConfigsAction.Request request,
                             ActionListener<GetCategorizationConfigsAction.Response> listener) {
        searchResources(request, ActionListener.wrap(
            queryPage -> listener.onResponse(new GetCategorizationConfigsAction.Response(queryPage)),
            listener::onFailure
        ));
    }

    @Nullable
    protected QueryBuilder additionalQuery() {
        return null;
    }

    @Override
    protected String executionOrigin() {
        return ML_ORIGIN;
    }

    @Override
    protected String extractIdFromResource(CategorizationConfig config) {
        return config.getCategorizationConfigId();
    }
}
