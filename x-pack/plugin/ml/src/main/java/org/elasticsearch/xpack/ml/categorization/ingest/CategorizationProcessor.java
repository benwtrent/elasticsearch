/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.categorization.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xpack.core.ml.action.CategorizeTextAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class CategorizationProcessor extends AbstractProcessor {

    public static final String TYPE = "categorization";
    public static final String CATEGORIZATION_CONFIG_ID = "categorization_config_id";
    public static final String TARGET_FIELD = "target_field";
    public static final String TEXT_FIELD = "text_field";
    private static final String DEFAULT_TARGET_FIELD = "ml.categorization";

    private final Client client;
    private final String categorizationConfigId;

    private final String targetField;
    private final String textField;
    private final boolean includeGrok;

    public CategorizationProcessor(Client client,
                                   String tag,
                                   String targetField,
                                   String categorizationConfigId,
                                   String textField,
                                   String description,
                                   boolean includeGrok) {
        super(tag, description);
        this.client = ExceptionsHelper.requireNonNull(client, "client");
        this.targetField = ExceptionsHelper.requireNonNull(targetField, TARGET_FIELD);
        this.categorizationConfigId = ExceptionsHelper.requireNonNull(categorizationConfigId, CATEGORIZATION_CONFIG_ID);
        this.textField = ExceptionsHelper.requireNonNull(textField, TEXT_FIELD);
        this.includeGrok = includeGrok;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        executeAsyncWithOrigin(client,
            ML_ORIGIN,
            CategorizeTextAction.INSTANCE,
            this.buildRequest(ingestDocument),
            ActionListener.wrap(
                r -> {
                    mutateDocument(r, ingestDocument);
                    handler.accept(ingestDocument, null);
                },
                e -> handler.accept(ingestDocument, e)
            ));
    }

    CategorizeTextAction.Request buildRequest(IngestDocument ingestDocument) {
        Map<String, Object> fields = new HashMap<>(ingestDocument.getSourceAndMetadata());
        String textField = MapHelper.dig(this.textField, fields).toString();
        CategorizeTextAction.Request request = new CategorizeTextAction.Request(textField, categorizationConfigId, includeGrok);
        return request;
    }

    void mutateDocument(CategorizeTextAction.Response response, IngestDocument ingestDocument) {
        response.writeToDoc(targetField, ingestDocument);
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        throw new UnsupportedOperationException("should never be called");
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        private static final Logger logger = LogManager.getLogger(Factory.class);

        private final Client client;

        public Factory(Client client) {
            this.client = client;
        }

        @Override
        public Processor create(Map<String, Processor.Factory> processorFactories,
                                String tag,
                                String description,
                                Map<String, Object> config) {
            String categorizationConfigId = ConfigurationUtils.readStringProperty(TYPE, tag, config, CATEGORIZATION_CONFIG_ID);
            String defaultTargetField = tag == null ? DEFAULT_TARGET_FIELD : DEFAULT_TARGET_FIELD + "." + tag;
            String targetField = ConfigurationUtils.readStringProperty(TYPE, tag, config, TARGET_FIELD, defaultTargetField);
            String textField = ConfigurationUtils.readStringProperty(TYPE, tag, config, TEXT_FIELD, "text");
            boolean includeGrok = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "include_grok", false);
            return new CategorizationProcessor(client,
                tag,
                targetField,
                categorizationConfigId,
                textField,
                description,
                includeGrok);
        }
    }
}
