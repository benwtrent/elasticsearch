/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.queries;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class SemanticQueryBuilder extends AbstractQueryBuilder<SemanticQueryBuilder> {

    public static final String NAME = "semantic_text";
    public static final ParseField QUERY = new ParseField("query");

    private final String fieldName;
    private final String query;
    private SetOnce<float[]> vectorSupplier;

    public SemanticQueryBuilder(String fieldName, String query) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a fieldName");
        }
        if (query == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + QUERY.getPreferredName() + " value");
        }

        this.fieldName = fieldName;
        this.query = query;
    }

    public SemanticQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.query = in.readString();
    }

    private SemanticQueryBuilder(SemanticQueryBuilder other, SetOnce<float[]> vectorSupplier) {
        this.fieldName = other.fieldName;
        this.boost = other.boost;
        this.queryName = other.queryName;
        this.query = other.query;
        this.vectorSupplier = vectorSupplier;
    }

    String getFieldName() {
        return fieldName;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_8_0;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (vectorSupplier != null) {
            throw new IllegalStateException("token supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        out.writeString(query);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(QUERY.getPreferredName(), query);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (vectorSupplier != null) {
            if (vectorSupplier.get() == null) {
                return this;
            }
            return new KnnVectorQueryBuilder(fieldName, vectorSupplier.get(), 100, null).boost(boost).queryName(queryName);
        }

        SetOnce<float[]> inferenceResults = new SetOnce<>();
        queryRewriteContext.registerAsyncAction((c, l) -> {
            OriginSettingClient client = new OriginSettingClient(c, ML_ORIGIN);
            FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
            request.includeUnmapped(false);
            request.fields(fieldName);
            // TODO Get actual indices from search request
            request.indices("*");
            client.execute(FieldCapabilitiesAction.INSTANCE, request, ActionListener.wrap(fc -> {
                Map<String, FieldCapabilities> caps = fc.get().get(fieldName);
                FieldCapabilities capabilities = caps.values().stream().findFirst().orElseThrow();
                String modelId = capabilities.meta().get("model").stream().findFirst().orElseThrow();
                InferModelAction.Request inferRequest = InferModelAction.Request.forTextInput(
                    modelId,
                    new EmptyConfigUpdate(),
                    List.of(query)
                );
                client.execute(InferModelAction.INSTANCE, inferRequest, ActionListener.wrap(ir -> {
                    TextEmbeddingResults results = (TextEmbeddingResults) ir.getInferenceResults().get(0);
                    inferenceResults.set(results.getInferenceAsFloat());
                    l.onResponse(null);
                }, l::onFailure));
            }, l::onFailure));
        });

        return new SemanticQueryBuilder(this, inferenceResults);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        throw new IllegalStateException("text_expansion should have been rewritten to another query type");
    }

    @Override
    protected boolean doEquals(SemanticQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) && Objects.equals(query, other.query);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, query);
    }

    public static SemanticQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        String query = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (QUERY.match(currentFieldName, parser.getDeprecationHandler())) {
                            query = parser.text();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[" + NAME + "] query does not support [" + currentFieldName + "]"
                            );
                        }
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                        );
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = parser.currentName();
                query = parser.text();
            }
        }

        if (query == null) {
            throw new ParsingException(parser.getTokenLocation(), "No text specified for text query");
        }

        if (fieldName == null) {
            throw new ParsingException(parser.getTokenLocation(), "No fieldname specified for query");
        }

        SemanticQueryBuilder queryBuilder = new SemanticQueryBuilder(fieldName, query);
        queryBuilder.queryName(queryName);
        queryBuilder.boost(boost);
        return queryBuilder;
    }
}
