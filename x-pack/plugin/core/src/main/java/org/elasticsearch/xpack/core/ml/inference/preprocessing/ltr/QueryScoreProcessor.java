/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.ltr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LeafReaderContextExtractor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LeafReaderContextExtractorFactory;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LenientlyParsedPreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.StrictlyParsedPreProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryScoreProcessor implements LenientlyParsedPreProcessor, StrictlyParsedPreProcessor, LeafReaderContextExtractorFactory {
    private static final Logger logger = LogManager.getLogger(QueryScoreProcessor.class);

    public record QueryAndFieldName(QueryBuilder queryBuilder, String name, boolean templated) implements ToXContentObject, Writeable {

        private static final ParseField OUTPUT_FIELD = new ParseField("output_field");
        private static final ParseField QUERY = new ParseField("query");
        private static final ParseField TEMPLATED = new ParseField("templated");

        private static final ConstructingObjectParser<QueryAndFieldName, Void> STRICT_PARSER = createParser(false);
        private static final ConstructingObjectParser<QueryAndFieldName, Void> LENIENT_PARSER = createParser(true);

        private static ConstructingObjectParser<QueryAndFieldName, Void> createParser(boolean lenient) {
            ConstructingObjectParser<QueryAndFieldName, Void> parser = new ConstructingObjectParser<>(
                NAME.getPreferredName(),
                lenient,
                (a, c) -> new QueryAndFieldName((QueryBuilder) a[0], (String) a[1], a[2] != null && (Boolean) a[2])
            );
            // TODO use query provider for better BWC!
            parser.declareObject(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p),
                QUERY
            );
            parser.declareString(ConstructingObjectParser.constructorArg(), OUTPUT_FIELD);
            parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), TEMPLATED);
            return parser;
        }

        static QueryAndFieldName fromStream(StreamInput input) throws IOException {
            return new QueryAndFieldName(input.readNamedWriteable(QueryBuilder.class), input.readString(), input.readBoolean());
        }

        static QueryAndFieldName fromXContent(XContentParser parser, boolean lenient) {
            return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(OUTPUT_FIELD.getPreferredName(), name);
            builder.field(QUERY.getPreferredName(), queryBuilder);
            builder.field(TEMPLATED.getPreferredName(), templated);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(queryBuilder);
            out.writeString(name);
            out.writeBoolean(templated);
        }

        QueryAndFieldName rewriteWithParams(QueryRewriteContext ctx, Map<String, Object> params) throws IOException {
            if (templated) {
                if (ctx.convertToSearchExecutionContext() == null) {
                    return this;
                }
                Script script = new Script(
                    ScriptType.INLINE,
                    "mustache",
                    XContentHelper.toXContent(this, XContentType.JSON, false).utf8ToString(),
                    params
                );
                TemplateScript compiledScript = ctx.convertToSearchExecutionContext()
                    .compile(script, TemplateScript.CONTEXT)
                    .newInstance(script.getParams());
                String source = compiledScript.execute();
                logger.info("Compiled script for the query [{}]", source);
                try (XContentParser parser = JsonXContent.jsonXContent.createParser(ctx.getParserConfig(), source)) {
                    QueryAndFieldName newQueryAndFieldName = QueryAndFieldName.fromXContent(parser, true);
                    return new QueryAndFieldName(newQueryAndFieldName.queryBuilder.rewrite(ctx), newQueryAndFieldName.name, false);
                }
            }
            QueryBuilder rewritten = queryBuilder.rewrite(ctx);
            if (rewritten == queryBuilder) {
                return this;
            }
            return new QueryAndFieldName(rewritten, name, false);
        }
    }

    public static final ParseField NAME = new ParseField("query_score_processor");
    private static final ParseField QUERY_FEATURES = new ParseField("query_features");

    private static final ConstructingObjectParser<QueryScoreProcessor, PreProcessorParseContext> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<QueryScoreProcessor, PreProcessorParseContext> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<QueryScoreProcessor, PreProcessorParseContext> createParser(boolean lenient) {
        ConstructingObjectParser<QueryScoreProcessor, PreProcessorParseContext> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            (a, c) -> new QueryScoreProcessor((List<QueryAndFieldName>) a[0])
        );
        // TODO, would be cool if these could be adjusted or something at runtime with parameters!!!!
        parser.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> QueryAndFieldName.fromXContent(p, lenient),
            QUERY_FEATURES
        );
        return parser;
    }

    public static QueryScoreProcessor fromXContentStrict(XContentParser parser, PreProcessorParseContext context) {
        return STRICT_PARSER.apply(parser, context == null ? PreProcessorParseContext.DEFAULT : context);
    }

    public static QueryScoreProcessor fromXContentLenient(XContentParser parser, PreProcessorParseContext context) {
        return LENIENT_PARSER.apply(parser, context == null ? PreProcessorParseContext.DEFAULT : context);
    }

    private final List<QueryAndFieldName> queryAndFieldNames;

    public QueryScoreProcessor(List<QueryAndFieldName> queryAndFieldNames) {
        this.queryAndFieldNames = queryAndFieldNames;
    }

    public QueryScoreProcessor(StreamInput input) throws IOException {
        this.queryAndFieldNames = input.readList(QueryAndFieldName::fromStream);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(QUERY_FEATURES.getPreferredName(), queryAndFieldNames);
        builder.endObject();
        return builder;
    }

    @Override
    public List<String> inputFields() {
        return null;
    }

    @Override
    public List<String> outputFields() {
        return queryAndFieldNames.stream().map(QueryAndFieldName::name).toList();
    }

    @Override
    public void process(Map<String, Object> fields) {}

    @Override
    public Map<String, String> reverseLookup() {
        return null;
    }

    @Override
    public boolean isCustom() {
        return true;
    }

    @Override
    public String getOutputFieldType(String outputField) {
        return NumberFieldMapper.NumberType.DOUBLE.typeName();
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(queryAndFieldNames);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public QueryScoreProcessor rewriteWithParams(QueryRewriteContext ctx, Map<String, Object> params) throws IOException {
        List<QueryAndFieldName> rewritten = new ArrayList<>(queryAndFieldNames.size());
        boolean rewrote = false;
        for (QueryAndFieldName queryAndFieldName : queryAndFieldNames) {
            QueryAndFieldName maybeRewritten = queryAndFieldName.rewriteWithParams(ctx, params);
            if (maybeRewritten != queryAndFieldName) {
                rewrote = true;
            }
            rewritten.add(maybeRewritten);
        }
        if (rewrote) {
            return new QueryScoreProcessor(rewritten);
        }
        return this;
    }

    @Override
    public LeafReaderContextExtractor createLeafReaderExtractor(SearchExecutionContext executionContext) throws IOException {
        List<Weight> weights = new ArrayList<>(queryAndFieldNames.size());
        List<String> names = new ArrayList<>(queryAndFieldNames.size());
        for (QueryAndFieldName queryAndFieldName : queryAndFieldNames) {
            Query rewritten = executionContext.searcher().rewrite(queryAndFieldName.queryBuilder.toQuery(executionContext));
            weights.add(executionContext.searcher().createWeight(rewritten, ScoreMode.COMPLETE, 1f));
            names.add(queryAndFieldName.name);
        }
        return new MultiQueryLeafValueExtractor(names, weights);
    }
}
