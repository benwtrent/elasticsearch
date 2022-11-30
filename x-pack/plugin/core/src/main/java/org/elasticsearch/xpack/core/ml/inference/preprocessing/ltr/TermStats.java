/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.ltr;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TermStats implements LenientlyParsedPreProcessor, StrictlyParsedPreProcessor, LeafReaderContextExtractorFactory {

    public static final ParseField NAME = new ParseField("term_stats");

    private static final ParseField TERMS = new ParseField("terms");
    record TermAndName(String name, String term) implements Writeable, ToXContentObject {
        private static final ParseField TERM = new ParseField("term");
        private static final ParseField OUTPUT_NAME = new ParseField("output_name");
        private static final ConstructingObjectParser<TermAndName, Void> PARSER = new ConstructingObjectParser<>(
            "term_and_name",
            true,
            (a, c) -> new TermAndName((String)a[0], (String)a[1])
        );
        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), OUTPUT_NAME);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TERM);
        }
        static TermAndName fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        static TermAndName fromStream(StreamInput input) throws IOException {
            return new TermAndName(input.readString(), input.readString());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(OUTPUT_NAME.getPreferredName(), name);
            builder.field(TERM.getPreferredName(), term);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(term);
        }
    }
    private static final ParseField FIELDS = new ParseField("fields");
    private static final ParseField STATS = new ParseField("stats");
    private static final ParseField TEMPLATED = new ParseField("templated");

    private static final ConstructingObjectParser<TermStats, PreProcessorParseContext> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<TermStats, PreProcessorParseContext> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<TermStats, PreProcessorParseContext> createParser(boolean lenient) {
        ConstructingObjectParser<TermStats, PreProcessorParseContext> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            (a, c) -> new TermStats((List<TermAndName>) a[0], (List<String>) a[1], (List<String>) a[2], a[3] != null && (Boolean) a[3])
        );
        parser.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> TermAndName.fromXContent(p), TERMS);
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), FIELDS);
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), STATS);
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), TEMPLATED);
        return parser;
    }

    public static TermStats fromXContentStrict(XContentParser parser, PreProcessorParseContext context) {
        return STRICT_PARSER.apply(parser, context == null ? PreProcessorParseContext.DEFAULT : context);
    }

    public static TermStats fromXContentLenient(XContentParser parser, PreProcessorParseContext context) {
        return LENIENT_PARSER.apply(parser, context == null ? PreProcessorParseContext.DEFAULT : context);
    }

    private final List<TermAndName> terms;
    private final List<String> fields;
    private final List<String> stats;
    private final boolean templated;

    public TermStats(List<TermAndName> terms, List<String> fields, List<String> stats, boolean templated) {
        this.terms = terms;
        this.fields = fields;
        this.stats = stats;
        this.templated = templated;
    }

    public TermStats(StreamInput in) throws IOException {
        this.terms = in.readList(TermAndName::fromStream);
        this.fields = in.readStringList();
        this.stats = in.readStringList();
        this.templated = in.readBoolean();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TERMS.getPreferredName(), terms);
        builder.field(FIELDS.getPreferredName(), fields);
        builder.field(STATS.getPreferredName(), stats);
        builder.field(TEMPLATED.getPreferredName(), templated);
        builder.endObject();
        return builder;
    }

    @Override
    public List<String> inputFields() {
        return null;
    }

    @Override
    public List<String> outputFields() {
        return allPossibleOutputFeatureNames();
    }

    @Override
    public void process(Map<String, Object> fields) {

    }

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
        out.writeCollection(terms);
        out.writeStringCollection(fields);
        out.writeStringCollection(stats);
        out.writeBoolean(templated);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    private List<String> allPossibleOutputFeatureNames() {
        List<String> outputNames = new ArrayList<>(terms.size() * fields.size() * stats.size());
        for (String field : fields) {
            for (var term : terms) {
                for (String stat : stats) {
                    outputNames.add(field + "_" + term.name + "_" + stat);
                }
            }
        }
        return outputNames;
    }

    @Override
    public TermStats rewriteWithParams(QueryRewriteContext ctx, Map<String, Object> params) throws IOException {
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
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(ctx.getParserConfig(), source)) {
                TermStats newTermStats = TermStats.fromXContentLenient(parser, PreProcessorParseContext.DEFAULT);
                return new TermStats(newTermStats.terms, newTermStats.fields, newTermStats.stats, false);
            }
        }
        return this;
    }

    @Override
    public LeafReaderContextExtractor createLeafReaderExtractor(SearchExecutionContext executionContext) throws IOException {
        Map<String, Map<TermAndName, Set<Term>>> fieldToTerms = Maps.newMapWithExpectedSize(fields.size());
        for (String field : fields) {
            MappedFieldType fieldType = executionContext.getFieldType(field);
            Analyzer analyzer = fieldType.getTextSearchInfo().searchAnalyzer();
            if (analyzer == null) {
                throw new IllegalArgumentException("No analyzer found for field [" + fieldType + "]");
            }
            Map<TermAndName, Set<Term>> termToTerm = Maps.newMapWithExpectedSize(terms.size());
            for (var termString : terms) {
                Set<Term> termSet = new HashSet<>();
                try (TokenStream ts = analyzer.tokenStream(field, termString.term)) {
                    TermToBytesRefAttribute termAtt = ts.getAttribute(TermToBytesRefAttribute.class);
                    ts.reset();
                    while (ts.incrementToken()) {
                        termSet.add(new Term(field, termAtt.getBytesRef()));
                    }
                }
                termToTerm.put(termString, termSet);
            }
            fieldToTerms.put(field, termToTerm);
        }
        return new TermStatsLeafValueExtractor(
            fieldToTerms,
            fields.size() * terms.size() * stats.size(),
            stats,
            executionContext.searcher()
        );
    }
}
