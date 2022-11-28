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
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LenientlyParsedPreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.StrictlyParsedPreProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TermStats implements LenientlyParsedPreProcessor, StrictlyParsedPreProcessor {

    public static final ParseField NAME = new ParseField("term_stats");

    private static final ParseField TERMS = new ParseField("terms");
    private static final ParseField FIELDS = new ParseField("fields");
    private static final ParseField STATS = new ParseField("stats");

    private static final ConstructingObjectParser<TermStats, PreProcessorParseContext> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<TermStats, PreProcessorParseContext> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<TermStats, PreProcessorParseContext> createParser(boolean lenient) {
        ConstructingObjectParser<TermStats, PreProcessorParseContext> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            (a, c) -> new TermStats((List<String>) a[0], (List<String>) a[1], (List<String>) a[2])
        );
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), TERMS);
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), FIELDS);
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), STATS);
        return parser;
    }

    public static TermStats fromXContentStrict(XContentParser parser, PreProcessorParseContext context) {
        return STRICT_PARSER.apply(parser, context == null ? PreProcessorParseContext.DEFAULT : context);
    }

    public static TermStats fromXContentLenient(XContentParser parser, PreProcessorParseContext context) {
        return LENIENT_PARSER.apply(parser, context == null ? PreProcessorParseContext.DEFAULT : context);
    }

    private final List<String> terms;
    private final List<String> fields;
    private final List<String> stats;

    public TermStats(List<String> terms, List<String> fields, List<String> stats) {
        this.terms = terms;
        this.fields = fields;
        this.stats = stats;
    }

    public TermStats(StreamInput in) throws IOException {
        this.terms = in.readStringList();
        this.fields = in.readStringList();
        this.stats = in.readStringList();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TERMS.getPreferredName(), terms);
        builder.field(FIELDS.getPreferredName(), fields);
        builder.field(STATS.getPreferredName(), terms);
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
        out.writeStringCollection(terms);
        out.writeStringCollection(fields);
        out.writeStringCollection(stats);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    private List<String> allPossibleOutputFeatureNames() {
        List<String> outputNames = new ArrayList<>(terms.size() * fields.size() * stats.size());
        for (String field : fields) {
            for (String term : terms) {
                for (String stat : stats) {
                    outputNames.add(field + "_" + term + "_" + stat);
                }
            }
        }
        return outputNames;
    }

    Map<String, Map<String, Set<Term>>> getTerms(SearchExecutionContext context) throws IOException {
        Map<String, Map<String, Set<Term>>> fieldToTerms = Maps.newMapWithExpectedSize(fields.size());
        for (String field : fields) {
            MappedFieldType fieldType = context.getFieldType(field);
            Analyzer analyzer = fieldType.getTextSearchInfo().searchAnalyzer();
            if (analyzer == null) {
                throw new IllegalArgumentException("No analyzer found for field [" + fieldType + "]");
            }
            Map<String, Set<Term>> termToTerm = Maps.newMapWithExpectedSize(terms.size());
            for (String termString : terms) {
                Set<Term> termSet = new HashSet<>();
                try (TokenStream ts = analyzer.tokenStream(field, termString)) {
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
        return fieldToTerms;
    }
}
