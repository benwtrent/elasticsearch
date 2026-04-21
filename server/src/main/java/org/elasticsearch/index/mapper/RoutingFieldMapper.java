/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedOrdinalsIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.util.Collections;
import java.util.Map;

public class RoutingFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_routing";
    public static final String CONTENT_TYPE = "_routing";

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(useDocValues).init(this);
    }

    public static class Defaults {
        public static final boolean REQUIRED = false;
    }

    private static RoutingFieldMapper toType(FieldMapper in) {
        return (RoutingFieldMapper) in;
    }

    public static class Builder extends MetadataFieldMapper.Builder {

        final Parameter<Boolean> required = Parameter.boolParam("required", false, m -> toType(m).required, Defaults.REQUIRED);
        private final boolean useDocValues;

        protected Builder(boolean useDocValues) {
            super(NAME);
            this.useDocValues = useDocValues;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { required };
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public RoutingFieldMapper build() {
            if (useDocValues) {
                if (required.isConfigured() && required.getValue() == false) {
                    throw new IllegalArgumentException("[_routing.required] cannot be set to [false] when [index.slice.enabled] is true");
                }
                return RoutingFieldMapper.get(true, true);
            }
            return RoutingFieldMapper.get(required.getValue(), false);
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(c -> {
        boolean sliceEnabled = c.getIndexSettings().isSliceEnabled() && SliceIndexing.SLICE_FEATURE_FLAG.isEnabled();
        return new Builder(sliceEnabled);
    });

    public static final MappedFieldType FIELD_TYPE = new RoutingFieldType(false);
    private static final MappedFieldType DOC_VALUES_FIELD_TYPE = new RoutingFieldType(true);

    static final class RoutingFieldType extends StringFieldType {

        private RoutingFieldType(boolean hasDocValues) {
            // TODO we need to ensure we have doc values skipper aka SortedDocValuesField.indexedField
            super(NAME, IndexType.terms(true, hasDocValues), true, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new StoredValueFetcher(context.lookup(), NAME);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return new SortedOrdinalsIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n)
            );
        }
    }

    /**
     * Should we require {@code routing} on CRUD operations?
     */
    private final boolean required;
    private final boolean useDocValues;

    private static final RoutingFieldMapper REQUIRED = new RoutingFieldMapper(true, false);
    private static final RoutingFieldMapper NOT_REQUIRED = new RoutingFieldMapper(false, false);
    private static final RoutingFieldMapper REQUIRED_WITH_DOC_VALUES = new RoutingFieldMapper(true, true);
    private static final RoutingFieldMapper NOT_REQUIRED_WITH_DOC_VALUES = new RoutingFieldMapper(false, true);

    private static final Map<String, NamedAnalyzer> ANALYZERS = Map.of(NAME, Lucene.KEYWORD_ANALYZER);

    public static RoutingFieldMapper get(boolean required, boolean useDocValues) {
        if (useDocValues) {
            return required ? REQUIRED_WITH_DOC_VALUES : NOT_REQUIRED_WITH_DOC_VALUES;
        }
        return required ? REQUIRED : NOT_REQUIRED;
    }

    private RoutingFieldMapper(boolean required, boolean useDocValues) {
        super(useDocValues ? DOC_VALUES_FIELD_TYPE : FIELD_TYPE);
        this.required = required;
        this.useDocValues = useDocValues;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return ANALYZERS;
    }

    /**
     * Should we require {@code routing} on CRUD operations?
     */
    public boolean required() {
        return this.required;
    }

    @Override
    public void preParse(DocumentParserContext context) {
        String routing = context.routing();
        if (routing != null) {
            context.doc().add(new StringField(fieldType().name(), routing, Field.Store.YES));
            if (useDocValues) {
                context.doc().add(SortedDocValuesField.indexedField(fieldType().name(), new BytesRef(routing)));
            } else {
                context.addToFieldNames(fieldType().name());
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
