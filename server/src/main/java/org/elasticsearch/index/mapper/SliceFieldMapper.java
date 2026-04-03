/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.SortedOrdinalsIndexFieldData;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.util.Collections;

public class SliceFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_slice";
    public static final String CONTENT_TYPE = "_slice";

    public static final FeatureFlag SLICE_FEATURE_FLAG = new FeatureFlag("slice_mapper");

    private static final SliceFieldMapper ENABLED_INSTANCE = new SliceFieldMapper(true);
    private static final SliceFieldMapper DISABLED_INSTANCE = new SliceFieldMapper(false);

    private static SliceFieldMapper toType(FieldMapper in) {
        return (SliceFieldMapper) in;
    }

    public static class Builder extends MetadataFieldMapper.Builder {

        private final IndexSortConfig indexSortConfig;
        private final Parameter<Boolean> enabled = Parameter.boolParam("enabled", false, m -> toType(m).enabled, false);

        public Builder(IndexSortConfig indexSortConfig) {
            super(NAME);
            this.indexSortConfig = indexSortConfig;
        }

        public Builder() {
            this(null);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { enabled };
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public SliceFieldMapper build() {
            if (enabled.getValue() && indexSortConfig != null && indexSortConfig.hasPrimarySortOnField(NAME) == false) {
                throw new IllegalArgumentException(
                    "[" + NAME + "] requires primary index sort field [" + NAME + "] when enabled; set [index.sort.field] to [" + NAME + "]"
                );
            }
            return enabled.getValue() ? ENABLED_INSTANCE : DISABLED_INSTANCE;
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(c -> {
        if (c.getIndexSettings().getMode() == IndexMode.TIME_SERIES) {
            throw new IllegalArgumentException("[" + NAME + "] is not supported in [index.mode=time_series]");
        }
        return new Builder(c.getIndexSettings().getIndexSortConfig());
    });

    static final class SliceFieldType extends MappedFieldType {
        static final SliceFieldType INSTANCE = new SliceFieldType();

        private SliceFieldType() {
            super(NAME, IndexType.docValuesOnly(), false, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isSearchable() {
            return false;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new QueryShardException(context, "The _slice field is not searchable");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new DocValueFetcher(docValueFormat(format, null), context.getForField(this, FielddataOperation.SEARCH));
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return new SortedOrdinalsIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                (dv, n) -> new DelegateDocValuesField(
                    new ScriptDocValues.Strings(new ScriptDocValues.StringsSupplier(FieldData.toString(dv))),
                    n
                )
            );
        }
    }

    private final boolean enabled;

    private SliceFieldMapper(boolean enabled) {
        super(SliceFieldType.INSTANCE);
        this.enabled = enabled;
    }

    @Override
    public void preParse(DocumentParserContext context) {
        String slice = context.slice();
        if (enabled) {
            if (slice == null) {
                throw new IllegalArgumentException("Slice is required when [" + NAME + "] is enabled");
            }
            context.doc().add(new SortedDocValuesField(NAME, new BytesRef(slice)));
        } else if (slice != null) {
            throw new IllegalArgumentException("Cannot provide [" + NAME + "] when it is disabled");
        }
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public boolean isEnabled() {
        return enabled;
    }
}
