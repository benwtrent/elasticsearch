/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.investigation;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.RuntimeMappingsValidator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InvestigationSource implements Writeable, ToXContentObject {

    public static final ParseField INDEX = new ParseField("index");
    public static final ParseField QUERY = new ParseField("query");

    @SuppressWarnings({ "unchecked"})
    public static ConstructingObjectParser<InvestigationSource, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<InvestigationSource, Void> parser = new ConstructingObjectParser<>("investigation_source",
            ignoreUnknownFields, a -> new InvestigationSource(
                ((List<String>) a[0]).toArray(new String[0]),
                (QueryBuilder) a[1],
                (Map<String, Object>) a[2]));
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), INDEX);
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p),
            QUERY);
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(),
            SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD);
        return parser;
    }

    private final String[] index;
    private final QueryBuilder queryProvider;
    private final Map<String, Object> runtimeMappings;

    public InvestigationSource(String[] index, @Nullable QueryBuilder queryProvider, @Nullable Map<String, Object> runtimeMappings) {
        this.index = ExceptionsHelper.requireNonNull(index, INDEX);
        if (index.length == 0) {
            throw new IllegalArgumentException("source.index must specify at least one index");
        }
        if (Arrays.stream(index).anyMatch(Strings::isNullOrEmpty)) {
            throw new IllegalArgumentException("source.index must contain non-null and non-empty strings");
        }
        this.queryProvider = queryProvider == null ? QueryBuilders.matchAllQuery() : queryProvider;
        this.runtimeMappings = runtimeMappings == null ? Collections.emptyMap() : Collections.unmodifiableMap(runtimeMappings);
        RuntimeMappingsValidator.validate(this.runtimeMappings);
    }

    public InvestigationSource(StreamInput in) throws IOException {
        index = in.readStringArray();
        queryProvider = in.readNamedWriteable(QueryBuilder.class);
        runtimeMappings = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(index);
        out.writeNamedWriteable(queryProvider);
        out.writeMap(runtimeMappings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.array(INDEX.getPreferredName(), index);
        builder.field(QUERY.getPreferredName(), queryProvider);
        if (runtimeMappings.isEmpty() == false) {
            builder.field(SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD.getPreferredName(), runtimeMappings);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InvestigationSource other = (InvestigationSource) o;
        return Arrays.equals(index, other.index)
            && Objects.equals(queryProvider, other.queryProvider)
            && Objects.equals(runtimeMappings, other.runtimeMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.asList(index), queryProvider, runtimeMappings);
    }

    public String[] getIndex() {
        return index;
    }

    public QueryBuilder getQuery() {
        return queryProvider;
    }

    public Map<String, Object> getRuntimeMappings() {
        return runtimeMappings;
    }

}
