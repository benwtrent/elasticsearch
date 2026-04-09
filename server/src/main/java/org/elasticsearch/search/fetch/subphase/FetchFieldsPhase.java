/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.index.mapper.LegacyTypeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A fetch sub-phase for high-level field retrieval. Given a list of fields, it
 * retrieves the field values through the relevant {@link org.elasticsearch.index.mapper.ValueFetcher}
 * and returns them as document fields.
 */
public final class FetchFieldsPhase implements FetchSubPhase {

    private static final String SLICE_FIELD = "_slice";

    private static final List<FieldAndFormat> DEFAULT_METADATA_FIELDS = List.of(
        new FieldAndFormat(IgnoredFieldMapper.NAME, null),
        new FieldAndFormat(RoutingFieldMapper.NAME, null),
        // will only be fetched when mapped (older archived indices)
        new FieldAndFormat(LegacyTypeFieldMapper.NAME, null)
    );

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        final FetchFieldsContext fetchFieldsContext = fetchContext.fetchFieldsContext();
        final StoredFieldsContext storedFieldsContext = fetchContext.storedFieldsContext();

        boolean fetchStoredFields = storedFieldsContext != null && storedFieldsContext.fetchFields();
        if (fetchFieldsContext == null && fetchStoredFields == false) {
            return null;
        }

        // NOTE: FieldFetcher for non-metadata fields, as well as `_id` and `_source`.
        // We need to retain `_id` and `_source` here to correctly populate the `StoredFieldSpecs` created by the
        // `FieldFetcher` constructor.
        final SearchExecutionContext searchExecutionContext = fetchContext.getSearchExecutionContext();
        final boolean sliceAliasEnabled = SliceIndexing.SLICE_FEATURE_FLAG.isEnabled();
        final boolean sliceRequested = sliceAliasEnabled
            && fetchFieldsContext != null
            && fetchFieldsContext.fields() != null
            && fetchFieldsContext.fields().stream().anyMatch(f -> SLICE_FIELD.equals(f.field));

        final FieldFetcher fieldFetcher = (fetchFieldsContext == null
            || fetchFieldsContext.fields() == null
            || fetchFieldsContext.fields().isEmpty())
                ? null
                : FieldFetcher.create(
                    searchExecutionContext,
                    fetchFieldsContext.fields()
                        .stream()
                        .filter(fieldAndFormat -> sliceAliasEnabled == false || SLICE_FIELD.equals(fieldAndFormat.field) == false)
                        .filter(
                            fieldAndFormat -> (searchExecutionContext.isMetadataField(fieldAndFormat.field) == false
                                || searchExecutionContext.getFieldType(fieldAndFormat.field).isStored() == false
                                || IdFieldMapper.NAME.equals(fieldAndFormat.field)
                                || SourceFieldMapper.NAME.equals(fieldAndFormat.field))
                        )
                        .toList()
                );

        // NOTE: Collect stored metadata fields requested via `fields` (in FetchFieldsContext) like for instance the _ignored source field
        final Set<FieldAndFormat> fetchContextMetadataFields = new HashSet<>();
        if (fetchFieldsContext != null && fetchFieldsContext.fields() != null && fetchFieldsContext.fields().isEmpty() == false) {
            for (final FieldAndFormat fieldAndFormat : fetchFieldsContext.fields()) {
                // NOTE: _id and _source are always retrieved anyway, no need to do it explicitly. See FieldsVisitor.
                if (SourceFieldMapper.NAME.equals(fieldAndFormat.field) || IdFieldMapper.NAME.equals(fieldAndFormat.field)) {
                    continue;
                }
                if (sliceAliasEnabled && SLICE_FIELD.equals(fieldAndFormat.field)) {
                    fetchContextMetadataFields.add(new FieldAndFormat(RoutingFieldMapper.NAME, fieldAndFormat.format));
                    continue;
                }
                if (searchExecutionContext.isMetadataField(fieldAndFormat.field)
                    && searchExecutionContext.getFieldType(fieldAndFormat.field).isStored()) {
                    fetchContextMetadataFields.add(fieldAndFormat);
                }
            }
        }

        final FieldFetcher metadataFieldFetcher;
        if (storedFieldsContext != null
            && storedFieldsContext.fieldNames() != null
            && storedFieldsContext.fieldNames().isEmpty() == false) {
            final Set<FieldAndFormat> metadataFields = new HashSet<>(DEFAULT_METADATA_FIELDS);
            for (final String storedField : storedFieldsContext.fieldNames()) {
                final Set<String> matchingFieldNames = searchExecutionContext.getMatchingFieldNames(storedField);
                for (final String matchingFieldName : matchingFieldNames) {
                    if (SourceFieldMapper.NAME.equals(matchingFieldName) || IdFieldMapper.NAME.equals(matchingFieldName)) {
                        continue;
                    }
                    final MappedFieldType fieldType = searchExecutionContext.getFieldType(matchingFieldName);
                    // NOTE: Exclude _ignored_source when requested via wildcard '*'
                    if (matchingFieldName.equals(IgnoredSourceFieldMapper.NAME) && Regex.isSimpleMatchPattern(storedField)) {
                        continue;
                    }
                    // NOTE: checking if the field is stored is required for backward compatibility reasons and to make
                    // sure we also handle here stored fields requested via `stored_fields`, which was previously a
                    // responsibility of StoredFieldsPhase.
                    if (searchExecutionContext.isMetadataField(matchingFieldName) && fieldType.isStored()) {
                        metadataFields.add(new FieldAndFormat(matchingFieldName, null));
                    }
                }
            }
            // NOTE: Include also metadata stored fields requested via `fields`
            metadataFields.addAll(fetchContextMetadataFields);
            metadataFieldFetcher = FieldFetcher.create(searchExecutionContext, metadataFields);
        } else {
            // NOTE: Include also metadata stored fields requested via `fields`
            final Set<FieldAndFormat> allMetadataFields = new HashSet<>(DEFAULT_METADATA_FIELDS);
            allMetadataFields.addAll(fetchContextMetadataFields);
            metadataFieldFetcher = FieldFetcher.create(searchExecutionContext, allMetadataFields);
        }
        return new FetchSubPhaseProcessor() {
            private StoredFields storedFields;

            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                if (fieldFetcher != null) {
                    fieldFetcher.setNextReader(readerContext);
                }
                metadataFieldFetcher.setNextReader(readerContext);
                storedFields = null;
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                if (fieldFetcher != null) {
                    return metadataFieldFetcher.storedFieldsSpec().merge(fieldFetcher.storedFieldsSpec());
                }
                return metadataFieldFetcher.storedFieldsSpec();
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                Map<String, DocumentField> fields = fieldFetcher != null
                    ? fieldFetcher.fetch(hitContext.source(), hitContext.docId())
                    : Collections.emptyMap();
                Map<String, DocumentField> metadataFields = metadataFieldFetcher.fetch(hitContext.source(), hitContext.docId());
                if (sliceRequested) {
                    DocumentField routing = metadataFields.get(RoutingFieldMapper.NAME);
                    if (routing != null) {
                        if (fields.isEmpty()) {
                            fields = new java.util.HashMap<>(1);
                        } else {
                            fields = new java.util.HashMap<>(fields);
                        }
                        fields.put(SLICE_FIELD, new DocumentField(SLICE_FIELD, routing.getValues(), routing.getIgnoredValues()));
                    } else {
                        List<Object> loadedRoutingValues = hitContext.loadedFields().get(RoutingFieldMapper.NAME);
                        if (loadedRoutingValues != null && loadedRoutingValues.isEmpty() == false) {
                            List<Object> values = loadedRoutingValues;
                            if (loadedRoutingValues.get(0) instanceof BytesRef) {
                                values = new ArrayList<>(loadedRoutingValues.size());
                                for (Object o : loadedRoutingValues) {
                                    if (o instanceof BytesRef bytesRef) {
                                        values.add(bytesRef.utf8ToString());
                                    } else {
                                        values.add(o);
                                    }
                                }
                            }
                            if (fields.isEmpty()) {
                                fields = new java.util.HashMap<>(1);
                            } else {
                                fields = new java.util.HashMap<>(fields);
                            }
                            fields.put(SLICE_FIELD, new DocumentField(SLICE_FIELD, values));
                        } else {
                            try {
                                SortedDocValues routingDocValues = org.apache.lucene.index.DocValues.getSorted(
                                    hitContext.reader(),
                                    RoutingFieldMapper.NAME
                                );
                                if (routingDocValues.advanceExact(hitContext.docId())) {
                                    BytesRef value = routingDocValues.lookupOrd(routingDocValues.ordValue());
                                    if (fields.isEmpty()) {
                                        fields = new java.util.HashMap<>(1);
                                    } else {
                                        fields = new java.util.HashMap<>(fields);
                                    }
                                    fields.put(SLICE_FIELD, new DocumentField(SLICE_FIELD, List.of(value.utf8ToString())));
                                }
                            } catch (IllegalStateException e) {
                                // Field does not have doc values; ignore.
                            }
                            if (fields.containsKey(SLICE_FIELD) == false) {
                                if (storedFields == null) {
                                    storedFields = hitContext.reader().storedFields();
                                }
                                final var values = new ArrayList<>(1);
                                storedFields.document(hitContext.docId(), new StoredFieldVisitor() {
                                    @Override
                                    public Status needsField(org.apache.lucene.index.FieldInfo fieldInfo) {
                                        return RoutingFieldMapper.NAME.equals(fieldInfo.name) ? Status.YES : Status.NO;
                                    }

                                    @Override
                                    public void binaryField(org.apache.lucene.index.FieldInfo fieldInfo, byte[] value) {
                                        values.add(new BytesRef(value).utf8ToString());
                                    }

                                    @Override
                                    public void stringField(org.apache.lucene.index.FieldInfo fieldInfo, String value) {
                                        values.add(value);
                                    }
                                });
                                if (values.isEmpty() == false) {
                                    if (fields.isEmpty()) {
                                        fields = new java.util.HashMap<>(1);
                                    } else {
                                        fields = new java.util.HashMap<>(fields);
                                    }
                                    fields.put(SLICE_FIELD, new DocumentField(SLICE_FIELD, values));
                                }
                            }
                        }
                    }
                }
                hitContext.hit().addDocumentFields(fields, metadataFields);
            }
        };
    }
}
