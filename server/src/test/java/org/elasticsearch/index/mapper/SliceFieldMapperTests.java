/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SliceFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected Settings getIndexSettings() {
        if (SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled() == false) {
            return super.getIndexSettings();
        }
        return Settings.builder().put(super.getIndexSettings()).put("index.sort.field", SliceFieldMapper.NAME).build();
    }

    @Override
    protected String fieldName() {
        return SliceFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled();
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        if (SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled() == false) {
            return;
        }
        checker.registerConflictCheck(
            "enabled",
            topMapping(b -> b.startObject(SliceFieldMapper.NAME).field("enabled", false).endObject()),
            topMapping(b -> b.startObject(SliceFieldMapper.NAME).field("enabled", true).endObject()),
            d -> {}
        );
        checker.registerConflictCheck(
            "enabled",
            topMapping(b -> b.startObject(SliceFieldMapper.NAME).field("enabled", true).endObject()),
            topMapping(b -> b.startObject(SliceFieldMapper.NAME).field("enabled", false).endObject()),
            d -> {}
        );
    }

    public void testIncludeInObjectNotAllowed() throws Exception {
        assumeTrue("slice mapper feature flag must be enabled", SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled());
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> docMapper.parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field(SliceFieldMapper.NAME, 1).endObject()),
                    XContentType.JSON
                )
            )
        );
        assertThat(e.getCause().getMessage(), containsString("Field [_slice] is a metadata field and cannot be added inside a document"));
    }

    public void testSliceEnabledRequiresSliceValue() throws Exception {
        assumeTrue("slice mapper feature flag must be enabled", SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled());
        DocumentMapper docMapper = createDocumentMapper(
            topMapping(b -> b.startObject(SliceFieldMapper.NAME).field("enabled", true).endObject())
        );
        Exception e = expectThrows(DocumentParsingException.class, () -> docMapper.parse(source(b -> b.field("field", "value"))));
        assertThat(e.getCause().getMessage(), containsString("Slice is required"));
    }

    public void testSliceDisabledRejectsProvidedSliceValue() throws Exception {
        assumeTrue("slice mapper feature flag must be enabled", SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled());
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));
        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject());
        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> docMapper.parse(new SourceToParse("1", source, XContentType.JSON, null, "s1"))
        );
        assertThat(e.getCause().getMessage(), containsString("Cannot provide [_slice]"));
    }

    public void testSliceEnabledIndexesDocValues() throws Exception {
        assumeTrue("slice mapper feature flag must be enabled", SliceFieldMapper.SLICE_FEATURE_FLAG.isEnabled());
        DocumentMapper docMapper = createDocumentMapper(
            topMapping(b -> b.startObject(SliceFieldMapper.NAME).field("enabled", true).endObject())
        );
        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject());
        ParsedDocument doc = docMapper.parse(new SourceToParse("1", source, XContentType.JSON, null, "s1"));
        IndexableField sliceField = doc.rootDoc().getField(SliceFieldMapper.NAME);
        assertThat(sliceField, notNullValue());
        assertThat(sliceField.binaryValue().utf8ToString(), equalTo("s1"));
    }
}
