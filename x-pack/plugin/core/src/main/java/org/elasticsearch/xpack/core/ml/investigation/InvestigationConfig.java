/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.investigation;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class InvestigationConfig implements ToXContentObject, Writeable {

    public static ParseField KEY_INDICATOR = new ParseField("key_indicator");
    public static ParseField TERMS = new ParseField("terms");

    public static ObjectParser<InvestigationConfig, Void> PARSER = new ObjectParser<InvestigationConfig, Void>(
        "investigation_config",
        InvestigationConfig::new
    );

    static {
        PARSER.declareString(InvestigationConfig::setKeyIndicator, KEY_INDICATOR);
        PARSER.declareStringArray(InvestigationConfig::setTerms, TERMS);
    }

    private String keyIndicator;
    private List<String> terms;

    public InvestigationConfig() {

    }

    public String getKeyIndicator() {
        return keyIndicator;
    }

    public InvestigationConfig setKeyIndicator(String keyIndicator) {
        this.keyIndicator = keyIndicator;
        return this;
    }

    public List<String> getTerms() {
        return terms;
    }

    public InvestigationConfig setTerms(List<String> terms) {
        this.terms = terms;
        return this;
    }

    public InvestigationConfig(StreamInput in) throws IOException {
        this.keyIndicator = in.readString();
        this.terms = in.readStringList();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(keyIndicator);
        out.writeStringCollection(terms);
    }
}
