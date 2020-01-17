/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.categorization;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class CategorizationOverride implements ToXContentObject, Writeable {
    public static final String NAME = "categorization_override";

    public static final ParseField CATEGORY_IDS = new ParseField("category_ids");
    public static final ParseField GROK_PATTERN = new ParseField("grok_pattern");
    public static final ParseField REGEX = new ParseField("regex");
    public static final ParseField TERMS = new ParseField("terms");
    public static final ParseField CATEGORY_NAME = new ParseField("category_name");
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);
    public static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);

    private final Set<Long> categoryIds;
    private final String grokPattern;
    private final String name;
    private final String regex;
    private final Set<String> terms;

    private static ObjectParser<CategorizationOverride.Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>(NAME,
            ignoreUnknownFields,
            CategorizationOverride.Builder::new);
        parser.declareLongArray(Builder::setCategoryIds, CATEGORY_IDS);
        parser.declareString(Builder::setGrokPattern, GROK_PATTERN);
        parser.declareString(Builder::setName, CATEGORY_NAME);
        parser.declareString(Builder::setRegex, REGEX);
        parser.declareStringArray(Builder::setTerms, TERMS);
        return parser;
    }

    public CategorizationOverride(String name, Set<Long> categoryIds, String grokPattern, String regex, Set<String> terms) {
        this.name = ExceptionsHelper.requireNonNull(name, CATEGORY_NAME);
        this.categoryIds = ExceptionsHelper.requireNonNull(categoryIds, CATEGORY_IDS);
        this.grokPattern = grokPattern;
        this.regex = regex;
        this.terms = terms == null ? null : Collections.unmodifiableSet(terms);
    }

    public CategorizationOverride(StreamInput in) throws IOException {
        this.name = in.readString();
        this.categoryIds = in.readSet(StreamInput::readLong);
        this.grokPattern = in.readOptionalString();
        this.regex = in.readOptionalString();
        this.terms = in.readBoolean() ? Collections.unmodifiableSet(in.readSet(StreamInput::readString)) : null;
    }

    public Set<Long> getCategoryIds() {
        return categoryIds;
    }

    public String getGrokPattern() {
        return grokPattern;
    }

    public String getName() {
        return name;
    }

    public Set<String> getTerms() {
        return terms;
    }

    public String getRegex() {
        return regex;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeCollection(categoryIds, StreamOutput::writeLong);
        out.writeOptionalString(grokPattern);
        out.writeOptionalString(regex);
        out.writeBoolean(terms != null);
        if (terms != null) {
            out.writeCollection(terms, StreamOutput::writeString);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CATEGORY_NAME.getPreferredName(), name);
        builder.field(CATEGORY_IDS.getPreferredName(), categoryIds);
        if(grokPattern != null) {
            builder.field(GROK_PATTERN.getPreferredName(), grokPattern);
        }
        if (regex != null) {
            builder.field(REGEX.getPreferredName(), regex);
        }
        if (terms != null) {
            builder.field(TERMS.getPreferredName(), terms);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CategorizationOverride that = (CategorizationOverride) o;
        return Objects.equals(categoryIds, that.categoryIds) &&
            Objects.equals(name, that.name) &&
            Objects.equals(grokPattern, that.grokPattern) &&
            Objects.equals(regex, that.regex) &&
            Objects.equals(terms, that.terms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(categoryIds, name, grokPattern, regex, terms);
    }

    public static class Builder {
        private Set<Long> categoryIds;
        private String grokPattern;
        private String name;
        private String regex;
        private Set<String> terms;

        private Builder setCategoryIds(List<Long> categoryIds) {
            return setCategoryIds(new HashSet<>(categoryIds));
        }

        public Builder setCategoryIds(Set<Long> categoryIds) {
            this.categoryIds = categoryIds;
            return this;
        }

        public Builder setGrokPattern(String grokPattern) {
            this.grokPattern = grokPattern;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setRegex(String regex) {
            this.regex = regex;
            return this;
        }

        private Builder setTerms(List<String> terms) {
            return setTerms(new HashSet<>(terms));
        }

        public Builder setTerms(Set<String> terms) {
            this.terms = terms;
            return this;
        }

        public CategorizationOverride build() {
            return new CategorizationOverride(name, categoryIds, grokPattern, regex, terms);
        }
    }
}
