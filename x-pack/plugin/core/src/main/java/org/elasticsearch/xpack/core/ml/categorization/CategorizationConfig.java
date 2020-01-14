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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class CategorizationConfig implements ToXContentObject, Writeable {

    public static final String NAME = "categorization_config";

    public static final ParseField ID = new ParseField("categorization_config_id");
    public static final ParseField JOB_ID = new ParseField("job_id");
    public static final ParseField OVERRIDES = new ParseField("overrides");
    public static final ParseField CATEGORIES = new ParseField("categories");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField CREATE_TIME = new ParseField("create_time");
    public static final ParseField UPDATE_TIME = new ParseField("update_time");
    public static final ParseField CATEGORIZATION_ANALYZER = CategorizationAnalyzerConfig.CATEGORIZATION_ANALYZER;
    public static final ParseField CATEGORIZATION_FILTERS = new ParseField("categorization_filters");

    public static final ObjectParser<CategorizationConfig.Builder, Void> LENIENT_PARSER = createParser(true);
    public static final ObjectParser<CategorizationConfig.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<CategorizationConfig.Builder, Void> parser = new ObjectParser<>(NAME,
            ignoreUnknownFields,
            CategorizationConfig.Builder::new);
        parser.declareString(CategorizationConfig.Builder::setCategorizationConfigId, ID);
        parser.declareString(CategorizationConfig.Builder::setJobId, JOB_ID);
        parser.declareObjectArray(CategorizationConfig.Builder::setOverrides,
            (p, c) -> ignoreUnknownFields ? CategorizationOverride.LENIENT_PARSER.apply(p, c).build() : CategorizationOverride.STRICT_PARSER.apply(p, c).build(),
            OVERRIDES);
        parser.declareString(CategorizationConfig.Builder::setDescription, DESCRIPTION);
        parser.declareField(CategorizationConfig.Builder::setCreateTime,
            (p, c) -> TimeUtils.parseTimeFieldToInstant(p, CREATE_TIME.getPreferredName()),
            CREATE_TIME,
            ObjectParser.ValueType.VALUE);
        parser.declareObjectArray(CategorizationConfig.Builder::setCategories,
            (p, c) -> ignoreUnknownFields ? CategoryDefinition.LENIENT_PARSER.apply(p, c) : CategoryDefinition.STRICT_PARSER.apply(p, c),
            CATEGORIES);
        parser.declareField(CategorizationConfig.Builder::setUpdateTime,
            (p, c) -> TimeUtils.parseTimeFieldToInstant(p, UPDATE_TIME.getPreferredName()),
            UPDATE_TIME,
            ObjectParser.ValueType.VALUE);
        parser.declareStringArray(CategorizationConfig.Builder::setCategorizationFilters, CATEGORIZATION_FILTERS);
        // This one is nasty - the syntax for analyzers takes either names or objects at many levels, hence it's not
        // possible to simply declare whether the field is a string or object and a completely custom parser is required
        parser.declareField(CategorizationConfig.Builder::setCategorizationAnalyzerConfig,
            (p, c) -> CategorizationAnalyzerConfig.buildFromXContentFragment(p, ignoreUnknownFields),
            CATEGORIZATION_ANALYZER, ObjectParser.ValueType.OBJECT_OR_STRING);
        return parser;
    }

    public static CategorizationConfig.Builder fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    private final String categorizationConfigId;
    private final String jobId;
    private final List<CategorizationOverride> overrides;
    private final List<CategoryDefinition> categories;
    private final String description;
    private final Instant createTime;
    private final Instant updateTime;
    private final CategorizationAnalyzerConfig categorizationAnalyzerConfig;
    private final List<String> categorizationFilters;

    public CategorizationConfig(String categorizationConfigId,
                                String jobId,
                                List<CategorizationOverride> overrides,
                                List<CategoryDefinition> categories,
                                String description,
                                Instant createTime,
                                Instant updateTime,
                                CategorizationAnalyzerConfig categorizationAnalyzerConfig,
                                List<String> categorizationFilters) {
        this.categorizationConfigId = categorizationConfigId;
        this.jobId = jobId;
        this.overrides = overrides == null ? Collections.emptyList() : Collections.unmodifiableList(overrides);
        this.categories = categories == null ? Collections.emptyList() : Collections.unmodifiableList(categories);
        this.description = description;
        this.createTime = createTime == null ? null : Instant.ofEpochMilli(createTime.toEpochMilli());
        this.updateTime = updateTime == null ? null : Instant.ofEpochMilli(updateTime.toEpochMilli());
        this.categorizationAnalyzerConfig = categorizationAnalyzerConfig;
        this.categorizationFilters = categorizationFilters == null ? Collections.emptyList() : Collections.unmodifiableList(categorizationFilters);
    }

    public CategorizationConfig(StreamInput in) throws IOException {
        this.categorizationConfigId = in.readString();
        this.jobId = in.readOptionalString();
        this.overrides = in.readList(CategorizationOverride::new);
        this.categories = in.readList(CategoryDefinition::new);
        this.description = in.readOptionalString();
        this.createTime = in.readOptionalInstant();
        this.updateTime = in.readOptionalInstant();
        this.categorizationAnalyzerConfig = in.readBoolean() ? new CategorizationAnalyzerConfig(in) : null;
        this.categorizationFilters = in.readStringList();
    }

    public CategorizationAnalyzerConfig getCategorizationAnalyzerConfig() {
        return categorizationAnalyzerConfig;
    }

    public List<String> getCategorizationFilters() {
        return categorizationFilters;
    }

    public String getCategorizationConfigId() {
        return categorizationConfigId;
    }

    public String getJobId() {
        return jobId;
    }

    public List<CategorizationOverride> getOverrides() {
        return overrides;
    }

    public List<CategoryDefinition> getCategories() {
        return categories;
    }

    public String getDescription() {
        return description;
    }

    public Instant getCreateTime() {
        return createTime;
    }

    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.categorizationConfigId);
        out.writeOptionalString(this.jobId);
        out.writeList(overrides);
        out.writeList(categories);
        out.writeOptionalString(description);
        out.writeOptionalInstant(createTime);
        out.writeOptionalInstant(updateTime);
        out.writeBoolean(categorizationAnalyzerConfig != null);
        if (categorizationAnalyzerConfig != null) {
            categorizationAnalyzerConfig.writeTo(out);
        }
        out.writeStringCollection(categorizationFilters);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), categorizationConfigId);
        if (jobId != null) {
            builder.field(JOB_ID.getPreferredName(), jobId);
        }
        builder.field(OVERRIDES.getPreferredName(), overrides);
        builder.field(CATEGORIES.getPreferredName(), categories);
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        if (createTime != null) {
            builder.timeField(CREATE_TIME.getPreferredName(), CREATE_TIME.getPreferredName() + "_string", createTime.toEpochMilli());
        }
        if (updateTime != null) {
            builder.timeField(UPDATE_TIME.getPreferredName(), UPDATE_TIME.getPreferredName() + "_string", updateTime.toEpochMilli());
        }
        if (categorizationAnalyzerConfig != null) {
            categorizationAnalyzerConfig.toXContent(builder, params);
        }
        builder.field(CATEGORIZATION_FILTERS.getPreferredName(), categorizationFilters);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CategorizationConfig that = (CategorizationConfig) o;
        return Objects.equals(categorizationConfigId, that.categorizationConfigId) &&
            Objects.equals(jobId, that.jobId) &&
            Objects.equals(overrides, that.overrides) &&
            Objects.equals(description, that.description) &&
            Objects.equals(createTime, that.createTime) &&
            Objects.equals(updateTime, that.updateTime) &&
            Objects.equals(description, that.description) &&
            Objects.equals(categories, that.categories) &&
            Objects.equals(categorizationAnalyzerConfig, that.categorizationAnalyzerConfig) &&
            Objects.equals(categorizationFilters, that.categorizationFilters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(categorizationConfigId,
            jobId,
            overrides,
            createTime,
            updateTime,
            description,
            overrides,
            categories,
            categorizationAnalyzerConfig,
            categorizationFilters);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String categorizationConfigId;
        private String jobId;
        private List<CategorizationOverride> overrides;
        private List<CategoryDefinition> categories;
        private String description;
        private Instant createTime;
        private Instant updateTime;
        private CategorizationAnalyzerConfig categorizationAnalyzerConfig;
        private List<String> categorizationFilters;

        public Builder() { }

        public Builder(CategorizationConfig config) {
            this.categorizationConfigId = config.getCategorizationConfigId();
            this.jobId = config.getJobId();
            this.overrides = config.overrides;
            this.categories = config.categories;
            this.description = config.description;
            this.createTime = config.createTime;
            this.updateTime = config.updateTime;
            this.categorizationAnalyzerConfig = config.categorizationAnalyzerConfig;
            this.categorizationFilters = config.categorizationFilters;
        }

        public Builder setCategorizationConfigId(String categorizationConfigId) {
            this.categorizationConfigId = categorizationConfigId;
            return this;
        }

        public String getCategorizationConfigId() {
            return categorizationConfigId;
        }

        public Builder setJobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder setOverrides(List<CategorizationOverride> overrides) {
            this.overrides = overrides;
            return this;
        }

        public Builder setCategories(List<CategoryDefinition> categories) {
            this.categories = categories;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setCreateTime(Instant createTime) {
            this.createTime = createTime;
            return this;
        }

        public Builder setUpdateTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        public Builder setCategorizationAnalyzerConfig(CategorizationAnalyzerConfig categorizationAnalyzerConfig) {
            this.categorizationAnalyzerConfig = categorizationAnalyzerConfig;
            return this;
        }

        public Builder setCategorizationFilters(List<String> categorizationFilters) {
            this.categorizationFilters = categorizationFilters;
            return this;
        }

        public CategorizationConfig build(Instant createTime) {
            this.createTime = createTime;
            Set<Long> categoryIds = categories.stream().map(CategoryDefinition::getCategoryId).collect(Collectors.toSet());
            if (categoryIds.size() != categories.size()) {
                throw ExceptionsHelper.badRequestException("[{}] categories must all have unique ids", categorizationConfigId);
            }
            if (overrides != null) {
                List<String> overridesMissingCategories = overrides.stream()
                    .filter(override -> categoryIds.containsAll(override.getCategoryIds()))
                    .map(CategorizationOverride::getName)
                    .collect(Collectors.toList());
                if (overridesMissingCategories.isEmpty() == false) {
                    throw ExceptionsHelper.badRequestException("[{}] overrides contain category ids that do not exist {}",
                        categorizationConfigId,
                        overridesMissingCategories);
                }
            }
            return new CategorizationConfig(categorizationConfigId, jobId, overrides, categories, description, createTime, updateTime, categorizationAnalyzerConfig, categorizationFilters);
        }

        public CategorizationConfig build() {
            return new CategorizationConfig(categorizationConfigId, jobId, overrides, categories, description, createTime, updateTime, categorizationAnalyzerConfig, categorizationFilters);
        }
    }
}
