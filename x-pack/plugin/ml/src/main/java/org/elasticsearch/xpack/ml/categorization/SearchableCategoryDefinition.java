/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.categorization;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.xpack.core.ml.action.CategorizeTextAction;
import org.elasticsearch.xpack.core.ml.categorization.CategorizationOverride;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

final class SearchableCategoryDefinition implements Accountable {

    private static final Logger logger = LogManager.getLogger(SearchableCategoryDefinition.class);
    static final int NOT_MATCHED = -1;
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(SearchableCategoryDefinition.class);
    private static final long SHALLOW_SIZE_PATTERN = RamUsageEstimator.shallowSizeOfInstance(Pattern.class);
    private static final long SHALLOW_SIZE_GROK = RamUsageEstimator.shallowSizeOfInstance(Grok.class);

    private final List<Set<String>> terms;
    private final long[] maxLengths;
    private final long[] categoryIds;
    private final Pattern[] compiledPattern;
    private final Grok[] grok;
    private final String name;

    SearchableCategoryDefinition(CategoryDefinition definition, Map<String, String> grokPatternBank, MatcherWatchdog watchdog) {
        this.terms = Collections.singletonList(new HashSet<>(Arrays.asList(definition.getTerms().split(" "))));
        this.maxLengths = new long[]{ definition.getMaxMatchingLength() };
        this.categoryIds = new long[] { definition.getCategoryId() };
        this.compiledPattern = new Pattern[] { Pattern.compile(definition.getRegex()) };
        this.name = definition.getRegex();
        this.grok = definition.getGrokPattern() == null ?
            null :
            new Grok[] { new Grok(grokPatternBank, definition.getGrokPattern(), watchdog) };
    }

    // TODO adjust to allow the override regex
    SearchableCategoryDefinition(List<CategoryDefinition> definitions,
                                 CategorizationOverride override,
                                 Map<String, String> grokPatternBank,
                                 MatcherWatchdog watchdog) {
        this.terms = override.getTerms() == null ?
            new ArrayList<>(definitions.size()) :
            Collections.singletonList(override.getTerms());
        this.maxLengths = new long[definitions.size()];
        this.categoryIds = new long[definitions.size()];
        this.compiledPattern = override.getRegex() == null ?
            new Pattern[definitions.size()] :
            new Pattern[] { Pattern.compile(override.getRegex()) };
        this.name = override.getName();
        this.grok = override.getGrokPattern() == null ?
            new Grok[definitions.size()] :
            new Grok[] { new Grok(grokPatternBank, override.getGrokPattern(), watchdog) };
        for (int i = 0; i < definitions.size(); i++) {
            CategoryDefinition definition = definitions.get(i);
            maxLengths[i] = definition.getMaxMatchingLength();
            categoryIds[i] = definition.getCategoryId();
            if (override.getRegex() == null) {
                compiledPattern[i] = Pattern.compile(definition.getRegex());
            }
            if (override.getTerms() == null) {
                terms.add(new HashSet<>(Arrays.asList(definition.getTerms().split(" "))));
            }
            if (override.getGrokPattern() == null) {
                grok[i] = definition.getGrokPattern() == null ?
                    null :
                    new Grok(grokPatternBank, definitions.get(i).getGrokPattern(), watchdog);
            }
        }
    }

    public String getName() {
        return name;
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE + SHALLOW_SIZE_PATTERN + SHALLOW_SIZE_GROK;
        size += RamUsageEstimator.sizeOf(maxLengths);
        size += RamUsageEstimator.sizeOf(categoryIds);
        size += RamUsageEstimator.sizeOf(name);
        size += RamUsageEstimator.sizeOfCollection(terms);
        return size;
    }

    int matchesCategory(Set<String> analyzedTokens, String originalText) {
        for (int i = 0; i < this.categoryIds.length; i++) {
            if (originalText.length() <= maxLengths[i] &&
                analyzedTokens.containsAll(getTerms(i)) &&
                getPattern(i).matcher(originalText).find()) {
                return i;
            }
        }
        return NOT_MATCHED;
    }

    long maxLength() {
        long max = 0;
        for(long maxLength : maxLengths) {
            max = Math.max(max, maxLength);
        }
        return max;
    }

    int maxTokensLength() {
        int length = 0;
        for (Set<String> term : terms) {
           length = Math.max(term.size(), length);
        }
        return length;
    }

    // If the override contains a pattern, we will only use that one
    // otherwise we will choose the appropriate pattern that is contained in the pattern array
    private Pattern getPattern(int i) {
        if (compiledPattern.length == 1) {
            return compiledPattern[0];
        }
        assert i < compiledPattern.length && i >= 0;
        return compiledPattern[i];
    }

    private Set<String> getTerms(int i) {
        if (terms.size() == 1) {
            return terms.get(0);
        }
        assert i < terms.size() && i >= 0;
        return terms.get(i);
    }

    private Grok getGrok(int i) {
        if (grok.length == 1) {
            return grok[0];
        }
        assert i < grok.length && i >= 0;
        return grok[i];
    }

    private Map<String, Object> grokIt(int matchCategoryIndex, String text) {
        Grok specificGrok = getGrok(matchCategoryIndex);
        if (specificGrok != null) {
            try {
                Map<String, Object> grokedText = grok[0].captures(text);
                logger.trace("[{}] groked with results [{}]", name, grokedText);
                return grokedText;
            } catch (RuntimeException ex) {
                logger.warn(() -> new ParameterizedMessage("[{}] exception attempting to grok text", name), ex);
                return null;
            }
        }
        return null;
    }

    CategorizeTextAction.Response createResponse(int matchedCategoryIndex, boolean includeGrok, String text) {
        assert matchedCategoryIndex < categoryIds.length && matchedCategoryIndex >= 0;
        Map<String, Object> grokedData = null;
        if (includeGrok) {
            grokedData = grokIt(matchedCategoryIndex, text);
        }
        return new CategorizeTextAction.Response(name,
            categoryIds[matchedCategoryIndex],
            getTerms(matchedCategoryIndex).toArray(new String[0]),
            grokedData);
    }

}
