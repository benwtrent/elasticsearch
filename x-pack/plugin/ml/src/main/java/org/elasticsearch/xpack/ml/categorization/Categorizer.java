/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.categorization;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.xpack.core.ml.action.CategorizeTextAction;
import org.elasticsearch.xpack.core.ml.categorization.CategorizationOverride;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Categorizer implements Accountable, Closeable {

    private static final Logger logger = LogManager.getLogger(Categorizer.class);
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Categorizer.class);

    private final List<SearchableCategoryDefinition> searchableCategoryDefinitions;
    private final CategorizationAnalyzer analyzer;

    public Categorizer(List<CategoryDefinition> definitions,
                       List<CategorizationOverride> overrides,
                       CategorizationAnalyzer analyzer,
                       Map<String, String> customGrokPatterns,
                       MatcherWatchdog watchdog) {
        Map<String, String> grokPatternBank = new HashMap<>(Grok.getBuiltinPatterns());
        if(customGrokPatterns != null) {
            grokPatternBank.putAll(customGrokPatterns);
        }
        if (overrides == null || overrides.isEmpty()) {
            this.searchableCategoryDefinitions = definitions.stream()
                .map(d -> new SearchableCategoryDefinition(d, grokPatternBank, watchdog))
                .collect(Collectors.toList());
        } else {
            Map<Long, CategoryDefinition> definitionsById = definitions.stream()
                .collect(Collectors.toMap(CategoryDefinition::getCategoryId, Function.identity()));
            this.searchableCategoryDefinitions = new ArrayList<>();
            for (CategorizationOverride override : overrides) {
                List<CategoryDefinition> referencedDefinitions = override.getCategoryIds()
                    .stream()
                    .map(definitionsById::remove)
                    .collect(Collectors.toList());
                this.searchableCategoryDefinitions.add(
                    new SearchableCategoryDefinition(referencedDefinitions, override, grokPatternBank, watchdog));
            }
            definitionsById.values()
                .forEach(definition -> this.searchableCategoryDefinitions.add(
                    new SearchableCategoryDefinition(definition, grokPatternBank, watchdog)));
        }
        // Sort by shortest to longest as we verify a match by text.length() < maxLength
        // Then if the lengths are the same, sort by the one having the most terms, as this will most likely be
        // the more "specific" route.
        this.searchableCategoryDefinitions.sort(
            Comparator.comparing(SearchableCategoryDefinition::maxLength)
                .thenComparing((l,r) -> Integer.compare(r.maxTokensLength(), l.maxTokensLength())));
        this.analyzer = analyzer;
    }

    public CategorizeTextAction.Response getCategory(String text, boolean includeGrok) {
        logger.trace("categorizing text [{}]", text);
        Set<String> tokens = new HashSet<>(analyzer.tokenizeField("categorization_analyzer", text));
        logger.trace("tokenized into {}", tokens);
        for (SearchableCategoryDefinition searchableCategoryDefinition : searchableCategoryDefinitions) {
            int specificCategoryMatched = searchableCategoryDefinition.matchesCategory(tokens, text);
            if (specificCategoryMatched != SearchableCategoryDefinition.NOT_MATCHED) {
                logger.trace("[{}] found category", searchableCategoryDefinition.getName());
                return searchableCategoryDefinition.createResponse(specificCategoryMatched, includeGrok, text);
            }
        }
        logger.trace("no category found for text [{}]", text);
        return new CategorizeTextAction.Response("__unknown__", -1, new String[0], null);
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.sizeOfCollection(searchableCategoryDefinitions);
        return size;
    }

    @Override
    public void close() {
        if (analyzer != null) {
            analyzer.close();
        }
    }

}
