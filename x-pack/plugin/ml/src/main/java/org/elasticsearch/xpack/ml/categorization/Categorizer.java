package org.elasticsearch.xpack.ml.categorization;

import org.elasticsearch.grok.Grok;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.xpack.core.ml.action.CategorizeTextAction;
import org.elasticsearch.xpack.core.ml.categorization.CategorizationConfig;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Categorizer {

    private static final CategoryDefinition EMPTY_DEFINITION;
    static {
        EMPTY_DEFINITION = new CategoryDefinition("");
        EMPTY_DEFINITION.setCategoryId(-1);
    }
    private final List<SearchableCategoryDefinition> searchableCategoryDefinitions;
    private final CategorizationAnalyzer analyzer;

    public Categorizer(CategorizationConfig config, CategorizationAnalyzer analyzer) {
        this.searchableCategoryDefinitions = categories.stream().map(SearchableCategoryDefinition::new).collect(Collectors.toList());
        this.analyzer = analyzer;
    }

    public CategorizeTextAction.Response getCategory(String text) {
        Set<String> tokens = new HashSet<>(analyzer.tokenizeField("categorization_analyzer", text));
        for (SearchableCategoryDefinition searchableCategoryDefinition : searchableCategoryDefinitions) {
            if (searchableCategoryDefinition.maxLength >= text.length() && tokens.containsAll(searchableCategoryDefinition.terms)) {
                if (searchableCategoryDefinition.compiledPattern.matcher(text).find()) {
                    return searchableCategoryDefinition.definition;
                }
            }
        }
        return EMPTY_DEFINITION;
    }


    public static class SearchableCategoryDefinition {

        private final Set<String> terms;
        private final long maxLength;
        private final long categoryId;
        private final Pattern compiledPattern;

        public SearchableCategoryDefinition(CategoryDefinition definition) {
            this.terms = new LinkedHashSet<>(Arrays.asList(definition.getTerms().split(" ")));
            this.maxLength = definition.getMaxMatchingLength();
            this.categoryId = definition.getCategoryId();
            this.compiledPattern = Pattern.compile(definition.getRegex());
            this.definition = definition;
        }

    }
}
