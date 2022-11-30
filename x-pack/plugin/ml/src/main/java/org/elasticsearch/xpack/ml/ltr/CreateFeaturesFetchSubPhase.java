/*
 * Copyright [2017] Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.xpack.ml.ltr;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LeafReaderContextExtractor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LeafReaderContextExtractorFactory;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LtrConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateFeaturesFetchSubPhase implements FetchSubPhase {
    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) throws IOException {
        CreateFeaturesSearchExtBuilder ext = (CreateFeaturesSearchExtBuilder) context.getSearchExt(CreateFeaturesSearchExtBuilder.NAME);
        if (ext == null) {
            return null;
        }
        List<PreProcessor> preProcessors = ext.getProcessors();
        InferenceConfig inferenceConfig = ext.getInferenceConfig();
        assert inferenceConfig instanceof LtrConfig;
        LtrConfig ltrConfig = (LtrConfig) inferenceConfig;
        SearchExecutionContext executionContext = context.getSearchExecutionContext();

        int rewriteAttempts = 0;
        List<PreProcessor> rewritten = new ArrayList<>(preProcessors);
        for (; rewriteAttempts < 15; rewriteAttempts++) {
            boolean didRewrite = false;
            rewritten.clear();
            for (PreProcessor preprocessor : preProcessors) {
                PreProcessor toAdd = preprocessor.rewriteWithParams(executionContext, ltrConfig.getParams());
                if (toAdd != preprocessor) {
                    didRewrite = true;
                }
                rewritten.add(toAdd);
            }
            if (didRewrite == false) {
                break;
            }
        }
        List<LeafReaderContextExtractor> extractors = new ArrayList<>();
        for (LeafReaderContextExtractorFactory factory : rewritten.stream()
            .filter(p -> p instanceof LeafReaderContextExtractorFactory)
            .map(p -> (LeafReaderContextExtractorFactory) p)
            .toList()) {
            extractors.add(factory.createLeafReaderExtractor(executionContext));
        }
        return new CreateFeaturesFetchSubPhaseProcessor(extractors);
    }

    static class CreateFeaturesFetchSubPhaseProcessor implements FetchSubPhaseProcessor {
        private LeafReaderContext currentContext;

        List<LeafReaderContextExtractor> extractors;
        final int featureSize;

        CreateFeaturesFetchSubPhaseProcessor(List<LeafReaderContextExtractor> extractors) {
            this.extractors = extractors;
            featureSize = extractors.stream().mapToInt(LeafReaderContextExtractor::expectedFieldOutputSize).sum();
        }

        @Override
        public void setNextReader(LeafReaderContext readerContext) throws IOException {
            currentContext = readerContext;
            for (var extractor : extractors) {
                extractor.resetContext(readerContext);
            }
        }

        @Override
        public void process(HitContext hitContext) throws IOException {
            Map<String, Object> features = Maps.newMapWithExpectedSize(featureSize);
            for (var extractor : extractors) {
                extractor.extract(hitContext.docId());
                extractor.populate(hitContext.docId() + currentContext.docBase, features);
            }
            hitContext.hit().setDocumentField("_ltr_features", new DocumentField("_ltr_features", List.of(features)));
        }

        @Override
        public StoredFieldsSpec storedFieldsSpec() {
            return StoredFieldsSpec.NEEDS_SOURCE;
        }
    }

}
