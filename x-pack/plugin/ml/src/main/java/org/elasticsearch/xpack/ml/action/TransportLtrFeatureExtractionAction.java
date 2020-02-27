/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.LtrFeatureExtractionAction;
import org.elasticsearch.xpack.core.ml.ltr.RankEvalSpec;
import org.elasticsearch.xpack.core.ml.ltr.RatedHit;
import org.elasticsearch.xpack.core.ml.ltr.RatedDocument;
import org.elasticsearch.xpack.core.ml.ltr.RatedRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentHelper.createParser;

/**
 * copied from the rank-eval module
 */
public class TransportLtrFeatureExtractionAction
    extends HandledTransportAction<LtrFeatureExtractionAction.Request, LtrFeatureExtractionAction.Response> {
    private final Client client;
    private static final Logger logger = LogManager.getLogger(TransportLtrFeatureExtractionAction.class);
    private final ScriptService scriptService;
    private final NamedXContentRegistry namedXContentRegistry;

    @Inject
    public TransportLtrFeatureExtractionAction(ActionFilters actionFilters, Client client, TransportService transportService,
                                               ScriptService scriptService, NamedXContentRegistry namedXContentRegistry) {
        super(LtrFeatureExtractionAction.NAME, transportService, actionFilters, LtrFeatureExtractionAction.Request::new);
        this.scriptService = scriptService;
        this.namedXContentRegistry = namedXContentRegistry;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, LtrFeatureExtractionAction.Request request, ActionListener<LtrFeatureExtractionAction.Response> listener) {
        RankEvalSpec evaluationSpecification = request.getRankEvalSpec();

        List<RatedRequest> ratedRequests = evaluationSpecification.getRatedRequests();
        Map<String, ElasticsearchException> errors = new ConcurrentHashMap<>(ratedRequests.size());
        List<RatedRequest> ratedRequestsInSearch = new ArrayList<>();
        Map<String, TemplateScript.Factory> scriptsWithoutParams = new HashMap<>();
        for (Map.Entry<String, Script> entry : evaluationSpecification.getTemplates().entrySet()) {
            scriptsWithoutParams.put(entry.getKey(), scriptService.compile(entry.getValue(), TemplateScript.CONTEXT));
        }
        MultiSearchRequest initialMultiSearch = new MultiSearchRequest();
        initialMultiSearch.maxConcurrentSearchRequests(evaluationSpecification.getMaxConcurrentSearches());
        for (RatedRequest ratedRequest : ratedRequests) {
            SearchSourceBuilder evaluationRequest = ratedRequest.getEvaluationRequest();
            if (evaluationRequest == null) {
                Map<String, Object> params = ratedRequest.getParams();
                String templateId = ratedRequest.getTemplateId();
                TemplateScript.Factory templateScript = scriptsWithoutParams.get(templateId);
                String resolvedRequest = templateScript.newInstance(params).execute();
                try (XContentParser subParser = createParser(namedXContentRegistry,
                    LoggingDeprecationHandler.INSTANCE, new BytesArray(resolvedRequest), XContentType.JSON)) {
                    evaluationRequest = SearchSourceBuilder.fromXContent(subParser, false);
                } catch (IOException e) {
                    listener.onFailure(new ElasticsearchException("Failed parsing query for request [" + ratedRequest.getId() + "]", e));
                    return;
                }
            }

            evaluationRequest.size(request.getNumDocs());
            ratedRequestsInSearch.add(ratedRequest);
            List<String> summaryFields = ratedRequest.getSummaryFields();
            if (summaryFields.isEmpty()) {
                evaluationRequest.fetchSource(false);
            } else {
                evaluationRequest.fetchSource(summaryFields.toArray(new String[0]), new String[0]);
            }
            SearchRequest searchRequest = new SearchRequest(request.indices(), evaluationRequest);
            searchRequest.indicesOptions(request.indicesOptions());
            searchRequest.searchType(request.searchType());
            initialMultiSearch.add(searchRequest);
        }
        if (initialMultiSearch.requests().isEmpty()) {
            listener.onResponse(new LtrFeatureExtractionAction.Response(errors));
            logger.info("no search took place for request");
            return;
        }

        ActionListener<MultiSearchResponse> handleFirstMSearchResponse = ActionListener.wrap(
            multiSearchResponse -> {
                int responsePosition = 0;
                BulkRequest bulkRequest = new BulkRequest();
                Map<String, CategorizedHits> specificationAndHits = new HashMap<>();
                List<RatedRequest> unmetSpecifications = new ArrayList<>();
                MultiSearchRequest mSearchForUnmatched = new MultiSearchRequest();
                mSearchForUnmatched.maxConcurrentSearchRequests(request.getRankEvalSpec().getMaxConcurrentSearches());
                for (Item response : multiSearchResponse.getResponses()) {
                    RatedRequest specification = ratedRequestsInSearch.get(responsePosition++);
                    List<RatedDocument> docs = specification.getRatedDocs();
                    if (response.isFailure()) {
                        errors.put(specification.getId(), new ElasticsearchException(response.getFailure()));
                        continue;
                    }
                    SearchHit[] hits = response.getResponse().getHits().getHits();
                    CategorizedHits categorizedHits = joinHitsWithRatings(hits, docs);
                    if (categorizedHits.ratingsWithoutHits.isEmpty() == false) {
                        mSearchForUnmatched.add(buildSearchRequestFromUnmetRatings(request,
                            specification,
                            categorizedHits.ratingsWithoutHits));
                        unmetSpecifications.add(specification);
                        specificationAndHits.put(specification.getId(), categorizedHits);
                        continue;
                    }
                    // We don't have to worry about unmet ranked docs for this rating request
                    // Add it to our bulk indexing
                    EvalDoc evalDoc = new EvalDoc(specification.getId(), categorizedHits);
                    addEvalDocToBulkRequest(evalDoc, bulkRequest, request.getDestinationIndex(), errors);
                }
                assert unmetSpecifications.size() == mSearchForUnmatched.requests().size();
                if (bulkRequest.requests().isEmpty()) {
                    logger.info("bulk requests are empty. errors [" + errors + "]");
                    if (unmetSpecifications.isEmpty()) {
                        logger.info("no unmatched specifications");
                        listener.onResponse(new LtrFeatureExtractionAction.Response(errors));
                        return;
                    }
                    handleUnmetSpecifications(mSearchForUnmatched,
                        unmetSpecifications,
                        specificationAndHits,
                        request.getDestinationIndex(),
                        errors,
                        listener);
                } else {
                    client.bulk(bulkRequest, ActionListener.wrap(
                        bulkResponse -> {
                            if (bulkResponse.hasFailures()) {
                                errors.put("initial_bulk_indexing_failures", new ElasticsearchException(bulkResponse.buildFailureMessage()));
                            }
                            if (unmetSpecifications.isEmpty()) {
                                listener.onResponse(new LtrFeatureExtractionAction.Response(errors));
                            } else {
                                handleUnmetSpecifications(mSearchForUnmatched,
                                    unmetSpecifications,
                                    specificationAndHits,
                                    request.getDestinationIndex(),
                                    errors,
                                    listener);
                            }
                        },
                        listener::onFailure
                    ));
                }
            },
            listener::onFailure
        );
        assert ratedRequestsInSearch.size() == initialMultiSearch.requests().size();
        client.multiSearch(initialMultiSearch, handleFirstMSearchResponse);
    }

    void addEvalDocToBulkRequest(EvalDoc evalDoc, BulkRequest bulkRequest, String dest, Map<String, ElasticsearchException> errors) {
        try {
            BytesReference reference = XContentHelper.toXContent(evalDoc, XContentType.JSON, false);
            bulkRequest.add(client.prepareIndex(dest)
                .setId(evalDoc.specificationId)
                .setSource(reference, XContentType.JSON)
                .request());
        } catch (IOException ex) {
            errors.put(evalDoc.specificationId, new ElasticsearchException(ex));
        }
    }

    void handleUnmetSpecifications(MultiSearchRequest msearchRequest,
                                   List<RatedRequest> unmetSpecifications,
                                   Map<String, CategorizedHits> specificationAndHits,
                                   String destination,
                                   Map<String, ElasticsearchException> errors,
                                   ActionListener<LtrFeatureExtractionAction.Response> listener) {
        client.multiSearch(msearchRequest, ActionListener.wrap(
            multiSearchResponse -> {
                BulkRequest bulkRequest = new BulkRequest();
                int currResponse = 0;
                for (Item response : multiSearchResponse.getResponses()) {
                    RatedRequest specification = unmetSpecifications.get(currResponse++);
                    if (response.isFailure()) {
                        errors.put(specification.getId(), new ElasticsearchException(response.getFailure()));
                        continue;
                    }
                    List<RatedDocument> docs = specification.getRatedDocs();
                    SearchHit[] hits = response.getResponse().getHits().getHits();
                    CategorizedHits categorizedHits = joinHitsWithRatings(hits, docs);
                    CategorizedHits originalHits = specificationAndHits.get(specification.getId());

                    assert originalHits != null;

                    // We don't have to worry about unmet ranked docs for this rating request
                    // Add it to our bulk indexing
                    EvalDoc evalDoc = new EvalDoc(specification.getId(),
                        originalHits.ratedHits,
                        originalHits.unrankedHits,
                        categorizedHits.ratedHits);
                    addEvalDocToBulkRequest(evalDoc, bulkRequest, destination, errors);
                }
                if (bulkRequest.requests().isEmpty()) {
                    logger.info("bulk request for unmet specifications is empty. Errors [" + errors + "]");
                    listener.onResponse(new LtrFeatureExtractionAction.Response(errors));
                    return;
                }
                client.bulk(bulkRequest, ActionListener.wrap(
                    bulkResponse -> {
                        if (bulkResponse.hasFailures()) {
                            errors.put("bulk_indexing_failures", new ElasticsearchException(bulkResponse.buildFailureMessage()));
                        }
                        listener.onResponse(new LtrFeatureExtractionAction.Response(errors));
                    },
                    listener::onFailure
                ));
            },
            listener::onFailure
        ));
    }

    /**
     * Joins hits with rated documents using the joint _index/_id document key.
     */
    static CategorizedHits joinHitsWithRatings(SearchHit[] hits, List<RatedDocument> ratedDocs) {
        Map<RatedDocument.DocumentKey, RatedDocument> ratedDocumentMap = ratedDocs.stream()
            .collect(Collectors.toMap(RatedDocument::getKey, item -> item));
        List<RatedHit> ratedSearchHits = new ArrayList<>(hits.length);
        List<RatedHit> hitsWithoutRanking = new ArrayList<>();
        Set<RatedDocument> ratingsWithHits = new HashSet<>();
        for (SearchHit hit : hits) {
            RatedDocument.DocumentKey key = new RatedDocument.DocumentKey(hit.getIndex(), hit.getId());
            RatedDocument ratedDoc = ratedDocumentMap.get(key);
            if (ratedDoc != null) {
                ratedSearchHits.add(new RatedHit(hit, ratedDoc.getRating()));
                ratingsWithHits.add(ratedDoc);
            } else {
                hitsWithoutRanking.add(new RatedHit(hit, null));
            }
        }
        CategorizedHits categorizedHits = new CategorizedHits();
        categorizedHits.ratedHits = ratedSearchHits;
        List<RatedDocument> ratingsWithoutHits = ratedDocs.stream()
            .filter(rd -> ratingsWithHits.contains(rd) == false)
            .collect(Collectors.toList());
        categorizedHits.ratingsWithoutHits = ratingsWithoutHits;
        categorizedHits.unrankedHits = hitsWithoutRanking;
        return categorizedHits;
    }

    static SearchRequest buildSearchRequestFromUnmetRatings(LtrFeatureExtractionAction.Request request,
                                                            RatedRequest ratedRequest,
                                                            List<RatedDocument> ratedDocuments) {
        //TODO what about doc ids that match two different docs in different indices?
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .size(ratedDocuments.size())
            .query(QueryBuilders.boolQuery()
                .filter(QueryBuilders.idsQuery()
                .addIds(ratedDocuments.stream().map(RatedDocument::getDocID).toArray(String[]::new))));
        List<String> summaryFields = ratedRequest.getSummaryFields();
        if (summaryFields.isEmpty()) {
            searchSourceBuilder.fetchSource(false);
        } else {
            searchSourceBuilder.fetchSource(summaryFields.toArray(new String[0]), new String[0]);
        }
        SearchRequest searchRequest = new SearchRequest(request.indices(), searchSourceBuilder);
        searchRequest.indicesOptions(request.indicesOptions());
        return searchRequest;
    }

    private static class CategorizedHits {
        List<RatedHit> ratedHits;
        List<RatedDocument> ratingsWithoutHits;
        List<RatedHit> unrankedHits;
    }

    private static class EvalDoc implements ToXContentObject {

        private final String specificationId;
        private final List<RatedHit> ratedHits;
        private final List<RatedHit> unrankedHits;
        private final List<RatedHit> rankedWithoutHit;

        public EvalDoc(String specificationId, CategorizedHits categorizedHits) {
            this(specificationId, categorizedHits.ratedHits, categorizedHits.unrankedHits, Collections.emptyList());
        }
        public EvalDoc(String specificationId,
                       List<RatedHit> ratedHits,
                       List<RatedHit> unrankedHits,
                       List<RatedHit> rankedWithoutHit) {
            this.specificationId = specificationId;
            this.ratedHits = ratedHits;
            this.unrankedHits = unrankedHits;
            this.rankedWithoutHit = rankedWithoutHit;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("specification_id", specificationId);
            if (ratedHits.size() > 0) {
                builder.field("rated_hits", ratedHits);
            }
            if (unrankedHits.size() > 0) {
                builder.field("unrated_hits", unrankedHits);
            }
            if (rankedWithoutHit.size() > 0) {
                builder.field("rated_without_hits", rankedWithoutHit);
            }
            builder.endObject();
            return builder;
        }
    }
}
