/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.categorization;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.categorization.CategorizationConfig;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.categorization.persistence.CategorizationProvider;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

import static org.elasticsearch.xpack.ml.MachineLearning.UTILITY_THREAD_POOL_NAME;

public class CategorizerLoadingService {

    // TODO: Add grok watchdog settings
    private static final Logger logger = LogManager.getLogger(CategorizerLoadingService.class);

    private final Cache<String, Categorizer> categorizerCache;
    private final CategorizationProvider categorizationProvider;
    private final Map<String, Queue<ActionListener<Categorizer>>> loadingListeners = new HashMap<>();
    private final MatcherWatchdog matcherWatchdog;

    public CategorizerLoadingService(CategorizationProvider categorizationProvider, ThreadPool threadPool) {
        this(categorizationProvider,
            threadPool::relativeTimeInMillis,
            (delay, command) -> threadPool.schedule(command, TimeValue.timeValueMillis(delay), UTILITY_THREAD_POOL_NAME));
    }

    CategorizerLoadingService(CategorizationProvider categorizationProvider,
                              LongSupplier relativeTimeSupplier,
                              BiConsumer<Long, Runnable> scheduler) {
        this.categorizationProvider = categorizationProvider;
        this.categorizerCache = CacheBuilder.<String, Categorizer>builder()
            .setMaximumWeight(new ByteSizeValue(1, ByteSizeUnit.GB).getBytes())
            .weigher((id, categorizer) -> categorizer.ramBytesUsed())
            .removalListener(this::cacheEvictionListener)
            .setExpireAfterAccess(TimeValue.timeValueMinutes(5))
            .build();
        this.matcherWatchdog = MatcherWatchdog.newInstance(1000, 1000, relativeTimeSupplier, scheduler);
    }

    private void cacheEvictionListener(RemovalNotification<String, Categorizer> notification) {
        // Close the internal analyzer
        logger.info("[{}] evicted from cache for reason [{}]", notification.getKey(), notification.getRemovalReason());
        notification.getValue().close();
    }

    public void getCategorizer(String categorizationConfigId,
                               AnalysisRegistry analysisRegistry,
                               ActionListener<Categorizer> modelActionListener) {
        Categorizer cachedModel = categorizerCache.get(categorizationConfigId);
        if (cachedModel != null) {
            logger.trace("[{}] loaded from cache", categorizationConfigId);
            modelActionListener.onResponse(cachedModel);
            return;
        }
        if (loadModelIfNecessary(categorizationConfigId, analysisRegistry, modelActionListener) == false) {
            // If we the model is not loaded and we did not kick off a new loading attempt, this means that we may be getting called
            // by a simulated pipeline
            logger.trace("[{}] not actively loading, eager loading without cache", categorizationConfigId);
            categorizationProvider.getSingleCategorizationConfig(categorizationConfigId, ActionListener.wrap(
                config -> modelActionListener.onResponse(buildCategorizer(config, analysisRegistry)),
                modelActionListener::onFailure
            ));
        } else {
            logger.trace("[{}] is loading or loaded, added new listener to queue", categorizationConfigId);
        }
    }

    private Categorizer buildCategorizer(CategorizationConfig config, AnalysisRegistry analysisRegistry) {
        try {
            Categorizer categorizer = new Categorizer(config.getCategories(),
                config.getOverrides(),
                createCategorizationAnalyzer(config, analysisRegistry),
                config.getCustomGrokPatterns(),
                matcherWatchdog);
            logger.trace("[{}] loaded bytes [{},{}]", config.getCategorizationConfigId(), categorizer.ramBytesUsed(), new ByteSizeValue(categorizer.ramBytesUsed()));
            return categorizer;
        } catch (IOException ex) {
            throw ExceptionsHelper.serverError("[{}] unexpected failure creating analyzer", ex, config.getCategorizationConfigId());
        }
    }

    /**
     * Returns true if the model is loaded and the listener has been given the cached model
     * Returns true if the model is CURRENTLY being loaded and the listener was added to be notified when it is loaded
     * Returns false if the model is not loaded or actively being loaded
     */
    private boolean loadModelIfNecessary(String categorizationConfigId, AnalysisRegistry analysisRegistry, ActionListener<Categorizer> modelActionListener) {
        synchronized (loadingListeners) {
            Categorizer cachedModel = categorizerCache.get(categorizationConfigId);
            if (cachedModel != null) {
                modelActionListener.onResponse(cachedModel);
                return true;
            }

            // If the loaded model is referenced there but is not present,
            // that means the previous load attempt failed or the model has been evicted
            // Attempt to load and cache the model if necessary
            if (loadingListeners.computeIfPresent(
                categorizationConfigId,
                (storedModelKey, listenerQueue) -> addFluently(listenerQueue, modelActionListener)) == null) {
                logger.trace("[{}] attempting to load and cache", categorizationConfigId);
                loadingListeners.put(categorizationConfigId, addFluently(new ArrayDeque<>(), modelActionListener));
                loadModel(categorizationConfigId, analysisRegistry);
            }
            return true;
        } // synchronized (loadingListeners)
    }

    private void loadModel(String categorizationConfigId, AnalysisRegistry analysisRegistry) {
        categorizationProvider.getSingleCategorizationConfig(categorizationConfigId, ActionListener.wrap(
            categorizationConfig -> {
                logger.debug("[{}] successfully loaded model", categorizationConfigId);
                handleLoadSuccess(categorizationConfigId, analysisRegistry, categorizationConfig);
            },
            failure -> {
                logger.warn(new ParameterizedMessage("[{}] failed to load model", categorizationConfigId), failure);
                handleLoadFailure(categorizationConfigId, failure);
            }
        ));
    }

    private void handleLoadSuccess(String modelId, AnalysisRegistry analysisRegistry, CategorizationConfig trainedModelConfig) {
        Queue<ActionListener<Categorizer>> listeners;
        Categorizer loadedModel = buildCategorizer(trainedModelConfig, analysisRegistry);
        synchronized (loadingListeners) {
            listeners = loadingListeners.remove(modelId);
            // If there is no loadingListener that means the loading was canceled and the listener was already notified as such
            // Consequently, we should not store the retrieved model
            if (listeners == null) {
                return;
            }
            categorizerCache.put(modelId, loadedModel);
        } // synchronized (loadingListeners)
        for (ActionListener<Categorizer> listener = listeners.poll(); listener != null; listener = listeners.poll()) {
            listener.onResponse(loadedModel);
        }
    }

    private void handleLoadFailure(String modelId, Exception failure) {
        Queue<ActionListener<Categorizer>> listeners;
        synchronized (loadingListeners) {
            listeners = loadingListeners.remove(modelId);
            if (listeners == null) {
                return;
            }
        } // synchronized (loadingListeners)
        // If we failed to load and there were listeners present, that means that this model is referenced by a processor
        // Alert the listeners to the failure
        for (ActionListener<Categorizer> listener = listeners.poll(); listener != null; listener = listeners.poll()) {
            listener.onFailure(failure);
        }
    }

    private static CategorizationAnalyzer createCategorizationAnalyzer(CategorizationConfig config,
                                                                       AnalysisRegistry analysisRegistry) throws IOException {
        CategorizationAnalyzerConfig categorizationAnalyzerConfig = config.getCategorizationAnalyzerConfig();
        if (categorizationAnalyzerConfig == null) {
            categorizationAnalyzerConfig =
                CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(config.getCategorizationFilters());
        }
        return new CategorizationAnalyzer(analysisRegistry, categorizationAnalyzerConfig);
    }

    private static <T> Queue<T> addFluently(Queue<T> queue, T object) {
        queue.add(object);
        return queue;
    }
}
