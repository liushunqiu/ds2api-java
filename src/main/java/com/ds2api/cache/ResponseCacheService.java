package com.ds2api.cache;

import com.ds2api.adapter.OpenAiResponsesAdapter;
import com.ds2api.model.InternalStreamEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;

/**
 * Caches completed Responses API response payloads for subsequent retrieval via GET.
 * Uses Spring Cache abstraction backed by Caffeine (TTL: 10 minutes).
 */
@Service
@RequiredArgsConstructor
public class ResponseCacheService {

    private static final String CACHE_NAME = "responses";

    private final CacheManager cacheManager;
    private final OpenAiResponsesAdapter responsesAdapter;

    /**
     * Cache a completed response as a JSON node.
     */
    public void cache(String responseId, List<InternalStreamEvent> events) {
        JsonNode cached = responsesAdapter.buildCachedResponse(responseId, events);
        Cache cache = cacheManager.getCache(CACHE_NAME);
        if (cache != null) {
            cache.put(responseId, cached);
        }
    }

    /**
     * Retrieve a cached response by ID.
     */
    public Mono<JsonNode> get(String responseId) {
        Cache cache = cacheManager.getCache(CACHE_NAME);
        return Mono.justOrEmpty(
            Optional.ofNullable(cache)
                .map(c -> c.get(responseId, JsonNode.class))
        );
    }
}
