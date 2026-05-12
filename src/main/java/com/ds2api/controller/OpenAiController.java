package com.ds2api.controller;

import com.ds2api.adapter.OpenAiChatAdapter;
import com.ds2api.adapter.OpenAiResponsesAdapter;
import com.ds2api.auth.ApiAuthFilter;
import com.ds2api.auth.AuthInfo;
import com.ds2api.cache.ResponseCacheService;
import com.ds2api.config.ModelAliasService;
import com.ds2api.model.InternalRequest;
import com.ds2api.model.InternalStreamEvent;
import com.ds2api.model.ModelMeta;
import com.ds2api.registry.ModelRegistryService;
import com.ds2api.runtime.ChatRuntimeService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

/**
 * OpenAI-compatible API controller.
 * Routes:
 *   GET  /v1/models             - Model list (DeepSeek native IDs only)
 *   GET  /v1/models/{id}        - Single model lookup
 *   POST /v1/chat/completions   - Chat streaming
 *   POST /v1/responses          - Responses API (streaming)
 *   GET  /v1/responses/{id}     - Responses retrieval (cached)
 */
@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
public class OpenAiController {

    private static final Logger log = LoggerFactory.getLogger(OpenAiController.class);

    private final ObjectMapper mapper;
    private final ChatRuntimeService runtime;
    private final OpenAiChatAdapter chatAdapter;
    private final OpenAiResponsesAdapter responsesAdapter;
    private final ResponseCacheService responseCache;
    private final ModelAliasService modelAlias;
    private final ModelRegistryService modelRegistry;

    /**
     * GET /v1/models
     * Returns the 10 DeepSeek v4 native model IDs (no aliases).
     */
    @GetMapping("/models")
    public Mono<ObjectNode> listModels() {
        ObjectNode response = mapper.createObjectNode();
        response.put("object", "list");
        ArrayNode data = response.putArray("data");
        for (ModelMeta m : modelRegistry.getAll()) {
            ObjectNode node = data.addObject();
            node.put("id", m.id());
            node.put("object", "model");
            node.put("created", m.created());
            node.put("owned_by", "deepseek");
        }
        return Mono.just(response);
    }

    /**
     * GET /v1/models/{id}
     * Look up a single model by its native DeepSeek ID.
     */
    @GetMapping("/models/{id}")
    public Mono<ResponseEntity<ObjectNode>> getModel(@PathVariable String id) {
        return Mono.justOrEmpty(modelRegistry.getById(id))
            .map(m -> {
                ObjectNode node = mapper.createObjectNode();
                node.put("id", m.id());
                node.put("object", "model");
                node.put("created", m.created());
                node.put("owned_by", "deepseek");
                return ResponseEntity.ok(node);
            })
            .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /**
     * POST /v1/chat/completions
     * OpenAI Chat Completions endpoint with SSE streaming support.
     */
    @PostMapping("/chat/completions")
    public Flux<ServerSentEvent<String>> chatCompletions(
            @RequestBody JsonNode body, ServerWebExchange exchange) {
        String clientSessionId = exchange.getRequest().getHeaders().getFirst("Session_id");
        if (clientSessionId == null || clientSessionId.isBlank()) {
            clientSessionId = exchange.getRequest().getHeaders().getFirst("X-Claude-Code-Session-Id");
        }
        log.debug("Session_id from header: '{}'", clientSessionId);
        final String sessionIdFromHeader = clientSessionId;

        return chatAdapter.normalizeRequest(body)
            .map(req -> {
                if (sessionIdFromHeader != null && !sessionIdFromHeader.isBlank()) {
                    return req.withConversationId(sessionIdFromHeader);
                }
                return req;
            })
            .flatMapMany(req -> {
                String reqId = "chat_" + UUID.randomUUID().toString().substring(0, 8);
                String resolvedModel = modelAlias.resolveModel(req.model())
                        .orElse(req.model());
                InternalRequest resolved = req.withModel(resolvedModel);

                AuthInfo authInfo = ApiAuthFilter.getAuthInfo(exchange);

                log.debug("Chat request [{}] model={}->{} stream={} mode={}",
                        reqId, req.model(), resolvedModel, resolved.stream(),
                        authInfo != null ? authInfo.mode() : "none");

                Flux<InternalStreamEvent> events = runtime.execute(resolved, authInfo);
                return chatAdapter.toSse(events, resolved, reqId, resolved.stream());
            })
            .doOnError(e -> log.error("Chat completions error", e));
    }

    /**
     * POST /v1/responses
     * OpenAI Responses API - streaming with caching for later retrieval.
     */
    @PostMapping("/responses")
    public Flux<ServerSentEvent<String>> createResponse(
            @RequestBody JsonNode body, ServerWebExchange exchange) {
        String respId = "resp_" + UUID.randomUUID().toString().replace("-", "");
        log.debug("Response create [{}]", respId);

        String clientSessionId = exchange.getRequest().getHeaders().getFirst("Session_id");
        if (clientSessionId == null || clientSessionId.isBlank()) {
            clientSessionId = exchange.getRequest().getHeaders().getFirst("X-Claude-Code-Session-Id");
        }
        log.debug("Session_id from header for responses: '{}'", clientSessionId);
        final String sessionIdFromHeader = clientSessionId;

        return responsesAdapter.normalizeRequest(body)
            .map(req -> {
                if (sessionIdFromHeader != null && !sessionIdFromHeader.isBlank()) {
                    return req.withConversationId(sessionIdFromHeader);
                }
                return req;
            })
            .flatMapMany(req -> {
                String resolvedModel = modelAlias.resolveModel(req.model())
                        .orElse(req.model());
                InternalRequest resolved = req.withModel(resolvedModel);

                List<InternalStreamEvent> buffer = Collections.synchronizedList(new ArrayList<>());

                AuthInfo authInfo = ApiAuthFilter.getAuthInfo(exchange);

                Flux<InternalStreamEvent> stream = runtime.execute(resolved, authInfo)
                    .doOnNext(buffer::add)
                    .doOnComplete(() -> {
                        log.debug("Response [{}] completed, caching {} events", respId, buffer.size());
                        responseCache.cache(respId, buffer);
                    })
                    .doOnError(e -> log.error("Response [{}] stream error", respId, e));

                return responsesAdapter.toSse(stream, resolved, respId, true);
            });
    }

    /**
     * GET /v1/responses/{id}
     * Retrieves a previously cached response.
     */
    @GetMapping("/responses/{id}")
    public Mono<ResponseEntity<JsonNode>> getResponse(@PathVariable String id) {
        log.debug("Response retrieval request for [{}]", id);
        return responseCache.get(id)
            .map(ResponseEntity::ok)
            .defaultIfEmpty(ResponseEntity.status(404).build());
    }
}
