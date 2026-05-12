package com.ds2api.runtime;

import com.ds2api.admin.dev.PacketCaptureService;
import com.ds2api.auth.AuthInfo;
import com.ds2api.cache.DeepSeekSessionCacheService;
import com.ds2api.client.DeepSeekPowClient;
import com.ds2api.client.DeepSeekSessionClient;
import com.ds2api.compat.HistoryFileSplitter;
import com.ds2api.compat.PromptCompatService;
import com.ds2api.config.ConfigLoaderService;
import com.ds2api.config.Ds2Config;
import com.ds2api.config.ModelAliasService;
import com.ds2api.tool.ToolCallStreamParser;
import com.ds2api.model.InternalRequest;
import com.ds2api.model.InternalStreamEvent;
import com.ds2api.pool.AccountLease;
import com.ds2api.pool.AccountPoolManager;
import com.ds2api.pool.TokenRefreshService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Core streaming pipeline: acquires an account lease from the pool,
 * resolves the token, applies P1 compat layers (thinking injection,
 * tool definition injection, history file splitting), sends requests
 * to upstream DeepSeek, and parses the SSE stream into unified
 * InternalStreamEvent emissions.
 *
 * Token flow:
 *   DIRECT mode  -> use the raw token from ApiAuthFilter, bypass pool
 *   MANAGED mode -> acquire a pool slot, use the account's token,
 *                   on 401 trigger TokenRefreshService and retry once.
 *
 * PoW is handled proactively: DeepSeekPowClient fetches and solves the PoW
 * challenge before sending the completion request (aligned with Go reference).
 *
 * P2 additions:
 *   - Exponential backoff retry on upstream 429/503 (max 3 attempts, 500ms initial)
 *   - Packet capture integration via PacketCaptureService
 *
 * Session continuation:
 *   - Supports DeepSeek native multi-turn via conversation_id
 *   - Caches chat_session_id and response_message_id for continuation
 */
@Service
public class ChatRuntimeService {

    private static final Logger log = LoggerFactory.getLogger(ChatRuntimeService.class);

    /** Track THINK fragment IDs to return as ThinkingDelta instead of TextDelta. */
    private final Set<String> thinkFragmentIds = ConcurrentHashMap.newKeySet();

    private final WebClient deepSeekClient;
    private final ConfigLoaderService configLoader;
    private final ModelAliasService modelAlias;
    private final AccountPoolManager poolManager;
    private final TokenRefreshService tokenRefreshService;
    private final PromptCompatService promptCompat;
    private final HistoryFileSplitter historySplitter;
    private final DeepSeekSessionClient sessionClient;
    private final DeepSeekPowClient powClient;
    private final DeepSeekSessionCacheService sessionCache;
    private final ObjectMapper mapper;
    private final PacketCaptureService captureService;

    public ChatRuntimeService(WebClient deepSeekClient,
                              ConfigLoaderService configLoader,
                              ModelAliasService modelAlias,
                              AccountPoolManager poolManager,
                              TokenRefreshService tokenRefreshService,
                              PromptCompatService promptCompat,
                              HistoryFileSplitter historySplitter,
                              DeepSeekSessionClient sessionClient,
                              DeepSeekPowClient powClient,
                              DeepSeekSessionCacheService sessionCache,
                              ObjectMapper mapper,
                              PacketCaptureService captureService) {
        this.deepSeekClient = deepSeekClient;
        this.configLoader = configLoader;
        this.modelAlias = modelAlias;
        this.poolManager = poolManager;
        this.tokenRefreshService = tokenRefreshService;
        this.promptCompat = promptCompat;
        this.historySplitter = historySplitter;
        this.sessionClient = sessionClient;
        this.powClient = powClient;
        this.sessionCache = sessionCache;
        this.mapper = mapper;
        this.captureService = captureService;
    }

    /**
     * Execute a streaming chat request against the upstream DeepSeek endpoint.
     *
     * @param request  Normalized internal request
     * @param authInfo Authentication metadata from ApiAuthFilter (mode, token, targetAccount)
     */
    public Flux<InternalStreamEvent> execute(InternalRequest request, AuthInfo authInfo) {
        String requestId = "req_" + UUID.randomUUID().toString().substring(0, 8);
        ToolCallStreamParser toolParser = new ToolCallStreamParser();
        Ds2Config config = configLoader.getConfig();

        // Check for session continuation via conversation_id
        String conversationId = request.conversationId();
        DeepSeekSessionCacheService.SessionInfo cachedSession = null;
        boolean isContinuation = false;

        if (conversationId != null && !conversationId.isBlank()) {
            cachedSession = sessionCache.get(conversationId);
            if (cachedSession != null) {
                isContinuation = true;
                log.debug("[{}] Session continuation: conversationId={}, chatSessionId={}, lastResponseMessageId={}",
                        requestId, conversationId, cachedSession.chatSessionId(), cachedSession.lastResponseMessageId());
            }
        }

        final DeepSeekSessionCacheService.SessionInfo sessionToUse = cachedSession;
        final boolean continueSession = isContinuation;

        // DIRECT mode: use raw token, no pool
        if (authInfo != null && authInfo.mode() == AuthInfo.Mode.DIRECT) {
            String directToken = authInfo.effectiveToken();
            log.debug("[{}] DIRECT mode, token={}...", requestId,
                directToken.length() > 8 ? directToken.substring(0, 8) : directToken);

            // P1: apply prompt compat (sync), no history split for DIRECT mode
            InternalRequest compatReq = promptCompat.applyCompat(request);

            // Create real session or use cached one
            Mono<String> sessionMono = continueSession
                ? Mono.just(sessionToUse.chatSessionId())
                : sessionClient.createSession(directToken);

            return sessionMono.flatMapMany(sessionId -> {
                Integer parentMessageId = continueSession ? sessionToUse.lastResponseMessageId() : null;
                ObjectNode payload = buildUpstreamPayload(compatReq, sessionId, parentMessageId, continueSession);

                return callUpstreamDirect(payload, directToken, requestId, toolParser, sessionId, conversationId)
                    .doFinally(signal -> {
                        toolParser.flushAndReset();
                        toolParser.reset();
                        if (!continueSession) {
                            sessionClient.deleteSession(sessionId,
                                config.getAutoDelete().getMode(), directToken).subscribe();
                        }
                    });
            });
        }

        // MANAGED mode: acquire pool slot, resolve token, apply P1 pipeline
        String targetAccount = authInfo != null ? authInfo.targetAccount() : null;
        if (targetAccount != null) {
            log.debug("[{}] MANAGED target={}", requestId, targetAccount);
        }

        return poolManager.acquire(targetAccount)
            .flatMapMany(lease -> poolManager.resolveToken(lease.accountIdentifier())
                .flatMapMany(token -> {
                    if (token == null || token.isBlank()) {
                        return Flux.<InternalStreamEvent>error(
                            new IllegalStateException(
                                "No token for account " + lease.accountIdentifier()));
                    }
                    log.debug("[{}] Acquired slot for {}", requestId, lease.accountIdentifier());

                    // P1 pipeline: Compat (sync) -> Create session -> Split (async) -> upstream call
                    InternalRequest compatReq = promptCompat.applyCompat(request);

                    // Create real session or use cached one
                    Mono<String> sessionMono = continueSession
                        ? Mono.just(sessionToUse.chatSessionId())
                        : sessionClient.createSession(token);

                    return sessionMono.flatMapMany(sessionId -> {
                        Integer parentMessageId = continueSession ? sessionToUse.lastResponseMessageId() : null;

                        return historySplitter.applySplit(compatReq, token)
                            .flatMapMany(splitReq -> {
                                ObjectNode payload = buildUpstreamPayload(splitReq, sessionId, parentMessageId, continueSession);
                                return callUpstreamWithToken(payload, token, lease, requestId, toolParser, sessionId, conversationId)
                                    .doFinally(signal -> {
                                        toolParser.flushAndReset();
                                        toolParser.reset();
                                        if (!continueSession) {
                                            sessionClient.deleteSession(sessionId,
                                                config.getAutoDelete().getMode(), token).subscribe();
                                        }
                                        lease.release();
                                        log.debug("[{}] Released slot for {}", requestId,
                                            lease.accountIdentifier());
                                    });
                            });
                    });
                }));
    }

    /**
     * Call upstream via pool-managed token. On 401, attempt one token refresh + retry.
     */
    private Flux<InternalStreamEvent> callUpstreamWithToken(ObjectNode payload, String token,
                                                             AccountLease lease,
                                                             String requestId,
                                                             ToolCallStreamParser toolParser,
                                                             String sessionId,
                                                             String conversationId) {
        return doUpstreamCall(payload, token, requestId, toolParser, sessionId, conversationId)
            .onErrorResume(e -> {
                if (isAuthError(e)) {
                    log.info("[{}] 401 on {}, attempting token refresh", requestId,
                        lease.accountIdentifier());
                    return tokenRefreshService
                        .refreshIfNeeded(lease.accountIdentifier(), e)
                        .flatMapMany(newToken -> {
                            log.info("[{}] Token refreshed, retrying upstream", requestId);
                            return doUpstreamCall(payload, newToken, requestId, toolParser, sessionId, conversationId);
                        })
                        .onErrorResume(retryErr -> {
                            log.error("[{}] Retry after refresh also failed: {}",
                                requestId, retryErr.getMessage());
                            return Flux.just(new InternalStreamEvent.Error(
                                "Upstream auth error after refresh: "
                                    + retryErr.getMessage(), 401));
                        });
                }
                return Flux.error(e);
            });
    }

    /**
     * Call upstream with a raw DIRECT token (no pool, no refresh).
     */
    private Flux<InternalStreamEvent> callUpstreamDirect(ObjectNode payload, String token,
                                                          String requestId,
                                                          ToolCallStreamParser toolParser,
                                                          String sessionId,
                                                          String conversationId) {
        return doUpstreamCall(payload, token, requestId, toolParser, sessionId, conversationId);
    }

    /**
     * Core upstream HTTP call: POST /api/v0/chat/completion with SSE streaming.
     *
     * P2: exponential backoff retry on 429 (Rate Limit) and 503 (Service Unavailable),
     * with packet capture accumulation after successful completion.
     *
     * Session continuation: extracts response_message_id from ready event and caches it.
     */
    private Flux<InternalStreamEvent> doUpstreamCall(ObjectNode payload, String token,
                                                       String requestId,
                                                       ToolCallStreamParser toolParser,
                                                       String sessionId,
                                                       String conversationId) {
        // Clear THINK fragment tracking for new request
        clearThinkFragmentIds();

        String payloadStr = payload.toString(); // cached for packet capture
        log.info("[{}] Upstream payload: {}", requestId, payloadStr);

        // Proactively get PoW token before sending completion request (aligned with Go reference)
        return powClient.getPowToken(token)
            .doOnNext(powToken -> log.debug("[{}] PoW token acquired", requestId))
            .flatMapMany(powToken -> {
                log.info("[{}] Sending completion: token={}..., powToken={}..., payloadSize={}",
                    requestId,
                    token.length() > 8 ? token.substring(0, 8) : token,
                    powToken.length() > 20 ? powToken.substring(0, 20) : powToken,
                    payloadStr.length());
                return deepSeekClient.post()
                .uri("/api/v0/chat/completion")
                .header("Authorization", "Bearer " + token)
                .header("x-ds-pow-response", powToken)
                .header("Content-Type", "application/json")
                .bodyValue(payload)
                .exchangeToFlux(resp -> {
                    if (resp.statusCode().isError()) {
                        return resp.bodyToMono(String.class)
                            .flatMapMany(body -> {
                                log.error("[{}] Upstream error {}: body={}", requestId, resp.statusCode(), body);
                                return Flux.<InternalStreamEvent>error(
                                    new UpstreamException(resp.statusCode(), body));
                            });
                    }
                    return resp.bodyToFlux(String.class)
                        .doOnNext(raw -> log.debug("[{}] Raw upstream chunk: {}", requestId, raw))
                        .map(this::parseUpstreamChunk)
                        .filter(event -> !(event instanceof InternalStreamEvent.TextDelta td) || !td.chunk().isEmpty())
                        .doOnNext(event -> log.debug("[{}] Parsed event: {}", requestId, event))
                        .concatMap(event -> {
                            if (event instanceof InternalStreamEvent.TextDelta td && !td.chunk().isEmpty()) {
                                List<InternalStreamEvent> parsed = toolParser.processChunk(td.chunk());
                                if (parsed.isEmpty()) {
                                    return Flux.empty();
                                }
                                return Flux.fromIterable(parsed);
                            }
                            if (event instanceof InternalStreamEvent.Finish) {
                                // Flush any remaining buffered text (incomplete tool call) before finishing
                                List<InternalStreamEvent> flushed = toolParser.flushAndReset();
                                List<InternalStreamEvent> result = new ArrayList<>(flushed);
                                result.add(event);
                                return Flux.fromIterable(result);
                            }
                            return Flux.just(event);
                        });
                })
                // P2: exponential backoff retry for 429/503 only
                .retryWhen(Retry.backoff(3, Duration.ofMillis(500))
                    .filter(e -> e instanceof UpstreamException ue &&
                                 (ue.getStatusCode().value() == 429 ||
                                  ue.getStatusCode().value() == 503))
                    .doBeforeRetry(sig -> log.warn("[{}] Retry upstream attempt {}: {}",
                        requestId, sig.totalRetries() + 1, sig.failure().getMessage()))
                    .onRetryExhaustedThrow((spec, sig) -> sig.failure()))
                .doOnCancel(() -> log.info("[{}] Client disconnected, cancelling upstream",
                    requestId))
                .doOnError(e -> log.error("[{}] Upstream stream error: {}", requestId,
                    e.getMessage()))
                .onErrorResume(e -> {
                    if (e instanceof UpstreamException) {
                        return Flux.just(new InternalStreamEvent.Error(
                            e.getMessage(), ((UpstreamException) e).getStatusCode().value()));
                    }
                    return Flux.just(new InternalStreamEvent.Error(
                        "Stream error: " + e.getMessage(), 500));
                });
            })
            .onErrorResume(e -> {
                log.error("[{}] PoW token acquisition failed: {}", requestId, e.getMessage());
                return Flux.just(new InternalStreamEvent.Error(
                    "PoW token acquisition failed: " + e.getMessage(), 500));
            });
    }

    /**
     * Extract response_message_id from the ready event in SSE chunks and cache it.
     * The ready event format: {"request_message_id":1,"response_message_id":2,"model_type":"expert"}
     */
    private void extractAndCacheResponseMessageId(List<String> chunks, String conversationId, String sessionId) {
        if (conversationId == null || conversationId.isBlank()) return;

        for (String chunk : chunks) {
            String trimmed = chunk.trim();
            // Look for ready event data
            if (trimmed.startsWith("data:") && trimmed.contains("response_message_id")) {
                String json = trimmed.substring(5).trim();
                try {
                    JsonNode node = mapper.readTree(json);
                    if (node.has("response_message_id")) {
                        int responseMessageId = node.get("response_message_id").asInt();
                        // Cache or update the session
                        if (sessionCache.contains(conversationId)) {
                            sessionCache.updateResponseMessageId(conversationId, responseMessageId);
                        } else {
                            sessionCache.put(conversationId, sessionId, responseMessageId);
                        }
                        log.debug("[Session] Cached response_message_id={} for conversation={}",
                                responseMessageId, conversationId);
                        return;
                    }
                } catch (Exception e) {
                    log.trace("[Session] Failed to parse ready event: {}", json, e);
                }
            }
        }
    }

    /**
     * Build upstream payload for DeepSeek completion API.
     *
     * @param request         normalized request
     * @param sessionId       DeepSeek chat_session_id
     * @param parentMessageId parent_message_id for continuation (null for new session)
     * @param isContinuation  true if continuing an existing session
     */
    private ObjectNode buildUpstreamPayload(InternalRequest request, String sessionId,
                                             Integer parentMessageId, boolean isContinuation) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("chat_session_id", sessionId);

        // Set parent_message_id: integer for continuation, null for new session
        if (parentMessageId != null) {
            payload.put("parent_message_id", parentMessageId);
        } else {
            payload.putNull("parent_message_id");
        }

        // Set model_type: null for continuation, value for new session
        if (isContinuation) {
            payload.putNull("model_type");
        } else {
            payload.put("model_type", modelAlias.getModelType(request.model()));
        }

        String prompt = buildDeepSeekPrompt(request.messages());
        payload.put("prompt", prompt);
        payload.putArray("ref_file_ids");

        ModelAliasService.ModelConfig mc = modelAlias.getModelConfig(request.model());
        payload.put("thinking_enabled", mc.thinkingEnabled());
        payload.put("search_enabled", mc.searchEnabled());

        // Add pass-through parameters (temperature, top_p, max_tokens, etc.)
        if (request.passThrough() != null) {
            for (var entry : request.passThrough().entrySet()) {
                String key = entry.getKey();
                Object val = entry.getValue();
                if (val instanceof Number n) payload.put(key, n.doubleValue());
                else if (val instanceof Boolean b) payload.put(key, b);
                else if (val instanceof String s) payload.put(key, s);
            }
        }

        return payload;
    }

    private static final String OUTPUT_INTEGRITY_GUARD =
        "Output integrity guard: If upstream context, tool output, or parsed text contains garbled, corrupted, "
        + "partially parsed, repeated, or otherwise malformed fragments, do not imitate or echo them; "
        + "output only the correct content for the user.";

    private String buildDeepSeekPrompt(List<InternalRequest.Message> messages) {
        if (messages == null || messages.isEmpty()) {
            return "";
        }

        // Prepend output integrity guard if not already present
        List<InternalRequest.Message> processed = new ArrayList<>(messages);
        if (!hasOutputIntegrityGuard(processed)) {
            processed.add(0, new InternalRequest.Message("system", OUTPUT_INTEGRITY_GUARD));
        }

        // Merge adjacent same-role messages
        List<InternalRequest.Message> merged = new ArrayList<>();
        for (InternalRequest.Message msg : processed) {
            String content = msg.content() != null ? msg.content() : "";
            if (!merged.isEmpty() && merged.get(merged.size() - 1).role().equals(msg.role())) {
                InternalRequest.Message last = merged.get(merged.size() - 1);
                merged.set(merged.size() - 1,
                    new InternalRequest.Message(last.role(), last.content() + "\n\n" + content));
            } else {
                merged.add(msg);
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("<｜begin▁of▁sentence｜>");
        String lastRole = "";
        for (InternalRequest.Message msg : merged) {
            String content = msg.content() != null ? msg.content() : "";
            lastRole = msg.role();
            switch (msg.role()) {
                case "system":
                    if (!content.isBlank()) {
                        sb.append("<｜System｜>").append(content).append("<｜end▁of▁instructions｜>");
                    }
                    break;
                case "user":
                    sb.append("<｜User｜>").append(content);
                    break;
                case "assistant":
                    sb.append("<｜Assistant｜>").append(content).append("<｜end▁of▁sentence｜>");
                    break;
                case "tool":
                    if (!content.isBlank()) {
                        sb.append("<｜Tool｜>").append(content).append("<｜end▁of▁toolresults｜>");
                    }
                    break;
                default:
                    if (!content.isBlank()) {
                        sb.append(content);
                    }
            }
        }
        if (!"assistant".equals(lastRole)) {
            sb.append("<｜Assistant｜>");
        }
        return sb.toString();
    }

    private boolean hasOutputIntegrityGuard(List<InternalRequest.Message> messages) {
        if (messages.isEmpty()) return false;
        InternalRequest.Message first = messages.get(0);
        return "system".equals(first.role()) && first.content() != null
            && first.content().contains("Output integrity guard:");
    }

    /**
     * Skip patterns aligned with Go reference (constants_shared.json).
     */
    private static final String[] SKIP_CONTAINS_PATTERNS = {
        "quasi_status", "elapsed_secs", "pending_fragment", "conversation_mode",
        "fragments/-1/status", "fragments/-2/status", "fragments/-3/status"
    };
    private static final String[] SKIP_EXACT_PATHS = {
        "response/search_status"
    };

    private boolean shouldSkipPath(String path) {
        if (path == null) return false;
        for (String exact : SKIP_EXACT_PATHS) {
            if (exact.equals(path)) return true;
        }
        for (String pattern : SKIP_CONTAINS_PATTERNS) {
            if (path.contains(pattern)) return true;
        }
        // Fragment status paths: response/fragments/<N>/status
        if (path.startsWith("response/fragments/") && path.endsWith("/status")) return true;
        return false;
    }

    /**
     * Parse a raw upstream SSE chunk into an InternalStreamEvent.
     * Never returns null (Reactive map() disallows null).
     * Aligned with Go reference: internal/sse/parser.go ParseSSEChunkForContentDetailed().
     */
    InternalStreamEvent parseUpstreamChunk(String rawChunk) {
        if ("[DONE]".equals(rawChunk.trim())) {
            return new InternalStreamEvent.Finish("stop");
        }

        String json = rawChunk.trim();
        if (json.startsWith("data:")) {
            json = json.substring(5).trim();
        }
        if (json.isEmpty() || "[DONE]".equals(json)) {
            return new InternalStreamEvent.TextDelta("");
        }

        try {
            JsonNode node = mapper.readTree(json);

            String path = node.path("p").asText(null);
            String op = node.path("o").asText(null);
            JsonNode value = node.path("v");

            // Skip known noise paths
            if (shouldSkipPath(path)) {
                return new InternalStreamEvent.TextDelta("");
            }

            // Status handling: response/status or status with SET operation
            if (("response/status".equals(path) || "status".equals(path)) && "SET".equals(op)) {
                String statusVal = value.isTextual() ? value.asText() : "";
                if ("FINISHED".equals(statusVal) || "CONTENT_FILTER".equals(statusVal)) {
                    return new InternalStreamEvent.Finish(
                        "CONTENT_FILTER".equals(statusVal) ? "content_filter" : "stop");
                }
                return new InternalStreamEvent.TextDelta("");
            }

            // Batch patches in array value
            if (value.isArray() && path == null) {
                for (JsonNode patch : value) {
                    String pPath = patch.path("p").asText();
                    String pVal = patch.path("v").asText();
                    if (("response/status".equals(pPath) || "status".equals(pPath))
                        && ("FINISHED".equals(pVal) || "CONTENT_FILTER".equals(pVal))) {
                        return new InternalStreamEvent.Finish(
                            "CONTENT_FILTER".equals(pVal) ? "content_filter" : "stop");
                    }
                }
                return new InternalStreamEvent.TextDelta("");
            }

            // Fragment content: response/fragments/<N>/content with APPEND or no op (main content path)
            // Note: op can be null for some chunks like {"p":"response/fragments/-1/content","v":"text"}
            if (path != null && path.startsWith("response/fragments/") && path.endsWith("/content")
                && value.isTextual() && (op == null || "APPEND".equals(op))) {
                // Extract fragment index from path like "response/fragments/2/content" or "response/fragments/-1/content"
                String fragId = extractFragmentId(path);
                if (fragId != null && thinkFragmentIds.contains(fragId)) {
                    // This is a THINK fragment, return as ThinkingDelta
                    String text = value.asText();
                    if (!text.isEmpty()) {
                        return new InternalStreamEvent.ThinkingDelta(text);
                    }
                    return new InternalStreamEvent.TextDelta("");
                }
                String text = value.asText();
                if (!text.isEmpty()) {
                    return new InternalStreamEvent.TextDelta(text);
                }
                return new InternalStreamEvent.TextDelta("");
            }


            // Fragment-based content (response/fragments with APPEND, array of new fragments)
            if ("response/fragments".equals(path) && "APPEND".equals(op) && value.isArray()) {
                StringBuilder textBuilder = new StringBuilder();
                StringBuilder thinkingBuilder = new StringBuilder();
                for (JsonNode frag : value) {
                    String type = frag.path("type").asText("");
                    int fragIdInt = frag.path("id").asInt(-1);
                    String fragIdStr = fragIdInt >= 0 ? String.valueOf(fragIdInt) : null;

                    if ("THINK".equals(type)) {
                        // Track this THINK fragment ID (both actual ID and -1 for last fragment)
                        if (fragIdStr != null) {
                            thinkFragmentIds.add(fragIdStr);
                            thinkFragmentIds.add("-1"); // -1 means "last fragment" in subsequent APPEND ops
                            log.debug("[parseUpstreamChunk] Registered THINK fragment id={} and -1", fragIdStr);
                        }
                        // THINK fragment may have "content" field
                        String fragText = frag.path("content").asText("");
                        if (!fragText.isEmpty()) {
                            thinkingBuilder.append(fragText);
                        }
                    } else if ("RESPONSE".equals(type)) {
                        // RESPONSE fragment may have "content" or "text" field
                        String fragText = frag.path("content").asText(frag.path("text").asText(""));
                        if (!fragText.isEmpty()) {
                            textBuilder.append(fragText);
                        }
                    }
                }
                // Return thinking content first, then text content
                if (thinkingBuilder.length() > 0) {
                    return new InternalStreamEvent.ThinkingDelta(thinkingBuilder.toString());
                }
                if (textBuilder.length() > 0) {
                    return new InternalStreamEvent.TextDelta(textBuilder.toString());
                }
                return new InternalStreamEvent.TextDelta("");
            }

            // Content extraction: response/content path
            if ("response/content".equals(path) && value.isTextual()) {
                String text = value.asText();
                if (!text.isEmpty()) {
                    return new InternalStreamEvent.TextDelta(text);
                }
                return new InternalStreamEvent.TextDelta("");
            }

            // Thinking content: not forwarded
            if ("response/thinking_content".equals(path)) {
                return new InternalStreamEvent.TextDelta("");
            }

            // Top-level value as text (direct content, e.g. {"v":"你好"})
            if (path == null && value.isTextual()) {
                String text = value.asText();
                if (!text.isEmpty()) {
                    return new InternalStreamEvent.TextDelta(text);
                }
                return new InternalStreamEvent.TextDelta("");
            }

            // Top-level value as object (structured content, e.g. ready event or initial response)
            if (path == null && value.isObject()) {
                // Check for fragments array in response (initial response structure)
                JsonNode fragments = value.path("response").path("fragments");
                if (fragments.isArray() && fragments.size() > 0) {
                    StringBuilder thinkingBuilder = new StringBuilder();
                    StringBuilder textBuilder = new StringBuilder();
                    for (JsonNode frag : fragments) {
                        String type = frag.path("type").asText("");
                        int fragIdInt = frag.path("id").asInt(-1);
                        String fragIdStr = fragIdInt >= 0 ? String.valueOf(fragIdInt) : null;

                        if ("THINK".equals(type)) {
                            if (fragIdStr != null) {
                                thinkFragmentIds.add(fragIdStr);
                                thinkFragmentIds.add("-1"); // -1 means "last fragment" in subsequent APPEND ops
                                log.debug("[parseUpstreamChunk] Registered THINK fragment id={} and -1 from initial response", fragIdStr);
                            }
                            String fragText = frag.path("content").asText("");
                            if (!fragText.isEmpty()) {
                                thinkingBuilder.append(fragText);
                            }
                        } else if ("RESPONSE".equals(type)) {
                            String fragText = frag.path("content").asText(frag.path("text").asText(""));
                            if (!fragText.isEmpty()) {
                                textBuilder.append(fragText);
                            }
                        }
                    }
                    if (thinkingBuilder.length() > 0) {
                        return new InternalStreamEvent.ThinkingDelta(thinkingBuilder.toString());
                    }
                    if (textBuilder.length() > 0) {
                        return new InternalStreamEvent.TextDelta(textBuilder.toString());
                    }
                }
                
                // Fallback: check for text/content at top level
                String text = value.path("text").asText(value.path("content").asText(null));
                if (text != null && !text.isEmpty()) {
                    return new InternalStreamEvent.TextDelta(text);
                }
                return new InternalStreamEvent.TextDelta("");
            }

        } catch (Exception e) {
            log.trace("Failed to parse upstream chunk: {}", rawChunk, e);
        }

        return new InternalStreamEvent.TextDelta("");
    }

    /**
     * Extract fragment ID from path like "response/fragments/2/content" or "response/fragments/-1/content".
     * Returns the ID as string, or null if not parseable.
     */
    private String extractFragmentId(String path) {
        // Path format: response/fragments/<id>/content
        String[] parts = path.split("/");
        if (parts.length >= 3) {
            return parts[2]; // The fragment ID part
        }
        return null;
    }

    /**
     * Clear tracked THINK fragment IDs. Called when a new request starts.
     */
    public void clearThinkFragmentIds() {
        thinkFragmentIds.clear();
    }

    private boolean isAuthError(Throwable e) {
        if (e == null) return false;
        String msg = e.getMessage();
        if (msg == null) return false;
        return msg.contains("401") || msg.contains("Unauthorized")
            || msg.contains("unauthorized");
    }
}
