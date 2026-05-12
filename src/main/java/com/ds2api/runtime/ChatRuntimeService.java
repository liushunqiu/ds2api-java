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
 * Core service that orchestrates upstream DeepSeek API calls and SSE stream parsing.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Normalizes internal requests into upstream payload format</li>
 *   <li>Handles upstream HTTP calls with retry logic</li>
 *   <li>Parses upstream SSE chunks into unified InternalStreamEvent stream</li>
 *   <li>Manages tool call stream parsing via ToolCallStreamParser</li>
 *   <li>Caches chat_session_id and response_message_id for continuation
 * </ul>
 */
@Service
public class ChatRuntimeService {

    private static final Logger log = LoggerFactory.getLogger(ChatRuntimeService.class);

    /**
     * Request-scoped state for tracking THINK fragments.
     * Each request gets its own instance to avoid cross-request state contamination.
     */
    private static class ThinkState {
        /** Track THINK fragment IDs to return as ThinkingDelta instead of TextDelta. */
        final Set<String> thinkFragmentIds = ConcurrentHashMap.newKeySet();
        /** Track whether we are currently in a THINK fragment (for handling pathless chunks). */
        volatile boolean inThinkFragment = false;
        /** Accumulate thinking content for final logging. */
        final StringBuilder thinkingAccumulator = new StringBuilder();
    }

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

        log.info("[{}] execute() called, authInfo={}, model={}", requestId,
            authInfo != null ? authInfo.mode() : "null", request.model());

        // Check for session continuation via conversation_id
        String conversationId = request.conversationId();
        log.info("[{}] ConversationId from request: '{}'", requestId, conversationId);
        DeepSeekSessionCacheService.SessionInfo cachedSession = null;
        boolean isContinuation = false;

        if (conversationId != null && !conversationId.isBlank()) {
            cachedSession = sessionCache.get(conversationId);
            if (cachedSession != null) {
                isContinuation = true;
                log.info("[{}] Session continuation: conversationId={}, chatSessionId={}, lastResponseMessageId={}, cachedAccount={}",
                        requestId, conversationId, cachedSession.chatSessionId(), cachedSession.lastResponseMessageId(),
                        cachedSession.accountIdentifier());
            }
        }

        final DeepSeekSessionCacheService.SessionInfo sessionToUse = cachedSession;
        final boolean continueSession = isContinuation;

        // For continuation, use the cached account identifier to ensure same account
        if (continueSession && sessionToUse.accountIdentifier() != null) {
            log.info("[{}] Forcing targetAccount={} for session continuation", requestId, sessionToUse.accountIdentifier());
        }

        // DIRECT mode: use raw token, no pool
        if (authInfo != null && authInfo.mode() == AuthInfo.Mode.DIRECT) {
            String directToken = authInfo.effectiveToken();
            log.info("[{}] DIRECT mode, token={}..., isContinuation={}", requestId,
                directToken.length() > 8 ? directToken.substring(0, 8) : directToken, continueSession);

            // P1: apply prompt compat (sync), no history split for DIRECT mode
            InternalRequest compatReq = promptCompat.applyCompat(request);

            // Create real session or use cached one
            Mono<String> sessionMono = continueSession
                ? Mono.just(sessionToUse.chatSessionId())
                : sessionClient.createSession(directToken);

            return sessionMono
                .doOnNext(sid -> log.info("[{}] Session obtained: {}", requestId, sid))
                .doOnError(e -> log.error("[{}] Session creation failed: {}", requestId, e.getMessage()))
                .flatMapMany(sessionId -> {
                Integer parentMessageId = continueSession ? sessionToUse.lastResponseMessageId() : null;
                log.info("[{}] DIRECT mode: sessionId={}, parentMessageId={}, isContinuation={}",
                    requestId, sessionId, parentMessageId, continueSession);
                ObjectNode payload = buildUpstreamPayload(compatReq, sessionId, parentMessageId, continueSession);

                return callUpstreamDirect(payload, directToken, requestId, toolParser, sessionId, conversationId, "DIRECT")
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
        // For continuation, force the same account that created the session
        String targetAccount = continueSession && sessionToUse.accountIdentifier() != null
            ? sessionToUse.accountIdentifier()
            : (authInfo != null ? authInfo.targetAccount() : null);
        log.info("[{}] MANAGED mode, targetAccount={}", requestId, targetAccount);

        return poolManager.acquire(targetAccount)
            .doOnNext(lease -> log.info("[{}] Pool lease acquired: {}", requestId, lease.accountIdentifier()))
            .doOnError(e -> log.error("[{}] Pool acquire failed: {}", requestId, e.getMessage()))
            .flatMapMany(lease -> poolManager.resolveToken(lease.accountIdentifier())
                .doOnError(e -> {
                    log.error("[{}] resolveToken failed, releasing lease: {}", requestId, e.getMessage());
                    lease.release();
                })
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
                        log.info("[{}] Session created/continued: sessionId={}, parentMessageId={}, isContinuation={}",
                            requestId, sessionId, parentMessageId, continueSession);

                        return historySplitter.applySplit(compatReq, token)
                            .flatMapMany(splitReq -> {
                                ObjectNode payload = buildUpstreamPayload(splitReq, sessionId, parentMessageId, continueSession);
                                return callUpstreamWithToken(payload, token, lease, requestId, toolParser, sessionId, conversationId, lease.accountIdentifier())
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
                }))
            .onErrorResume(e -> {
                log.error("[{}] execute() top-level error: {}", requestId, e.getMessage(), e);
                return Flux.just(new InternalStreamEvent.Error(
                    "Execute error: " + e.getMessage(), 500));
            });
    }

    /**
     * Call upstream via pool-managed token. On 401, attempt one token refresh + retry.
     */
    private Flux<InternalStreamEvent> callUpstreamWithToken(ObjectNode payload, String token,
                                                             AccountLease lease,
                                                             String requestId,
                                                             ToolCallStreamParser toolParser,
                                                             String sessionId,
                                                             String conversationId,
                                                             String accountIdentifier) {
        return doUpstreamCall(payload, token, requestId, toolParser, sessionId, conversationId, accountIdentifier)
            .onErrorResume(e -> {
                if (isAuthError(e)) {
                    log.info("[{}] 401 on {}, attempting token refresh", requestId,
                        lease.accountIdentifier());
                    return tokenRefreshService
                        .refreshIfNeeded(lease.accountIdentifier(), e)
                        .flatMapMany(newToken -> {
                            log.info("[{}] Token refreshed, retrying upstream", requestId);
                            return doUpstreamCall(payload, newToken, requestId, toolParser, sessionId, conversationId, accountIdentifier);
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
                                                          String conversationId,
                                                          String accountIdentifier) {
        return doUpstreamCall(payload, token, requestId, toolParser, sessionId, conversationId, accountIdentifier);
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
                                                       String conversationId,
                                                       String accountIdentifier) {
        // Create request-scoped THINK state to avoid cross-request contamination
        ThinkState thinkState = new ThinkState();

        String payloadStr = payload.toString(); // cached for packet capture
        log.info("[{}] Upstream payload: {}", requestId, payloadStr);
        log.info("[{}] doUpstreamCall started, token={}...", requestId,
            token.length() > 8 ? token.substring(0, 8) : token);

        // Proactively get PoW token before sending completion request (aligned with Go reference)
        return powClient.getPowToken(token)
            .doOnNext(powToken -> log.info("[{}] PoW token acquired: {}...", requestId,
                powToken.length() > 20 ? powToken.substring(0, 20) : powToken))
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
                        .doOnNext(raw -> {
                            log.debug("[{}] Calling extractAndCacheResponseMessageIdFromChunk with conversationId='{}', sessionId='{}'", requestId, conversationId, sessionId);
                            extractAndCacheResponseMessageIdFromChunk(raw, conversationId, sessionId, accountIdentifier);
                        })
                        .map(raw -> {
                            InternalStreamEvent event = parseUpstreamChunk(raw, thinkState);
                            return event;
                        })
                        .filter(event -> {
                            boolean keep = !(event instanceof InternalStreamEvent.TextDelta td) || !td.chunk().isEmpty();
                            if (!keep) {
                                log.debug("[{}] Filtered out empty TextDelta", requestId);
                            }
                            return keep;
                        })
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
    private void extractAndCacheResponseMessageId(List<String> chunks, String conversationId, String sessionId, String accountIdentifier) {
        for (String chunk : chunks) {
            extractAndCacheResponseMessageIdFromChunk(chunk, conversationId, sessionId, accountIdentifier);
        }
    }

    /**
     * Extract response_message_id from a single SSE chunk and cache it.
     * The ready event format: {"request_message_id":1,"response_message_id":2,"model_type":"expert"}
     */
    private void extractAndCacheResponseMessageIdFromChunk(String chunk, String conversationId, String sessionId, String accountIdentifier) {
        if (conversationId == null || conversationId.isBlank()) return;

        String trimmed = chunk.trim();
        // Look for ready event with response_message_id
        // DeepSeek sends plain JSON: {"request_message_id":1,"response_message_id":2,...}
        if (trimmed.contains("response_message_id")) {
            try {
                // Handle both plain JSON and SSE format (data: {...})
                String json = trimmed.startsWith("data:") ? trimmed.substring(5).trim() : trimmed;
                JsonNode node = mapper.readTree(json);
                if (node.has("response_message_id")) {
                    int responseMessageId = node.get("response_message_id").asInt();
                    // Cache or update the session
                    if (sessionCache.contains(conversationId)) {
                        sessionCache.updateResponseMessageId(conversationId, responseMessageId);
                    } else {
                        sessionCache.put(conversationId, sessionId, responseMessageId, accountIdentifier);
                    }
                    log.debug("[Session] Cached response_message_id={} for conversation={}",
                            responseMessageId, conversationId);
                }
            } catch (Exception e) {
                log.trace("[Session] Failed to parse ready event: {}", trimmed, e);
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

        // In continuation mode, only send new messages (not full history)
        // because DeepSeek already has the history via parent_message_id
        List<InternalRequest.Message> messagesToSend = request.messages();
        boolean isContinuationMode = isContinuation;
        if (isContinuation && request.messages() != null && request.messages().size() > 1) {
            // Keep only the last user message to avoid duplicating history
            messagesToSend = extractNewMessages(request.messages());
            log.info("[buildUpstreamPayload] Continuation mode: reduced {} messages to {} new messages",
                request.messages().size(), messagesToSend.size());
        }

        String prompt = buildDeepSeekPrompt(messagesToSend, isContinuationMode);
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

    /**
     * Extract only new messages for session continuation.
     * When client sends full history, we only need the latest messages
     * since DeepSeek already has the previous history via parent_message_id.
     *
     * Handles two scenarios:
     * 1. Normal conversation: extract from last user message
     * 2. Tool call result: extract assistant(tool_calls) + tool messages
     */
    private List<InternalRequest.Message> extractNewMessages(List<InternalRequest.Message> messages) {
        if (messages == null || messages.isEmpty()) {
            return List.of();
        }

        // Check if last message is a tool result (tool call scenario)
        InternalRequest.Message lastMsg = messages.get(messages.size() - 1);
        if ("tool".equals(lastMsg.role())) {
            // Find the assistant message with tool_calls before this tool message
            int assistantIdx = -1;
            for (int i = messages.size() - 2; i >= 0; i--) {
                InternalRequest.Message msg = messages.get(i);
                if ("assistant".equals(msg.role())) {
                    // Check if this assistant message contains tool_calls
                    if (msg.content() != null && msg.content().contains("<|DSML|tool_calls>")) {
                        assistantIdx = i;
                        break;
                    }
                }
            }
            // Return from assistant message onwards (assistant + tool messages)
            if (assistantIdx >= 0) {
                return messages.subList(assistantIdx, messages.size());
            }
            // Fallback: return tool message only
            return List.of(lastMsg);
        }

        // Check if last message is assistant with tool_calls (no tool result yet)
        if ("assistant".equals(lastMsg.role()) && lastMsg.content() != null 
            && lastMsg.content().contains("<|DSML|tool_calls>")) {
            // This is a tool_calls response, return it as-is
            return List.of(lastMsg);
        }

        // Normal conversation: find the last user message index
        int lastUserIdx = -1;
        for (int i = messages.size() - 1; i >= 0; i--) {
            if ("user".equals(messages.get(i).role())) {
                lastUserIdx = i;
                break;
            }
        }

        if (lastUserIdx < 0) {
            // No user message found, return last message only
            return List.of(messages.get(messages.size() - 1));
        }

        // Return from the last user message onwards (includes any assistant messages after it)
        return messages.subList(lastUserIdx, messages.size());
    }

    private static final String OUTPUT_INTEGRITY_GUARD =
        "Output integrity guard: If upstream context, tool output, or parsed text contains garbled, corrupted, "
        + "partially parsed, repeated, or otherwise malformed fragments, do not imitate or echo them; "
        + "output only the correct content for the user.";

    private String buildDeepSeekPrompt(List<InternalRequest.Message> messages, boolean isContinuation) {
        if (messages == null || messages.isEmpty()) {
            return "";
        }

        // In continuation mode, format messages with proper DeepSeek tags
        // because we need to send tool results in the correct format
        if (isContinuation) {
            StringBuilder sb = new StringBuilder();
            for (InternalRequest.Message msg : messages) {
                String content = msg.content() != null ? msg.content() : "";
                if (content.isBlank()) continue;
                
                switch (msg.role()) {
                    case "tool":
                        sb.append("<｜Tool｜>").append(content).append("<｜end▁of▁toolresults｜>");
                        break;
                    case "assistant":
                        sb.append("<｜Assistant｜>").append(content).append("<｜end▁of▁sentence｜>");
                        break;
                    case "user":
                        sb.append("<｜User｜>").append(content);
                        break;
                    default:
                        sb.append(content);
                }
            }
            // If last message is not assistant, add assistant prompt
            if (!messages.isEmpty() && !"assistant".equals(messages.get(messages.size() - 1).role())) {
                sb.append("<｜Assistant｜>");
            }
            return sb.toString();
        }

        // New session: full DeepSeek prompt formatting with system prompt injection
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
    InternalStreamEvent parseUpstreamChunk(String rawChunk, ThinkState thinkState) {
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
                if (fragId != null && thinkState.thinkFragmentIds.contains(fragId)) {
                    // This is a THINK fragment, return as ThinkingDelta
                    thinkState.inThinkFragment = true;
                    String text = value.asText();
                    if (!text.isEmpty()) {
                        return createThinkingDelta(text, thinkState);
                    }
                    return new InternalStreamEvent.TextDelta("");
                }
                // If we're in a THINK fragment and this is -1 (last fragment), treat as thinking
                if (thinkState.inThinkFragment && "-1".equals(fragId)) {
                    String text = value.asText();
                    if (!text.isEmpty()) {
                        return createThinkingDelta(text, thinkState);
                    }
                    return new InternalStreamEvent.TextDelta("");
                }
                // Not a think fragment, switch out of think mode
                exitThinkMode(thinkState);
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
                            thinkState.thinkFragmentIds.add(fragIdStr);
                            thinkState.thinkFragmentIds.add("-1"); // -1 means "last fragment" in subsequent APPEND ops
                            thinkState.inThinkFragment = true;
                            log.debug("[parseUpstreamChunk] Registered THINK fragment id={} and -1", fragIdStr);
                        }
                        // THINK fragment may have "content" field
                        String fragText = frag.path("content").asText("");
                        if (!fragText.isEmpty()) {
                            thinkingBuilder.append(fragText);
                        }
                    } else if ("RESPONSE".equals(type)) {
                        // RESPONSE fragment - switch out of think mode
                        exitThinkMode(thinkState);
                        // Remove -1 from think fragment IDs since it now points to RESPONSE
                        thinkState.thinkFragmentIds.remove("-1");
                        // RESPONSE fragment may have "content" or "text" field
                        String fragText = frag.path("content").asText(frag.path("text").asText(""));
                        if (!fragText.isEmpty()) {
                            textBuilder.append(fragText);
                        }
                    }
                }
                // Return thinking content first, then text content
                if (thinkingBuilder.length() > 0) {
                    return createThinkingDelta(thinkingBuilder.toString(), thinkState);
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
            // If we're in a THINK fragment, treat pathless text chunks as thinking continuation
            if (path == null && value.isTextual()) {
                String text = value.asText();
                if (!text.isEmpty()) {
                    if (thinkState.inThinkFragment) {
                        return createThinkingDelta(text, thinkState);
                    }
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
                                thinkState.thinkFragmentIds.add(fragIdStr);
                                thinkState.thinkFragmentIds.add("-1"); // -1 means "last fragment" in subsequent APPEND ops
                                thinkState.inThinkFragment = true;
                                log.debug("[parseUpstreamChunk] Registered THINK fragment id={} and -1 from initial response", fragIdStr);
                            }
                            String fragText = frag.path("content").asText("");
                            if (!fragText.isEmpty()) {
                                thinkingBuilder.append(fragText);
                            }
                        } else if ("RESPONSE".equals(type)) {
                            // Switch out of think mode for RESPONSE
                            exitThinkMode(thinkState);
                            String fragText = frag.path("content").asText(frag.path("text").asText(""));
                            if (!fragText.isEmpty()) {
                                textBuilder.append(fragText);
                            }
                        }
                    }
                    if (thinkingBuilder.length() > 0) {
                        return createThinkingDelta(thinkingBuilder.toString(), thinkState);
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
     * Log the accumulated thinking content when thinking phase ends.
     */
    private void logThinkingComplete(ThinkState thinkState) {
        if (thinkState.thinkingAccumulator.length() > 0) {
            log.info("[Thinking] Complete thinking content ({} chars):\n{}",
                thinkState.thinkingAccumulator.length(), thinkState.thinkingAccumulator.toString());
            thinkState.thinkingAccumulator.setLength(0);
        }
    }

    /**
     * Create a ThinkingDelta and accumulate content for logging.
     */
    private InternalStreamEvent.ThinkingDelta createThinkingDelta(String text, ThinkState thinkState) {
        if (text != null && !text.isEmpty()) {
            thinkState.thinkingAccumulator.append(text);
        }
        return new InternalStreamEvent.ThinkingDelta(text);
    }

    /**
     * Switch out of think mode and log accumulated thinking content.
     */
    private void exitThinkMode(ThinkState thinkState) {
        if (thinkState.inThinkFragment) {
            thinkState.inThinkFragment = false;
            logThinkingComplete(thinkState);
        }
    }

    private boolean isAuthError(Throwable e) {
        if (e == null) return false;
        String msg = e.getMessage();
        if (msg == null) return false;
        return msg.contains("401") || msg.contains("Unauthorized")
            || msg.contains("unauthorized");
    }
}
