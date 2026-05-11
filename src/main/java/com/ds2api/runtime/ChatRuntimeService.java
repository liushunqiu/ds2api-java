package com.ds2api.runtime;

import com.ds2api.admin.dev.PacketCaptureService;
import com.ds2api.auth.AuthInfo;
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
import java.util.UUID;

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
 * PoW is handled transparently by the DeepSeekPowRetryFilter registered on the WebClient.
 *
 * P2 additions:
 *   - Exponential backoff retry on upstream 429/503 (max 3 attempts, 500ms initial)
 *   - Packet capture integration via PacketCaptureService
 */
@Service
public class ChatRuntimeService {

    private static final Logger log = LoggerFactory.getLogger(ChatRuntimeService.class);

    private final WebClient deepSeekClient;
    private final ConfigLoaderService configLoader;
    private final ModelAliasService modelAlias;
    private final AccountPoolManager poolManager;
    private final TokenRefreshService tokenRefreshService;
    private final PromptCompatService promptCompat;
    private final HistoryFileSplitter historySplitter;
    private final DeepSeekSessionClient sessionClient;
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

        // DIRECT mode: use raw token, no pool
        if (authInfo != null && authInfo.mode() == AuthInfo.Mode.DIRECT) {
            String directToken = authInfo.effectiveToken();
            log.debug("[{}] DIRECT mode, token={}...", requestId,
                directToken.length() > 8 ? directToken.substring(0, 8) : directToken);

            // P1: apply prompt compat (sync), no history split for DIRECT mode
            InternalRequest compatReq = promptCompat.applyCompat(request);
            // Generate session ID for cleanup
            String sessionId = "sess_" + UUID.randomUUID().toString().substring(0, 8);
            ObjectNode payload = buildUpstreamPayload(compatReq, sessionId);

            return callUpstreamDirect(payload, directToken, requestId, toolParser, sessionId)
                .doFinally(signal -> {
                    toolParser.flushAndReset();
                    toolParser.reset();
                    sessionClient.deleteSession(sessionId,
                        config.getAutoDelete().getMode(), directToken).subscribe();
                });
        }

        // MANAGED mode: acquire pool slot, resolve token, apply P1 pipeline
        String targetAccount = authInfo != null ? authInfo.targetAccount() : null;
        if (targetAccount != null) {
            log.debug("[{}] MANAGED target={}", requestId, targetAccount);
        }

        return poolManager.acquire(targetAccount)
            .flatMapMany(lease -> {
                String token = poolManager.resolveToken(lease.accountIdentifier());
                if (token == null || token.isBlank()) {
                    return Flux.<InternalStreamEvent>error(
                        new IllegalStateException(
                            "No token for account " + lease.accountIdentifier()));
                }
                log.debug("[{}] Acquired slot for {}", requestId, lease.accountIdentifier());
                String sessionId = "sess_" + UUID.randomUUID().toString().substring(0, 8);

                // P1 pipeline: Compat (sync) -> Split (async) -> upstream call
                InternalRequest compatReq = promptCompat.applyCompat(request);
                return historySplitter.applySplit(compatReq, token)
                    .flatMapMany(splitReq -> {
                        ObjectNode payload = buildUpstreamPayload(splitReq, sessionId);
                        return callUpstreamWithToken(payload, token, lease, requestId, toolParser, sessionId)
                            .doFinally(signal -> {
                                toolParser.flushAndReset();
                                toolParser.reset();
                                // auto_delete session cleanup (fire-and-forget)
                                sessionClient.deleteSession(sessionId,
                                    config.getAutoDelete().getMode(), token).subscribe();
                                lease.release();
                                log.debug("[{}] Released slot for {}", requestId,
                                    lease.accountIdentifier());
                            });
                    });
            });
    }

    /**
     * Call upstream via pool-managed token. On 401, attempt one token refresh + retry.
     */
    private Flux<InternalStreamEvent> callUpstreamWithToken(ObjectNode payload, String token,
                                                             AccountLease lease,
                                                             String requestId,
                                                             ToolCallStreamParser toolParser,
                                                             String sessionId) {
        return doUpstreamCall(payload, token, requestId, toolParser, sessionId)
            .onErrorResume(e -> {
                if (isAuthError(e)) {
                    log.info("[{}] 401 on {}, attempting token refresh", requestId,
                        lease.accountIdentifier());
                    return tokenRefreshService
                        .refreshIfNeeded(lease.accountIdentifier(), e)
                        .flatMapMany(newToken -> {
                            log.info("[{}] Token refreshed, retrying upstream", requestId);
                            return doUpstreamCall(payload, newToken, requestId, toolParser, sessionId);
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
                                                          String sessionId) {
        return doUpstreamCall(payload, token, requestId, toolParser, sessionId);
    }

    /**
     * Core upstream HTTP call: POST /api/v1/chat/completions with SSE streaming.
     *
     * P2: exponential backoff retry on 429 (Rate Limit) and 503 (Service Unavailable),
     * with packet capture accumulation after successful completion.
     */
    private Flux<InternalStreamEvent> doUpstreamCall(ObjectNode payload, String token,
                                                      String requestId,
                                                      ToolCallStreamParser toolParser,
                                                      String sessionId) {
        String payloadStr = payload.toString(); // cached for packet capture

        return deepSeekClient.post()
            .uri("/api/v1/chat/completions")
            .header("Authorization", "Bearer " + token)
            .bodyValue(payload)
            .retrieve()
            .onStatus(status -> status.is4xxClientError() || status.is5xxServerError(),
                resp -> resp.createException().flatMap(ex -> {
                    log.error("[{}] Upstream error {}: {}", requestId,
                        resp.statusCode(), ex.getMessage());
                    return Mono.<Throwable>error(
                        new UpstreamException(resp.statusCode(), ex.getMessage()));
                }))
            .bodyToFlux(String.class)
            // P2: exponential backoff retry for 429/503 only
            .retryWhen(Retry.backoff(3, Duration.ofMillis(500))
                .filter(e -> e instanceof UpstreamException ue &&
                             (ue.getStatusCode().value() == 429 ||
                              ue.getStatusCode().value() == 503))
                .doBeforeRetry(sig -> log.warn("[{}] Retry upstream attempt {}: {}",
                    requestId, sig.totalRetries() + 1, sig.failure().getMessage()))
                .onRetryExhaustedThrow((spec, sig) -> sig.failure()))
            // Buffer chunks for packet capture, then re-emit downstream
            .collectList()
            .doOnSuccess(chunks -> {
                String fullResp = String.join("", chunks);
                captureService.push(sessionId, payloadStr, fullResp);
            })
            .flatMapMany(Flux::fromIterable)
            .map(this::parseUpstreamChunk)
            .concatMap(event -> {
                List<InternalStreamEvent> all = new ArrayList<>();
                all.add(event);
                if (event instanceof InternalStreamEvent.TextDelta td && !td.chunk().isEmpty()) {
                    all.addAll(toolParser.processChunk(td.chunk()));
                }
                return Flux.fromIterable(all);
            })
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
    }

    private ObjectNode buildUpstreamPayload(InternalRequest request, String sessionId) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("chat_id", UUID.randomUUID().toString().replace("-", ""));
        payload.put("chat_session_id", sessionId);

        ArrayNode messages = payload.putArray("messages");
        for (InternalRequest.Message msg : request.messages()) {
            ObjectNode m = messages.addObject();
            m.put("role", msg.role());
            m.put("content", msg.content());
        }

        ModelAliasService.ModelConfig mc = modelAlias.getModelConfig(request.model());

        payload.put("parent_message_id", "");
        payload.put("prompt", "");
        payload.put("model", request.model());
        payload.put("stream", true);
        payload.put("thinking_enabled", mc.thinkingEnabled());
        payload.put("search_enabled", mc.searchEnabled());

        if (request.tools() != null && !request.tools().isNull() && request.tools().isArray()) {
            payload.set("plugins", request.tools());
        }

        return payload;
    }

    /**
     * Parse a raw upstream SSE chunk into an InternalStreamEvent.
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
            return null;
        }

        try {
            JsonNode node = mapper.readTree(json);

            String path = node.path("p").asText(null);
            String op = node.path("o").asText(null);
            JsonNode value = node.path("v");

            if (("response/status".equals(path) || "status".equals(path)) && "SET".equals(op)) {
                String statusVal = value.isTextual() ? value.asText() : "";
                if ("FINISHED".equals(statusVal) || "CONTENT_FILTER".equals(statusVal)) {
                    return new InternalStreamEvent.Finish(
                        "CONTENT_FILTER".equals(statusVal) ? "content_filter" : "stop");
                }
                return null;
            }

            if ("quasi_status".equals(path) && "SET".equals(op)) {
                return null;
            }

            if (value.isArray()) {
                for (JsonNode patch : value) {
                    String pPath = patch.path("p").asText();
                    String pVal = patch.path("v").asText();
                    if (("response/status".equals(pPath) || "status".equals(pPath))
                        && ("FINISHED".equals(pVal) || "CONTENT_FILTER".equals(pVal))) {
                        return new InternalStreamEvent.Finish(
                            "CONTENT_FILTER".equals(pVal) ? "content_filter" : "stop");
                    }
                }
                return null;
            }

            if (path == null && value.isTextual()) {
                String text = value.asText();
                if (!text.isEmpty()) {
                    return new InternalStreamEvent.TextDelta(text);
                }
            }

            if (path == null && value.isObject()) {
                String text = value.path("text").asText(value.path("content").asText(null));
                if (text != null && !text.isEmpty()) {
                    return new InternalStreamEvent.TextDelta(text);
                }
            }

        } catch (Exception e) {
            log.trace("Failed to parse upstream chunk: {}", rawChunk, e);
        }

        return new InternalStreamEvent.TextDelta("");
    }

    private boolean isAuthError(Throwable e) {
        if (e == null) return false;
        String msg = e.getMessage();
        if (msg == null) return false;
        return msg.contains("401") || msg.contains("Unauthorized")
            || msg.contains("unauthorized");
    }
}
