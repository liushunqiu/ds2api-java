package com.ds2api.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * Session cleanup client for auto_delete feature.
 * Clears upstream DeepSeek sessions to prevent account session
 * pileup that could trigger risk-control mechanisms.
 */
@Service
public class DeepSeekSessionClient {

    private static final Logger log = LoggerFactory.getLogger(DeepSeekSessionClient.class);

    private final WebClient deepSeekWebClient;
    private final ObjectMapper mapper;

    public DeepSeekSessionClient(WebClient deepSeekWebClient, ObjectMapper mapper) {
        this.deepSeekWebClient = deepSeekWebClient;
        this.mapper = mapper;
    }

    /**
     * Create a new upstream session via POST /api/v0/chat_session/create.
     * Aligned with Go reference: internal/deepseek/client/client_auth.go CreateSession().
     *
     * @param accountToken Bearer token for the account
     * @return Mono with the created session ID
     */
    public Mono<String> createSession(String accountToken) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("agent", "chat");

        return deepSeekWebClient.post()
            .uri("/api/v0/chat_session/create")
            .header("Authorization", "Bearer " + accountToken)
            .bodyValue(payload)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(resp -> {
                int code = resp.path("code").asInt(-1);
                int bizCode = resp.path("data").path("biz_code").asInt(-1);
                if (code != 0 || bizCode != 0) {
                    String msg = resp.path("msg").asText("unknown");
                    String bizMsg = resp.path("data").path("biz_msg").asText("");
                    log.warn("[Session] Create failed: code={}, biz_code={}, msg={}, biz_msg={}",
                        code, bizCode, msg, bizMsg);
                    throw new RuntimeException("Session create failed: " + msg + " " + bizMsg);
                }
                // Extract session ID: data.biz_data.id or data.biz_data.chat_session.id
                JsonNode bizData = resp.path("data").path("biz_data");
                String sessionId = bizData.path("id").asText(null);
                if (sessionId == null || sessionId.isBlank()) {
                    sessionId = bizData.path("chat_session").path("id").asText(null);
                }
                if (sessionId == null || sessionId.isBlank()) {
                    log.error("[Session] No session ID in response: {}", resp);
                    throw new RuntimeException("Session create response missing ID");
                }
                log.debug("[Session] Created session: {}", sessionId);
                return sessionId;
            })
            .doOnError(e -> log.warn("[Session] Create failed: {}", e.getMessage()));
    }

    /**
     * Delete upstream session(s).
     *
     * @param sessionId    The session ID to delete (used when mode=single)
     * @param mode         none / single / all
     * @param accountToken Bearer token for the account
     * @return Mono that completes empty on success; errors are suppressed
     */
    public Mono<Void> deleteSession(String sessionId, String mode, String accountToken) {
        if (mode == null || "none".equalsIgnoreCase(mode)) {
            return Mono.empty();
        }

        boolean deleteAll = "all".equalsIgnoreCase(mode);
        String uri = deleteAll ? "/api/v0/chat_session/delete_all" : "/api/v0/chat_session/delete";

        ObjectNode payload = mapper.createObjectNode();
        if (!deleteAll) {
            payload.put("chat_session_id", sessionId);
        }

        return deepSeekWebClient.post()
            .uri(uri)
            .header("Authorization", "Bearer " + accountToken)
            .bodyValue(payload)
            .retrieve()
            .toBodilessEntity()
            .doOnSuccess(resp -> log.debug("[Session] Deleted session(s) mode={}", mode))
            .doOnError(e -> log.warn("[Session] Delete failed mode={}: {}", mode, e.getMessage()))
            .onErrorResume(e -> Mono.empty()) // cleanup failure must not block the main flow
            .then();
    }
}
