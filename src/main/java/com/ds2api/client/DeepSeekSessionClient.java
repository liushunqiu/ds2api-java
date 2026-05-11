package com.ds2api.client;

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

    public DeepSeekSessionClient(WebClient deepSeekWebClient) {
        this.deepSeekWebClient = deepSeekWebClient;
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

        String uri = "/api/v1/chat/sessions/"
            + ("all".equalsIgnoreCase(mode) ? "clear" : sessionId);

        return deepSeekWebClient.delete()
            .uri(uri)
            .header("Authorization", "Bearer " + accountToken)
            .retrieve()
            .toBodilessEntity()
            .doOnSuccess(resp -> log.debug("[Session] Deleted session(s) mode={}", mode))
            .doOnError(e -> log.warn("[Session] Delete failed mode={}: {}", mode, e.getMessage()))
            .onErrorResume(e -> Mono.empty()) // cleanup failure must not block the main flow
            .then();
    }
}
