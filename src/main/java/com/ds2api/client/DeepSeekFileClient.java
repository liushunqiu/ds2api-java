package com.ds2api.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * File upload client for DeepSeek history context.
 * Uploads conversation history as DS2API_HISTORY.txt when
 * current_input_file splitting is triggered.
 */
@Service
public class DeepSeekFileClient {

    private static final Logger log = LoggerFactory.getLogger(DeepSeekFileClient.class);

    private final WebClient deepSeekWebClient;
    private final ObjectMapper mapper;

    public DeepSeekFileClient(WebClient deepSeekWebClient, ObjectMapper mapper) {
        this.deepSeekWebClient = deepSeekWebClient;
        this.mapper = mapper;
    }

    /**
     * Upload history context as a file.
     *
     * @param content      Full conversation history text
     * @param accountToken Bearer token for the account
     * @return Mono with the file reference ID from the upstream response
     */
    public Mono<String> uploadHistoryFile(String content, String accountToken) {
        String payload;
        try {
            payload = mapper.writeValueAsString(java.util.Map.of(
                "file_name", "DS2API_HISTORY.txt",
                "content", content,
                "purpose", "context"
            ));
        } catch (Exception e) {
            return Mono.error(new RuntimeException("Failed to serialize upload payload", e));
        }

        return deepSeekWebClient.post()
            .uri("/api/v1/files/upload")
            .header("Authorization", "Bearer " + accountToken)
            .header("Content-Type", "application/json")
            .bodyValue(payload)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(resp -> {
                // Flexible parsing: try file_id, id, ref, data.file_id
                String ref = resp.path("data").path("file_id").asText(null);
                if (ref == null) ref = resp.path("file_id").asText(null);
                if (ref == null) ref = resp.path("id").asText(null);
                if (ref == null) ref = resp.path("ref").asText(null);
                if (ref == null) {
                    throw new RuntimeException(
                        "File upload response missing reference ID: " + resp);
                }
                log.info("[File] History context uploaded, ref={}", ref);
                return ref;
            })
            .subscribeOn(Schedulers.boundedElastic())
            .doOnError(e -> log.warn("[File] Upload failed: {}", e.getMessage()));
    }
}
