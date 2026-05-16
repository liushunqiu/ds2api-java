package com.ds2api.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.BodyInserters;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;

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
    private final DeepSeekPowClient powClient;

    public DeepSeekFileClient(WebClient deepSeekWebClient, ObjectMapper mapper,
                              DeepSeekPowClient powClient) {
        this.deepSeekWebClient = deepSeekWebClient;
        this.mapper = mapper;
        this.powClient = powClient;
    }

    public DeepSeekFileClient(WebClient deepSeekWebClient, ObjectMapper mapper) {
        this(deepSeekWebClient, mapper, new DeepSeekPowClient(deepSeekWebClient, mapper));
    }

    /**
     * Upload history context as a file using multipart/form-data.
     *
     * @param content      Full conversation history text
     * @param accountToken Bearer token for the account
     * @return Mono with the file reference ID from the upstream response
     */
    public Mono<String> uploadHistoryFile(String content, String accountToken) {
        return uploadHistoryFile(content, accountToken, null);
    }

    public Mono<String> uploadHistoryFile(String content, String accountToken, String webCookie) {
        ByteArrayResource fileResource = new ByteArrayResource(content.getBytes(StandardCharsets.UTF_8)) {
            @Override
            public String getFilename() {
                return "DS2API_HISTORY.txt";
            }
        };

        MultipartBodyBuilder builder = new MultipartBodyBuilder();
        builder.part("file", fileResource)
            .filename("DS2API_HISTORY.txt")
            .contentType(org.springframework.http.MediaType.TEXT_PLAIN);

        return powClient.getPowToken(accountToken, "/api/v0/file/upload_file",
                "https://chat.deepseek.com/", webCookie)
            .flatMap(powToken -> {
                WebClient.RequestBodySpec request = deepSeekWebClient.post()
                    .uri("/api/v0/file/upload_file")
                    .header("Authorization", "Bearer " + accountToken)
                    .header("x-ds-pow-response", powToken)
                    .header("Referer", "https://chat.deepseek.com/")
                    .contentType(org.springframework.http.MediaType.MULTIPART_FORM_DATA);
                if (webCookie != null && !webCookie.isBlank()) {
                    request.header("Cookie", webCookie);
                }
                return request
                    .body(BodyInserters.fromMultipartData(builder.build()))
                    .retrieve()
                    .bodyToMono(JsonNode.class);
            })
            .map(resp -> {
                // Flexible parsing: try nested and flat formats
                JsonNode data = resp.path("data");
                JsonNode bizData = data.path("biz_data");
                
                String ref = bizData.path("id").asText(null);
                if (ref == null) ref = bizData.path("file_id").asText(null);
                if (ref == null) ref = data.path("file_id").asText(null);
                if (ref == null) ref = data.path("id").asText(null);
                if (ref == null) ref = resp.path("file_id").asText(null);
                if (ref == null) ref = resp.path("id").asText(null);
                
                if (ref == null || ref.isBlank()) {
                    log.error("[File] Upload response missing reference ID: {}", resp);
                    throw new RuntimeException("File upload response missing reference ID");
                }
                log.info("[File] History context uploaded, ref={}", ref);
                return ref;
            })
            .subscribeOn(Schedulers.boundedElastic())
            .doOnError(e -> log.warn("[File] Upload failed: {}", e.getMessage()));
    }
}
