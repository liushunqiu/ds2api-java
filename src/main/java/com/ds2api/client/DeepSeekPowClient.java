package com.ds2api.client;

import com.ds2api.pow.DeepSeekHashV1Solver;
import com.ds2api.pow.PowException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * Proactive PoW client that fetches a PoW challenge from DeepSeek
 * and solves it before sending the completion request.
 *
 * Aligned with ds2api Go reference: internal/deepseek/client/client_auth.go GetPow().
 */
@Service
public class DeepSeekPowClient {

    private static final Logger log = LoggerFactory.getLogger(DeepSeekPowClient.class);
    private static final String COMPLETION_TARGET_PATH = "/api/v0/chat/completion";

    private final WebClient deepSeekWebClient;
    private final ObjectMapper mapper;
    private final DeepSeekHashV1Solver solver;

    public DeepSeekPowClient(WebClient deepSeekWebClient, ObjectMapper mapper) {
        this.deepSeekWebClient = deepSeekWebClient;
        this.mapper = mapper;
        this.solver = new DeepSeekHashV1Solver();
    }

    /**
     * Get PoW token for the completion endpoint.
     *
     * @param accountToken Bearer token for the account
     * @return the x-ds-pow-response header value (base64 encoded)
     */
    public Mono<String> getPowToken(String accountToken) {
        return getPowToken(accountToken, COMPLETION_TARGET_PATH);
    }

    /**
     * Get PoW token for a specific target path.
     *
     * @param accountToken Bearer token
     * @param targetPath   the API path to get PoW for
     * @return the x-ds-pow-response header value
     */
    public Mono<String> getPowToken(String accountToken, String targetPath) {
        return getPowToken(accountToken, targetPath, defaultReferer(targetPath), null);
    }

    /**
     * Get PoW token with Web-aligned Referer and optional browser Cookie.
     */
    public Mono<String> getPowToken(String accountToken, String targetPath,
                                    String referer, String webCookie) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("target_path", targetPath);

        WebClient.RequestBodySpec request = deepSeekWebClient.post()
            .uri("/api/v0/chat/create_pow_challenge")
            .header("Authorization", "Bearer " + accountToken)
            .header("Referer", referer != null && !referer.isBlank()
                ? referer : defaultReferer(targetPath));
        if (webCookie != null && !webCookie.isBlank()) {
            request.header("Cookie", webCookie);
        }

        return request
            .bodyValue(payload)
            .retrieve()
            .bodyToMono(String.class)
            .flatMap(body -> {
                try {
                    JsonNode root = mapper.readTree(body);
                    int code = root.path("code").asInt(-1);
                    int bizCode = root.path("data").path("biz_code").asInt(-1);

                    if (code != 0 || bizCode != 0) {
                        String msg = root.path("msg").asText("unknown");
                        String bizMsg = root.path("data").path("biz_msg").asText("");
                        log.warn("[PoW] Challenge request failed: code={}, biz_code={}, msg={}, biz_msg={}",
                            code, bizCode, msg, bizMsg);
                        return Mono.error(new PowException("PoW challenge failed: " + msg + " " + bizMsg));
                    }

                    JsonNode challenge = root.path("data").path("biz_data").path("challenge");
                    String algorithm = challenge.path("algorithm").asText("");
                    String challengeStr = challenge.path("challenge").asText("");
                    String salt = challenge.path("salt").asText("");
                    long expireAt = challenge.path("expire_at").asLong(0);
                    long difficulty = challenge.path("difficulty").asLong(144000);
                    String signature = challenge.path("signature").asText("");
                    String respTargetPath = challenge.path("target_path").asText(targetPath);

                    if (!"DeepSeekHashV1".equals(algorithm)) {
                        return Mono.error(new PowException("Unsupported PoW algorithm: " + algorithm));
                    }

                    log.debug("[PoW] Challenge received: algorithm={}, difficulty={}, salt={}",
                        algorithm, difficulty, salt);

                    long start = System.currentTimeMillis();
                    long answer = solver.solvePow(challengeStr, salt, expireAt, difficulty);
                    long elapsed = System.currentTimeMillis() - start;

                    String powHeader = solver.buildPowHeader(
                        algorithm, challengeStr, salt, answer, signature, respTargetPath);

                    log.info("[PoW] Solved in {}ms (nonce={}, difficulty={})", elapsed, answer, difficulty);
                    return Mono.just(powHeader);
                } catch (PowException e) {
                    return Mono.error(e);
                } catch (Exception e) {
                    log.error("[PoW] Failed to parse/solve challenge", e);
                    return Mono.error(new PowException("PoW processing failed: " + e.getMessage()));
                }
            })
            .onErrorResume(e -> {
                if (e instanceof PowException) {
                    return Mono.error(e);
                }
                log.error("[PoW] Request failed: {}", e.getMessage());
                return Mono.error(new PowException("PoW request failed: " + e.getMessage()));
            });
    }

    private String defaultReferer(String targetPath) {
        if ("/api/v0/file/upload_file".equals(targetPath)) {
            return "https://chat.deepseek.com/";
        }
        return "https://chat.deepseek.com/";
    }
}
