package com.ds2api.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Dedicated DeepSeek authentication client.
 * Uses an independent WebClient (no PoW filter, since the login endpoint
 * has its own auth flow) to perform email/mobile + password login.
 *
 * Captcha/verification detection: when the response code is 40002 or the
 * message contains "captcha"/"verify", the error is surfaced clearly so
 * operators can manually update the token.
 */
@Service
public class DeepSeekAuthClient {

    private static final Logger log = LoggerFactory.getLogger(DeepSeekAuthClient.class);

    private final WebClient authWebClient;
    private final ObjectMapper mapper;

    public DeepSeekAuthClient(WebClient.Builder webClientBuilder, ObjectMapper mapper) {
        this.mapper = mapper;
        this.authWebClient = webClientBuilder
                .baseUrl("https://chat.deepseek.com")
                .build();
    }

    /**
     * Login with email or mobile + password.
     * Returns a Mono that completes with the fresh DeepSeek token,
     * or errors with a descriptive message on failure / captcha / verification.
     *
     * @param email     account email (may be null if mobile is used)
     * @param mobile    account mobile (may be null if email is used)
     * @param password  account password (required)
     * @param areaCode  mobile area code, defaults to "86"
     */
    public Mono<String> login(String email, String mobile, String password, String areaCode) {
        ObjectNode payload = mapper.createObjectNode();
        if (email != null && !email.isBlank()) {
            payload.put("email", email);
        } else if (mobile != null && !mobile.isBlank()) {
            payload.put("mobile", mobile);
            payload.put("area_code", areaCode != null ? areaCode : "86");
        } else {
            return Mono.error(new IllegalArgumentException(
                    "Email or mobile must be provided for login"));
        }
        payload.put("password", password);

        log.debug("[DeepSeekAuth] Login attempt for {}",
                email != null ? email : mobile);

        return authWebClient.post()
                .uri("/api/v1/users/login")
                .header("Content-Type", "application/json")
                .header("User-Agent",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
                .bodyValue(payload)
                .retrieve()
                .onStatus(HttpStatusCode::is4xxClientError, resp ->
                        resp.bodyToMono(JsonNode.class).flatMap(body -> {
                            String msg = body.path("msg").asText("Unknown auth error");
                            int code = body.path("code").asInt(-1);
                            log.warn("[DeepSeekAuth] Login 4xx: code={} msg={}", code, msg);
                            if (code == 40002 || msg.contains("captcha") || msg.contains("verify")) {
                                return Mono.error(new RuntimeException(
                                        "DeepSeek requires captcha/verification. Manual login needed. "
                                                + "(code:" + code + " msg:" + msg + ")"));
                            }
                            return Mono.error(new RuntimeException(
                                    "Login failed: " + msg + " (code:" + code + ")"));
                        }))
                .bodyToMono(JsonNode.class)
                .map(this::extractToken)
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Flexibly extract token from login response.
     * Supports: { "data": { "token": "..." } } and { "token": "..." }
     */
    private String extractToken(JsonNode resp) {
        JsonNode data = resp.path("data");
        String token = data.isMissingNode()
                ? resp.path("token").asText(null)
                : data.path("token").asText(null);
        if (token == null || token.isBlank()) {
            log.error("[DeepSeekAuth] Token not found in response: {}", resp);
            throw new RuntimeException("Token not found in login response");
        }
        log.debug("[DeepSeekAuth] Token extracted successfully");
        return token;
    }
}
