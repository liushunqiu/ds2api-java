package com.ds2api.client;

import com.ds2api.config.Ds2Config;
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

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

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
    private final WebClient deviceProfileWebClient;
    private final ObjectMapper mapper;

    public DeepSeekAuthClient(WebClient.Builder webClientBuilder, ObjectMapper mapper) {
        this.mapper = mapper;
        this.authWebClient = webClientBuilder
                .baseUrl("https://chat.deepseek.com")
                .defaultHeader("Host", "chat.deepseek.com")
                .defaultHeader("Accept", "*/*")
                .defaultHeader("Accept-Language", "zh-CN,zh;q=0.9")
                .defaultHeader("Content-Type", "application/json")
                .defaultHeader("accept-charset", "UTF-8")
                .defaultHeader("Origin", "https://chat.deepseek.com")
                .defaultHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36")
                .defaultHeader("x-app-version", "2.0.0")
                .defaultHeader("x-client-locale", "zh_CN")
                .defaultHeader("x-client-platform", "web")
                .defaultHeader("x-client-timezone-offset", "28800")
                .defaultHeader("x-client-version", "2.0.0")
                .build();
        this.deviceProfileWebClient = webClientBuilder
                .baseUrl("https://fp-it-acc.portal101.cn")
                .defaultHeader("Accept", "*/*")
                .defaultHeader("Accept-Language", "zh-CN,zh;q=0.9")
                .defaultHeader("Content-Type", "application/json;charset=UTF-8")
                .defaultHeader("Origin", "https://chat.deepseek.com")
                .defaultHeader("Referer", "https://chat.deepseek.com/")
                .defaultHeader("Sec-Fetch-Dest", "empty")
                .defaultHeader("Sec-Fetch-Mode", "cors")
                .defaultHeader("Sec-Fetch-Site", "cross-site")
                .defaultHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36")
                .defaultHeader("sec-ch-ua", "\"Chromium\";v=\"148\", \"Google Chrome\";v=\"148\", \"Not/A)Brand\";v=\"99\"")
                .defaultHeader("sec-ch-ua-mobile", "?0")
                .defaultHeader("sec-ch-ua-platform", "\"macOS\"")
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
     * @param areaCode  mobile area code, defaults to "+86"
     */
    public Mono<String> login(String email, String mobile, String password, String areaCode) {
        return login(email, mobile, password, areaCode, null, null);
    }

    /**
     * Login with optional browser fingerprint context captured from DeepSeek Web.
     *
     * @param deviceId  explicit DeepSeek Web device_id, preferred when present
     * @param webCookie real browser Cookie header value, used for passthrough and
     *                  deriving device_id from .thumbcache_* when deviceId is absent
     */
    public Mono<String> login(String email, String mobile, String password, String areaCode,
                              String deviceId, String webCookie) {
        return login(email, mobile, password, areaCode, deviceId, webCookie, null);
    }

    public Mono<String> login(String email, String mobile, String password, String areaCode,
                              String deviceId, String webCookie,
                              Ds2Config.DeviceProfilePayload deviceProfilePayload) {
        ObjectNode payload = mapper.createObjectNode();
        if (email != null && !email.isBlank()) {
            payload.put("email", email);
        } else if (mobile != null && !mobile.isBlank()) {
            payload.put("mobile", mobile);
            payload.put("area_code", normalizeAreaCode(areaCode));
        } else {
            return Mono.error(new IllegalArgumentException(
                "Email or mobile must be provided for login"));
        }
        payload.put("password", password);
        payload.put("os", "web");

        log.debug("[DeepSeekAuth] Login attempt for {}",
                email != null ? email : mobile);

        return resolveDeviceId(deviceId, webCookie, deviceProfilePayload)
                .flatMap(resolvedDeviceId -> sendLogin(payload, resolvedDeviceId, webCookie));
    }

    private Mono<String> sendLogin(ObjectNode payload, String resolvedDeviceId, String webCookie) {
        payload.put("device_id", resolvedDeviceId);
        WebClient.RequestBodySpec request = authWebClient.post()
                .uri("/api/v0/users/login")
                .header("Referer", "https://chat.deepseek.com/sign_in");
        if (webCookie != null && !webCookie.isBlank()) {
            request.header("Cookie", webCookie);
        }

        return request
                .bodyValue(payload)
                .retrieve()
                .onStatus(HttpStatusCode::is4xxClientError, resp ->
                        resp.bodyToMono(String.class).flatMap(bodyStr -> {
                            log.warn("[DeepSeekAuth] Login 4xx raw response: {}", bodyStr);
                            try {
                                JsonNode body = mapper.readTree(bodyStr);
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
                            } catch (Exception e) {
                                return Mono.error(new RuntimeException(
                                        "Login failed with 4xx: " + bodyStr));
                            }
                        }))
                .bodyToMono(JsonNode.class)
                .map(this::extractToken)
                .subscribeOn(Schedulers.boundedElastic());
    }

    private String normalizeAreaCode(String areaCode) {
        if (areaCode == null || areaCode.isBlank()) {
            return "+86";
        }
        String trimmed = areaCode.trim();
        return trimmed.startsWith("+") ? trimmed : "+" + trimmed;
    }

    private Mono<String> resolveDeviceId(String explicitDeviceId, String webCookie,
                                         Ds2Config.DeviceProfilePayload deviceProfilePayload) {
        if (explicitDeviceId != null && !explicitDeviceId.isBlank()) {
            return Mono.just(explicitDeviceId.trim());
        }
        String smid = extractThumbcacheValue(webCookie);
        if (smid != null && !smid.isBlank()) {
            return Mono.just("B" + smid);
        }
        if (deviceProfilePayload != null) {
            return fetchDeviceId(deviceProfilePayload).map(deviceId -> "B" + deviceId);
        }
        return Mono.error(new IllegalArgumentException(
                "DeepSeek Web device_id is required. Provide account.device_id, "
                        + "account.web_cookie with .thumbcache_*, or account.device_profile"));
    }

    private String extractThumbcacheValue(String webCookie) {
        if (webCookie == null || webCookie.isBlank()) {
            return null;
        }
        String[] parts = webCookie.split(";");
        for (String part : parts) {
            String trimmed = part.trim();
            int equals = trimmed.indexOf('=');
            if (equals <= 0) {
                continue;
            }
            String name = trimmed.substring(0, equals).trim();
            if (name.startsWith(".thumbcache_")) {
                String value = trimmed.substring(equals + 1).trim();
                return URLDecoder.decode(value, StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private Mono<String> fetchDeviceId(Ds2Config.DeviceProfilePayload deviceProfilePayload) {
        if (deviceProfilePayload.getEp() == null || deviceProfilePayload.getEp().isBlank()
                || deviceProfilePayload.getData() == null || deviceProfilePayload.getData().isBlank()) {
            return Mono.error(new IllegalArgumentException(
                    "account.device_profile requires non-empty ep and data"));
        }
        return deviceProfileWebClient.post()
                .uri("/deviceprofile/v4")
                .bodyValue(deviceProfilePayload)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(resp -> {
                    int code = resp.path("code").asInt(-1);
                    String deviceId = resp.path("detail").path("deviceId").asText(null);
                    if (code != 1100 || deviceId == null || deviceId.isBlank()) {
                        throw new RuntimeException(
                                "Device profile failed: code=" + code + " response=" + resp);
                    }
                    return deviceId;
                });
    }

    /**
     * Flexibly extract token from login response.
     * Supports nested format: { "data": { "biz_data": { "user": { "token": "..." } } } }
     */
    private String extractToken(JsonNode resp) {
        JsonNode data = resp.path("data");
        // Check biz_code first
        int bizCode = data.path("biz_code").asInt(0);
        if (bizCode != 0) {
            String bizMsg = data.path("biz_msg").asText("Unknown error");
            log.error("[DeepSeekAuth] Login biz error: biz_code={} biz_msg={}", bizCode, bizMsg);
            throw new RuntimeException("Login failed: " + bizMsg);
        }
        // Try nested format: data.biz_data.user.token
        JsonNode bizData = data.path("biz_data");
        JsonNode user = bizData.path("user");
        String token = user.path("token").asText(null);
        if (token == null || token.isBlank()) {
            // Fallback: data.token
            token = data.path("token").asText(null);
        }
        if (token == null || token.isBlank()) {
            // Fallback: root token
            token = resp.path("token").asText(null);
        }
        if (token == null || token.isBlank()) {
            log.error("[DeepSeekAuth] Token not found in response: {}", resp);
            throw new RuntimeException("Token not found in login response");
        }
        log.debug("[DeepSeekAuth] Token extracted successfully");
        return token;
    }
}
