package com.ds2api.auth;

import com.ds2api.config.ConfigLoaderService;
import com.ds2api.config.Ds2Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * API authentication filter.
 *
 * Priority:
 *   1. Authorization: Bearer <token>
 *   2. x-api-key header
 *   3. x-goog-api-key header
 *
 * If the key matches a managed key in config.json (keys[] / api_keys[]), the
 * request is authenticated for managed proxy mode. Otherwise, the key is treated
 * as a direct DeepSeek token and passed through.
 *
 * X-Ds2-Target-Account header: if present, the request targets a specific account
 * by email or mobile. The downstream AccountPoolManager enforces slot/queue limits.
 */
@Component
@Order(1)
public class ApiAuthFilter implements WebFilter {

    private static final Logger log = LoggerFactory.getLogger(ApiAuthFilter.class);

    private static final String ATTR_AUTH_INFO = "ds2api.authInfo";

    private final ConfigLoaderService configLoader;

    public ApiAuthFilter(ConfigLoaderService configLoader) {
        this.configLoader = configLoader;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getURI().getPath();

        // Skip auth for admin endpoints and health checks
        if (path.startsWith("/admin/") || path.equals("/health") || path.equals("/login")) {
            return chain.filter(exchange);
        }

        Ds2Config config = configLoader.getConfig();

        // Extract token from headers per priority
        String token = extractToken(exchange);
        if (token == null || token.isBlank()) {
            log.warn("No credentials in request to {}", path);
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }

        // Determine auth mode: managed key or direct token
        AuthInfo authInfo = resolveAuth(token, config);

        // X-Ds2-Target-Account: match by email, mobile, or name
        String targetAccount = exchange.getRequest().getHeaders()
                .getFirst("X-Ds2-Target-Account");
        if (targetAccount != null && !targetAccount.isBlank()) {
            boolean found = config.getAccounts().stream()
                    .anyMatch(a -> targetAccount.equals(a.getEmail())
                            || targetAccount.equals(a.getMobile())
                            || targetAccount.equals(a.getName()));
            if (!found) {
                log.warn("Target account '{}' not found", targetAccount);
                exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
                return exchange.getResponse().setComplete();
            }
            authInfo = authInfo.withTargetAccount(targetAccount);
        }

        exchange.getAttributes().put(ATTR_AUTH_INFO, authInfo);
        return chain.filter(exchange);
    }

    private String extractToken(ServerWebExchange exchange) {
        String authHeader = exchange.getRequest().getHeaders().getFirst("Authorization");
        if (authHeader != null) {
            String lower = authHeader.toLowerCase().trim();
            if (lower.startsWith("bearer ")) {
                return authHeader.substring(7).trim();
            }
        }
        String apiKey = exchange.getRequest().getHeaders().getFirst("x-api-key");
        if (apiKey != null && !apiKey.isBlank()) {
            return apiKey.trim();
        }
        String googKey = exchange.getRequest().getHeaders().getFirst("x-goog-api-key");
        if (googKey != null && !googKey.isBlank()) {
            return googKey.trim();
        }
        return null;
    }

    private AuthInfo resolveAuth(String token, Ds2Config config) {
        for (Ds2Config.ApiKey k : config.getKeys()) {
            if (token.equals(k.getKey())) {
                return AuthInfo.managed(k.getName() != null ? k.getName() : k.getRemark());
            }
        }
        for (Ds2Config.ApiKey k : config.getApiKeys()) {
            if (token.equals(k.getKey())) {
                return AuthInfo.managed(k.getName() != null ? k.getName() : k.getRemark());
            }
        }
        return AuthInfo.direct(token);
    }

    /** Retrieve auth info set by this filter from the exchange attributes. */
    public static AuthInfo getAuthInfo(ServerWebExchange exchange) {
        return exchange.getAttribute(ATTR_AUTH_INFO);
    }
}
