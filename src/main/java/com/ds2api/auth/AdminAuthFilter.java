package com.ds2api.auth;

import com.ds2api.config.ConfigLoaderService;
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
 * Admin authentication filter for /admin/* endpoints.
 * Validates JWT tokens signed with the admin_key from config.json.
 */
@Component
@Order(0) // Runs before ApiAuthFilter
public class AdminAuthFilter implements WebFilter {

    private static final Logger log = LoggerFactory.getLogger(AdminAuthFilter.class);

    private final ConfigLoaderService configLoader;

    public AdminAuthFilter(ConfigLoaderService configLoader) {
        this.configLoader = configLoader;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getURI().getPath();

        if (!path.startsWith("/admin/")) {
            return chain.filter(exchange);
        }

        // /admin/login is unprotected
        if (path.equals("/admin/login")) {
            return chain.filter(exchange);
        }

        String authHeader = exchange.getRequest().getHeaders().getFirst("Authorization");
        if (authHeader == null || !authHeader.toLowerCase().startsWith("bearer ")) {
            return unauthorized(exchange, "No credentials provided");
        }

        String token = authHeader.substring(7).trim();
        String adminKey = configLoader.getConfig().getAdminKey();

        try {
            JwtUtil.verifyJWT(token, adminKey);
        } catch (JwtUtil.JwtException e) {
            log.warn("Admin auth failed: {}", e.getMessage());
            return unauthorized(exchange, e.getMessage());
        }

        return chain.filter(exchange);
    }

    private Mono<Void> unauthorized(ServerWebExchange exchange, String detail) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        exchange.getResponse().getHeaders().set("Content-Type", "application/json; charset=utf-8");
        byte[] body = ("{\"detail\":\"" + detail + "\"}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return exchange.getResponse()
            .writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(body)));
    }
}
