package com.ds2api.controller;

import com.ds2api.admin.dto.QueueStatusResponse;
import com.ds2api.auth.JwtUtil;
import com.ds2api.config.ConfigLoaderService;
import com.ds2api.config.ConfigService;
import com.ds2api.pool.AccountPoolManager;
import com.ds2api.pool.AccountSlotManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Admin endpoints for JWT login, config management, and pool monitoring.
 *
 * Admin authentication: the AdminAuthFilter (registered on /admin/**)
 * validates a JWT Bearer token on all admin endpoints except /admin/login.
 * For direct API access without JWT, an X-Admin-Key header can be used on
 * the queue/status endpoint as a lightweight alternative.
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private static final Logger log = LoggerFactory.getLogger(AdminController.class);

    private final ConfigLoaderService configLoader;
    private final ConfigService configService;
    private final AccountPoolManager poolManager;
    private final ObjectMapper mapper;

    public AdminController(ConfigLoaderService configLoader,
                           ConfigService configService,
                           AccountPoolManager poolManager,
                           ObjectMapper mapper) {
        this.configLoader = configLoader;
        this.configService = configService;
        this.poolManager = poolManager;
        this.mapper = mapper;
    }

    /**
     * POST /admin/login
     * Authenticate with admin_key and receive a JWT.
     */
    @PostMapping("/login")
    public Mono<ResponseEntity<ObjectNode>> login(@RequestBody JsonNode body) {
        String providedKey = body.path("admin_key").asText(null);
        if (providedKey == null || providedKey.isBlank()) {
            return error(401, "admin_key is required");
        }

        String adminKey = configLoader.getConfig().getAdminKey();
        if (adminKey == null || adminKey.isBlank()) {
            return error(403, "DS2API_ADMIN_KEY not configured");
        }
        if (!adminKey.equals(providedKey)) {
            log.warn("Admin login failed: invalid admin_key");
            return error(401, "Invalid admin_key");
        }

        String token = JwtUtil.createJWT(adminKey, 24);
        log.info("Admin JWT issued (expires in 24h)");

        ObjectNode response = mapper.createObjectNode();
        response.put("token", token);
        response.put("token_type", "Bearer");
        response.put("expires_in", 86400);
        return Mono.just(ResponseEntity.ok(response));
    }

    /**
     * POST /admin/reload-config
     * Hot-reload config.json without restart.
     */
    @PostMapping("/reload-config")
    public Mono<ResponseEntity<ObjectNode>> reloadConfig() {
        boolean ok = configLoader.reload();
        ObjectNode response = mapper.createObjectNode();
        response.put("success", ok);
        response.put("message", ok ? "Config reloaded"
                : "Reload failed, using previous config");
        return Mono.just(ResponseEntity.ok(response));
    }

    /**
     * GET /admin/config
     * Return the current effective config (keys/accounts masked).
     */
    @GetMapping("/config")
    public Mono<ResponseEntity<ObjectNode>> getConfig() {
        try {
            String json = mapper.writeValueAsString(configLoader.getConfig());
            JsonNode node = mapper.readTree(json);
            return Mono.just(ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body((ObjectNode) node));
        } catch (Exception e) {
            return error(500, "Failed to serialize config: " + e.getMessage());
        }
    }

    /**
     * POST /admin/config
     * Hot-reload config with partial JSON merge. Only the fields present in the
     * request body are updated; all other fields retain their current values.
     *
     * Accepts snake_case keys matching config.json (e.g. "account_max_inflight").
     * Returns the full updated config on success.
     * Returns 422 with the validation error message on invalid payload.
     */
    @PostMapping("/config")
    public Mono<ResponseEntity<ObjectNode>> updateConfig(@RequestBody JsonNode payload) {
        return configService.hotReload(payload)
            .flatMap(updated -> {
                try {
                    String json = mapper.writeValueAsString(updated);
                    JsonNode node = mapper.readTree(json);
                    return Mono.just(ResponseEntity.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .body((ObjectNode) node));
                } catch (Exception e) {
                    return error(500, "Failed to serialize config: " + e.getMessage());
                }
            })
            .onErrorMap(IllegalArgumentException.class, e ->
                new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, e.getMessage()));
    }

    /**
     * GET /admin/queue/status
     * Real-time account pool status: per-account available slots, queue depth,
     * in-flight count, plus global metrics like max inflight and 429 threshold.
     *
     * Supports optional X-Admin-Key header as a lightweight alternative to JWT
     * for monitoring systems (Prometheus, Grafana, etc.).
     */
    @GetMapping("/queue/status")
    public Mono<QueueStatusResponse> queueStatus(
            @RequestHeader(value = "X-Admin-Key", required = false) String adminKeyHeader) {
        // If X-Admin-Key header is present, validate it.
        // If absent, the AdminAuthFilter (JWT) should already have authenticated.
        if (adminKeyHeader != null && !adminKeyHeader.isBlank()) {
            validateAdminKey(adminKeyHeader);
        }

        return Mono.fromCallable(() -> {
            Map<String, AccountSlotManager> slotMgrs = poolManager.getSlotManagers();
            int maxInflight = configLoader.getConfig().getRuntime().getAccountMaxInflight();
            int maxQueue = configLoader.getConfig().getRuntime().getAccountMaxQueue();
            if (maxQueue <= 0) {
                maxQueue = maxInflight;
            }

            QueueStatusResponse resp = new QueueStatusResponse();
            resp.setTotalAccounts(slotMgrs.size());
            resp.setMaxInflightPerAccount(maxInflight);
            resp.setMaxQueuePerAccount(maxQueue);
            // ds2api experience: global 429 threshold = account_count * 4
            resp.setGlobal429Threshold(slotMgrs.size() * 4);

            Map<String, QueueStatusResponse.AccountSlotStatus> accounts =
                    new ConcurrentHashMap<>();
            slotMgrs.forEach((id, mgr) -> {
                QueueStatusResponse.AccountSlotStatus slot =
                        new QueueStatusResponse.AccountSlotStatus();
                int avail = mgr.availablePermits();
                slot.setAvailableSlots(avail);
                slot.setQueuedRequests(mgr.queueSize());
                slot.setInFlight(mgr.maxInflight() - avail);
                accounts.put(id, slot);
            });
            resp.setAccounts(accounts);
            return resp;
        });
    }

    private void validateAdminKey(String providedKey) {
        String adminKey = configLoader.getConfig().getAdminKey();
        if (adminKey == null || adminKey.isBlank()) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN,
                    "DS2API_ADMIN_KEY not configured");
        }
        if (!adminKey.equals(providedKey)) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED,
                    "Invalid admin key");
        }
    }

    private Mono<ResponseEntity<ObjectNode>> error(int status, String message) {
        ObjectNode err = mapper.createObjectNode();
        err.put("detail", message);
        return Mono.just(ResponseEntity.status(status).body(err));
    }
}
