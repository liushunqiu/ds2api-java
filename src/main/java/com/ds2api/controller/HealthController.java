package com.ds2api.controller;

import com.ds2api.config.ConfigLoaderService;
import com.ds2api.config.Ds2Config;
import com.ds2api.pool.AccountPoolManager;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Health and readiness probes for Docker/K8s orchestration.
 */
@RestController
@RequiredArgsConstructor
public class HealthController {

    private final ConfigLoaderService configLoader;
    private final AccountPoolManager poolManager;

    @GetMapping("/healthz")
    public Mono<ResponseEntity<String>> healthz() {
        return Mono.just(ResponseEntity.ok("OK"));
    }

    @GetMapping("/readyz")
    public Mono<ResponseEntity<String>> readyz() {
        return Mono.fromSupplier(() -> {
            Ds2Config config = configLoader.getConfig();
            boolean configLoaded = config.getAccounts() != null && !config.getAccounts().isEmpty();
            boolean poolReady = poolManager.queueStatus() != null && !poolManager.queueStatus().isEmpty();

            if (configLoaded && poolReady) {
                return ResponseEntity.ok("Ready");
            }
            return ResponseEntity.status(503).body("Not Ready: Config or Pool uninitialized");
        });
    }
}
