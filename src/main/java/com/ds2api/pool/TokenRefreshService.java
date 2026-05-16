package com.ds2api.pool;

import com.ds2api.client.DeepSeekAuthClient;
import com.ds2api.config.ConfigLoaderService;
import com.ds2api.config.Ds2Config;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Handles 401-triggered token refresh with per-account locking to prevent
 * thundering-herd refreshes, and atomically persists the new token back to
 * config.json (matching ds2api's behavior).
 *
 * Delegates the actual DeepSeek login call to {@link DeepSeekAuthClient}.
 */
@Service
public class TokenRefreshService {

    private static final Logger log = LoggerFactory.getLogger(TokenRefreshService.class);

    private final ConfigLoaderService configLoader;
    private final AccountPoolManager poolManager;
    private final DeepSeekAuthClient authClient;
    private final ObjectMapper mapper;
    private final Path configPath;
    private final ConcurrentHashMap<String, ReentrantLock> refreshLocks = new ConcurrentHashMap<>();

    public TokenRefreshService(ConfigLoaderService configLoader,
                               AccountPoolManager poolManager,
                               DeepSeekAuthClient authClient,
                               ObjectMapper mapper) {
        this.configLoader = configLoader;
        this.poolManager = poolManager;
        this.authClient = authClient;
        this.mapper = mapper;
        this.configPath = Path.of(
                System.getProperty("ds2api.config", "config.json"));
    }

    /**
     * Called when upstream returns 401.
     * Only refreshes if auto_refresh_token is enabled and the account has a password.
     * Uses a per-account ReentrantLock to avoid concurrent refreshes.
     *
     * @param accountIdentifier email or mobile of the account
     * @param upstreamError     the original 401 error
     * @return Mono with the new token, or the original error if refresh is not possible
     */
    public Mono<String> refreshIfNeeded(String accountIdentifier, Throwable upstreamError) {
        if (!isAuthError(upstreamError)) {
            return Mono.error(upstreamError);
        }

        Ds2Config config = configLoader.getConfig();
        if (!config.getRuntime().isAutoRefreshToken()) {
            log.debug("[Auth] auto_refresh_token disabled, not refreshing");
            return Mono.error(upstreamError);
        }

        ReentrantLock lock = refreshLocks.computeIfAbsent(
                accountIdentifier, k -> new ReentrantLock());

        if (!lock.tryLock()) {
            log.info("[Auth] Token refresh already in progress for {} -- waiting",
                    accountIdentifier);
            return Mono.error(new RuntimeException(
                    "Token refresh in progress for " + accountIdentifier));
        }

        log.info("[Auth] Triggering token refresh for {}", accountIdentifier);

        return Mono.fromCallable(() -> {
                    Ds2Config.Account acc = config.getAccounts().stream()
                            .filter(a -> accountIdentifier.equals(a.getEmail())
                                    || accountIdentifier.equals(a.getMobile()))
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException(
                                    "Account not found: " + accountIdentifier));

                    if (acc.getPassword() == null || acc.getPassword().isBlank()) {
                        log.warn("[Auth] No password configured for {} -- cannot refresh token",
                                accountIdentifier);
                        throw new RuntimeException(
                                "Password missing for auto-refresh: " + accountIdentifier);
                    }
                    return acc;
                })
                .flatMap(acc -> authClient.login(
                        acc.getEmail(), acc.getMobile(), acc.getPassword(),
                        acc.getAreaCode(), acc.getDeviceId(), acc.getWebCookie(),
                        acc.getDeviceProfile()))
                .flatMap(newToken -> {
                    updateInMemoryToken(accountIdentifier, newToken);
                    return persistToken(accountIdentifier, newToken);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .doOnSuccess(t -> log.info("[Auth] Token refreshed for {}", accountIdentifier))
                .doFinally(signal -> lock.unlock());
    }

    /** Persist the updated config back to config.json and return the new token. */
    private Mono<String> persistToken(String accountIdentifier, String newToken) {
        return Mono.fromRunnable(() -> {
            try {
                Ds2Config config = configLoader.getConfig();
                String json = mapper.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(config);
                Files.writeString(configPath, json,
                        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
                log.info("[Auth] Token refreshed & persisted for {}", accountIdentifier);
            } catch (Exception e) {
                log.error("[Auth] Failed to persist config: {}", e.getMessage());
                throw new RuntimeException("Failed to persist config", e);
            }
        }).thenReturn(newToken);
    }

    private void updateInMemoryToken(String accountIdentifier, String newToken) {
        Ds2Config config = configLoader.getConfig();
        config.getAccounts().stream()
                .filter(a -> accountIdentifier.equals(a.getEmail())
                        || accountIdentifier.equals(a.getMobile()))
                .findFirst()
                .ifPresent(a -> {
                    log.debug("[Auth] Updating in-memory token for {}: {}",
                            accountIdentifier,
                            a.getName() != null ? a.getName() : accountIdentifier);
                    a.setToken(newToken);
                });
    }

    private boolean isAuthError(Throwable e) {
        if (e == null) return false;
        String msg = e.getMessage();
        if (msg == null) return false;
        return msg.contains("401") || msg.contains("Unauthorized")
                || msg.contains("unauthorized");
    }
}
