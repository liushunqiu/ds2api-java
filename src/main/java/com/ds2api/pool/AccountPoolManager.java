package com.ds2api.pool;

import com.ds2api.client.DeepSeekAuthClient;
import com.ds2api.config.ConfigLoaderService;
import com.ds2api.config.ConfigReloadedEvent;
import com.ds2api.config.Ds2Config;
import org.springframework.context.event.EventListener;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages the account pool: creates per-account slot managers,
 * provides round-robin and targeted account routing.
 *
 * Aligned with ds2api Go reference:
 * - Round-robin dispatch when no X-Ds2-Target-Account header
 * - Targeted routing when header is present
 * - 429 when target account not found or queue is full
 */
@Service
public class AccountPoolManager {

    private static final Logger log = LoggerFactory.getLogger(AccountPoolManager.class);

    private final ConfigLoaderService configLoader;
    private final DeepSeekAuthClient authClient;
    private final Map<String, AccountSlotManager> slotManagers = new ConcurrentHashMap<>();
    private final List<String> accountIds = new CopyOnWriteArrayList<>();
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);

    public AccountPoolManager(ConfigLoaderService configLoader, DeepSeekAuthClient authClient) {
        this.configLoader = configLoader;
        this.authClient = authClient;
    }

    @PostConstruct
    public void init() {
        rebuildPool(configLoader.getConfig());
    }

    /**
     * Rebuild the account pool from an updated config without restart.
     * Old AccountSlotManager references in pending acquire() chains still
     * function for their existing leases; new requests use the new managers.
     */
    @EventListener
    public void onConfigReloaded(ConfigReloadedEvent event) {
        log.info("[Pool] Rebuilding account slots due to config hot-reload...");
        rebuildPool(event.getUpdatedConfig());
    }

    private void rebuildPool(Ds2Config config) {
        Ds2Config.RuntimeConfig rt = config.getRuntime();
        int maxInflight = rt.getAccountMaxInflight();
        int maxQueue = rt.getAccountMaxQueue();
        // ds2api semantics: 0 means "same as inflight"
        if (maxQueue <= 0) {
            maxQueue = maxInflight;
        }

        Map<String, AccountSlotManager> newManagers = new ConcurrentHashMap<>();
        List<String> newIds = new CopyOnWriteArrayList<>();

        for (Ds2Config.Account acc : config.getAccounts()) {
            String id = resolveIdentifier(acc);
            if (id == null) {
                log.warn("[Pool] Skipping account with no email/mobile");
                continue;
            }
            newManagers.put(id, new AccountSlotManager(id, maxInflight, maxQueue));
            newIds.add(id);
        }

        // Atomic replacement: clear and repopulate
        slotManagers.clear();
        slotManagers.putAll(newManagers);
        accountIds.clear();
        accountIds.addAll(newIds);
        roundRobinIndex.set(0);

        log.info("[Pool] Rebuilt {} accounts. inflight={}, queue={}",
                newIds.size(), maxInflight, maxQueue);
    }

    /**
     * Acquire a slot lease for an account.
     *
     * @param targetAccount Optional account identifier from X-Ds2-Target-Account header.
     *                      If null or empty, round-robin is used.
     * @return Mono<AccountLease> that completes when a slot is available.
     * @throws ResponseStatusException(429) if target account not found or queue is full.
     */
    public Mono<AccountLease> acquire(String targetAccount) {
        if (accountIds.isEmpty()) {
            return Mono.error(new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE, "No accounts configured"));
        }

        String selectedId;
        if (targetAccount != null && !targetAccount.isBlank()) {
            selectedId = targetAccount;
            AccountSlotManager mgr = slotManagers.get(selectedId);
            if (mgr == null) {
                return Mono.error(new ResponseStatusException(
                        HttpStatus.TOO_MANY_REQUESTS,
                        "Target account not found: " + selectedId));
            }
            return mgr.acquire();
        }

        // Round-robin with retry: try each account until one gives a slot or queue slot
        selectedId = nextRoundRobin();
        AccountSlotManager firstMgr = slotManagers.get(selectedId);
        if (firstMgr == null) {
            return Mono.error(new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE, "No valid account slot managers"));
        }
        return firstMgr.acquire()
                .onErrorResume(ResponseStatusException.class, e -> {
                    if (e.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS) {
                        // Try next account in round-robin
                        String nextId = nextRoundRobin();
                        AccountSlotManager nextMgr = slotManagers.get(nextId);
                        if (nextMgr != null && !nextId.equals(selectedId)) {
                            return nextMgr.acquire();
                        }
                    }
                    return Mono.error(e);
                });
    }

    /**
     * Resolve the token for a given account identifier.
     * If token is missing, attempts auto-login asynchronously.
     */
    public Mono<String> resolveToken(String accountId) {
        Ds2Config config = configLoader.getConfig();
        return config.getAccounts().stream()
                .filter(a -> accountId.equals(resolveIdentifier(a)))
                .findFirst()
                .map(acc -> {
                    if (acc.getToken() == null || acc.getToken().isBlank()) {
                        log.info("[Pool] Token missing for {}, attempting login...", accountId);
                        return authClient.login(acc.getEmail(), acc.getMobile(), acc.getPassword(),
                                        acc.getAreaCode(), acc.getDeviceId(), acc.getWebCookie(),
                                        acc.getDeviceProfile())
                                .doOnNext(token -> {
                                    acc.setToken(token);
                                    log.info("[Pool] Login successful for {}", accountId);
                                })
                                .onErrorResume(e -> {
                                    log.error("[Pool] Login failed for {}: {}", accountId, e.getMessage());
                                    return Mono.error(new IllegalStateException(
                                            "Login failed for " + accountId + ": " + e.getMessage()));
                                });
                    }
                    log.info("[Pool] Using cached token for {}", accountId);
                    return Mono.just(acc.getToken());
                })
                .orElse(Mono.empty());
    }

    /**
     * Get a slot manager by account identifier (for monitoring).
     */
    public AccountSlotManager getSlotManager(String accountId) {
        return slotManagers.get(accountId);
    }

    /** Returns the full map of slot managers for bulk monitoring. */
    public Map<String, AccountSlotManager> getSlotManagers() {
        return slotManagers;
    }

    private String nextRoundRobin() {
        int idx = Math.abs(roundRobinIndex.getAndIncrement() % accountIds.size());
        return accountIds.get(idx);
    }

    private static String resolveIdentifier(Ds2Config.Account acc) {
        if (acc.getEmail() != null && !acc.getEmail().isBlank()) {
            return acc.getEmail();
        }
        if (acc.getMobile() != null && !acc.getMobile().isBlank()) {
            return acc.getMobile();
        }
        return null;
    }

    /** Monitoring: snapshot of all account slot/queue states. */
    public Map<String, Map<String, Integer>> queueStatus() {
        Map<String, Map<String, Integer>> status = new ConcurrentHashMap<>();
        slotManagers.forEach((id, mgr) ->
                status.put(id, Map.of(
                        "available", mgr.availablePermits(),
                        "queued", mgr.queueSize())));
        return status;
    }
}
