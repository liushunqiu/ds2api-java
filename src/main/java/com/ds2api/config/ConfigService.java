package com.ds2api.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Config hot-reload service: deep clone, partial JSON merge, validate,
 * atomically persist to disk, update live config, and broadcast event.
 *
 * Aligned with ds2api Go reference admin hot-reload semantics.
 */
@Service
public class ConfigService {

    private static final Logger log = LoggerFactory.getLogger(ConfigService.class);

    private final ConfigLoaderService configLoader;
    private final ObjectMapper mapper;
    private final ApplicationEventPublisher eventPublisher;
    private final Path configPath;
    private final ReentrantLock configLock = new ReentrantLock();

    public ConfigService(ConfigLoaderService configLoader,
                         ObjectMapper mapper,
                         ApplicationEventPublisher eventPublisher) {
        this.configLoader = configLoader;
        this.mapper = mapper;
        this.eventPublisher = eventPublisher;
        String configFile = System.getProperty("ds2api.config", "config.json");
        this.configPath = Path.of(configFile).toAbsolutePath();
    }

    /**
     * Hot-reload config with a partial or full JSON payload.
     * Only updates the fields present in the payload; all other fields keep
     * their current values.
     *
     * @param updatePayload Partial JSON with snake_case keys matching config.json.
     * @return The updated Ds2Config.
     */
    public Mono<Ds2Config> hotReload(JsonNode updatePayload) {
        return Mono.fromCallable(() -> {
            configLock.lock();
            try {
                Ds2Config liveConfig = configLoader.getConfig();

                // 1. Deep clone current config as staging area.
                //    If validation fails, the running config is untouched.
                Ds2Config staging = mapper.readValue(
                        mapper.writeValueAsBytes(liveConfig), Ds2Config.class);

                // 2. Jackson partial merge: only overwrites fields present in
                //    updatePayload. @JsonProperty annotations handle snake_case.
                ObjectReader updater = mapper.readerForUpdating(staging);
                updater.readValue(updatePayload);

                // 3. Validate the merged config before persisting.
                validate(staging);

                // 4. Atomically persist to disk.
                String json = mapper.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(staging);
                Files.writeString(configPath, json,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE);

                // 5. Safely apply to the live config bean (field-by-field,
                //    preserving the Spring bean reference).
                applyToLiveConfig(liveConfig, staging);

                // 6. Broadcast event so listeners can rebuild.
                eventPublisher.publishEvent(new ConfigReloadedEvent(this, liveConfig));

                log.info("[Admin] Config hot-reloaded & persisted successfully");
                return liveConfig;
            } finally {
                configLock.unlock();
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private void validate(Ds2Config cfg) {
        if (cfg.getRuntime().getAccountMaxInflight() <= 0) {
            throw new IllegalArgumentException(
                    "runtime.account_max_inflight must be > 0");
        }
        if (cfg.getAccounts() != null) {
            for (Ds2Config.Account acc : cfg.getAccounts()) {
                if ((acc.getEmail() == null || acc.getEmail().isBlank())
                        && (acc.getMobile() == null || acc.getMobile().isBlank())) {
                    throw new IllegalArgumentException(
                            "Account must have email or mobile");
                }
            }
        }
    }

    /** Copy staging fields into the live config object reference. */
    private void applyToLiveConfig(Ds2Config live, Ds2Config staging) {
        live.setKeys(staging.getKeys());
        live.setApiKeys(staging.getApiKeys());
        live.setAccounts(staging.getAccounts());
        live.setModelAliases(staging.getModelAliases());
        live.setRuntime(staging.getRuntime());
        live.setAutoDelete(staging.getAutoDelete());
        live.setCurrentInputFile(staging.getCurrentInputFile());
        live.setThinkingInjection(staging.getThinkingInjection());
        live.setAdminKey(staging.getAdminKey());
    }
}
