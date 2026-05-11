package com.ds2api.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Loads config.json at startup, watches for file changes, and hot-reloads.
 * The current config is exposed via getConfig() and is always valid
 * (falls back to defaults if the file is missing or malformed).
 *
 * Environment variable overrides:
 *   DS2API_ADMIN_KEY  - overrides config.json's admin_key field
 *   DS2API_CONFIG_PATH - external path to config.json
 */
@Service
public class ConfigLoaderService {

    private static final Logger log = LoggerFactory.getLogger(ConfigLoaderService.class);

    private final ObjectMapper mapper;
    private final AtomicReference<Ds2Config> configRef = new AtomicReference<>(new Ds2Config());

    private final Path configPath;
    private Thread watcherThread;

    public ConfigLoaderService(ObjectMapper mapper) {
        this.mapper = mapper;
        String configFile = resolveConfigPath();
        this.configPath = Paths.get(configFile).toAbsolutePath();
    }

    @PostConstruct
    public void init() {
        reload();
        startWatcher();
    }

    @PreDestroy
    public void shutdown() {
        if (watcherThread != null) {
            watcherThread.interrupt();
        }
    }

    /** Returns the current live config (never null). */
    public Ds2Config getConfig() {
        return configRef.get();
    }

    /** Force reload from disk. Returns true on success, false if defaults were used. */
    public boolean reload() {
        if (!Files.exists(configPath)) {
            log.warn("config.json not found at {}, using defaults", configPath);
            applyEnvOverrides(configRef.get());
            return false;
        }
        try {
            String content = Files.readString(configPath);
            Ds2Config cfg = mapper.readValue(content, Ds2Config.class);
            applyEnvOverrides(cfg);
            configRef.set(cfg);
            log.info("Config loaded from {} ({} keys, {} api_keys, {} accounts, {} aliases)",
                configPath,
                cfg.getKeys().size(),
                cfg.getApiKeys().size(),
                cfg.getAccounts().size(),
                cfg.getModelAliases().size());
            return true;
        } catch (IOException e) {
            log.error("Failed to load config.json: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Apply environment variable overrides after loading config.
     * DS2API_ADMIN_KEY takes precedence over config.json's admin_key.
     */
    private void applyEnvOverrides(Ds2Config cfg) {
        String adminKeyEnv = System.getenv("DS2API_ADMIN_KEY");
        if (adminKeyEnv != null && !adminKeyEnv.isBlank()) {
            cfg.setAdminKey(adminKeyEnv);
        }
    }

    private void startWatcher() {
        Path watchDir = configPath.getParent();
        if (watchDir == null) {
            watchDir = Paths.get(".");
        }
        final Path finalWatchDir = watchDir;
        if (!Files.isDirectory(finalWatchDir)) {
            log.warn("Config directory {} not found, hot-reload disabled", finalWatchDir);
            return;
        }
        String configFileName = configPath.getFileName().toString();
        watcherThread = Executors.defaultThreadFactory().newThread(() -> {
            try (WatchService ws = FileSystems.getDefault().newWatchService()) {
                finalWatchDir.register(ws,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_CREATE);
                log.info("Watching {} for config changes", finalWatchDir);
                while (!Thread.currentThread().isInterrupted()) {
                    WatchKey key = ws.take();
                    for (WatchEvent<?> event : key.pollEvents()) {
                        Path changed = (Path) event.context();
                        if (changed.toString().equals(configFileName)) {
                            log.info("config.json changed, reloading...");
                            Thread.sleep(200);
                            reload();
                        }
                    }
                    if (!key.reset()) break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                log.error("Config watcher error: {}", e.getMessage());
            }
        });
        watcherThread.setDaemon(true);
        watcherThread.setName("config-watcher");
        watcherThread.start();
    }

    private static String resolveConfigPath() {
        String sysProp = System.getProperty("ds2api.config");
        if (sysProp != null && !sysProp.isBlank()) return sysProp;
        String envVar = System.getenv("DS2API_CONFIG_PATH");
        if (envVar != null && !envVar.isBlank()) return envVar;
        return "config.json";
    }
}
