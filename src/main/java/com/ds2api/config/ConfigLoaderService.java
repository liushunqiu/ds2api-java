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
 * 启动时加载 config.json，监听文件变更并热重载。
 * 通过 getConfig() 暴露当前配置，始终有效
 * （文件缺失或格式错误时回退到默认值）。
 *
 * 环境变量覆盖：
 *   DS2API_ADMIN_KEY  - overrides config.json's admin_key field
 *   DS2API_CONFIG_PATH - config.json 的外部路径
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

    /** 返回当前有效配置（永远不会为 null）。 */
    public Ds2Config getConfig() {
        return configRef.get();
    }

    /** 从磁盘强制重新加载。成功返回 true，使用默认值时返回 false。 */
    public boolean reload() {
        if (!Files.exists(configPath)) {
            log.warn("在 {} 未找到 config.json，使用默认配置", configPath);
            applyEnvOverrides(configRef.get());
            return false;
        }
        try {
            String content = Files.readString(configPath);
            Ds2Config cfg = mapper.readValue(content, Ds2Config.class);
            applyEnvOverrides(cfg);
            configRef.set(cfg);
            log.info("已加载配置 {} ({} 个密钥, {} 个API密钥, {} 个账号, {} 个别名)",
                configPath,
                cfg.getKeys().size(),
                cfg.getApiKeys().size(),
                cfg.getAccounts().size(),
                cfg.getModelAliases().size());
            return true;
        } catch (IOException e) {
            log.error("加载 config.json 失败: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 加载配置后应用环境变量覆盖。
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
            log.warn("配置目录 {} 不存在，热重载已禁用", finalWatchDir);
            return;
        }
        String configFileName = configPath.getFileName().toString();
        watcherThread = Executors.defaultThreadFactory().newThread(() -> {
            try (WatchService ws = FileSystems.getDefault().newWatchService()) {
                finalWatchDir.register(ws,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_CREATE);
                log.info("正在监听 {} 的配置变更", finalWatchDir);
                while (!Thread.currentThread().isInterrupted()) {
                    WatchKey key = ws.take();
                    for (WatchEvent<?> event : key.pollEvents()) {
                        Path changed = (Path) event.context();
                        if (changed.toString().equals(configFileName)) {
                            log.info("config.json 已变更，正在重新加载...");
                            Thread.sleep(200);
                            reload();
                        }
                    }
                    if (!key.reset()) break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                log.error("配置文件监听出错: {}", e.getMessage());
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
