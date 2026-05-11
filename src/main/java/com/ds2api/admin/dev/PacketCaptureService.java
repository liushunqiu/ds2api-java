package com.ds2api.admin.dev;

import com.ds2api.config.ConfigLoaderService;
import com.ds2api.config.Ds2Config;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * In-memory ring buffer for request/response packet capture.
 * Reads dev settings from config.json on each operation, so hot-reload
 * works without restart.
 * Autotrims to configured limit; truncates response bodies at 5 MB.
 */
@Slf4j
@Service
public class PacketCaptureService {

    private final ConfigLoaderService configLoader;
    private final long maxResponseBytes = 5 * 1024 * 1024; // 5MB truncation threshold
    private final Queue<CaptureRecord> buffer = new ConcurrentLinkedQueue<>();

    public PacketCaptureService(ConfigLoaderService configLoader) {
        this.configLoader = configLoader;
    }

    @PostConstruct
    public void init() {
        Ds2Config.DevConfig dev = configLoader.getConfig().getDev();
        log.info("[DevCapture] Enabled={}, Limit={}", dev.isPacketCapture(), dev.getPacketCaptureLimit());
    }

    /**
     * Check current enabled status (for external callers that gate capture).
     */
    public boolean isEnabled() {
        return configLoader.getConfig().getDev().isPacketCapture();
    }

    public record CaptureRecord(
            String id, String sessionId, String requestBody, String responseBody,
            boolean truncated, Instant timestamp) {
    }

    /**
     * Called by ChatRuntimeService at stream completion to persist a capture record.
     * Reads enabled/limit from live config on each call for hot-reload support.
     */
    public void push(String sessionId, String reqBody, String accumulatedResp) {
        Ds2Config.DevConfig dev = configLoader.getConfig().getDev();
        if (!dev.isPacketCapture())
            return;

        byte[] respBytes = accumulatedResp.getBytes(StandardCharsets.UTF_8);
        boolean truncated = respBytes.length > maxResponseBytes;
        String finalResp = truncated
                ? accumulatedResp.substring(0, (int) (maxResponseBytes / 4)) + "\n...[TRUNCATED]"
                : accumulatedResp;

        CaptureRecord rec = new CaptureRecord(
                UUID.randomUUID().toString().substring(0, 8),
                sessionId, reqBody, finalResp, truncated, Instant.now());
        buffer.add(rec);

        int limit = dev.getPacketCaptureLimit();
        if (limit <= 0) limit = 20;
        while (buffer.size() > limit)
            buffer.poll(); // ring eviction
    }

    public List<CaptureRecord> list() {
        List<CaptureRecord> list = new ArrayList<>(buffer);
        list.sort(Comparator.comparing(CaptureRecord::timestamp).reversed());
        return list;
    }

    public void clear() {
        buffer.clear();
    }

    public List<CaptureRecord> query(String keyword, int queryLimit) {
        return buffer.stream()
                .filter(r -> r.requestBody().contains(keyword) || r.responseBody().contains(keyword))
                .sorted(Comparator.comparing(CaptureRecord::timestamp).reversed())
                .limit(queryLimit)
                .collect(Collectors.toList());
    }

    public Optional<CaptureRecord> getById(String id) {
        return buffer.stream().filter(r -> r.id().equals(id)).findFirst();
    }
}
