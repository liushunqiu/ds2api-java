package com.ds2api.admin.dev;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * In-memory ring buffer for request/response packet capture.
 * Controlled via DS2API_DEV_PACKET_CAPTURE env var.
 * Autotrims to configured limit; truncates response bodies at 5 MB.
 */
@Slf4j
@Service
public class PacketCaptureService {

    private volatile boolean enabled = false;
    private int limit = 20;
    private final long maxResponseBytes = 5 * 1024 * 1024; // 5MB truncation threshold
    private final Queue<CaptureRecord> buffer = new ConcurrentLinkedQueue<>();

    @EventListener(ApplicationStartedEvent.class)
    public void init() {
        this.enabled = Boolean.parseBoolean(System.getenv("DS2API_DEV_PACKET_CAPTURE"));
        String limitEnv = System.getenv("DS2API_DEV_PACKET_CAPTURE_LIMIT");
        if (limitEnv != null) {
            try {
                this.limit = Integer.parseInt(limitEnv);
            } catch (NumberFormatException ignored) {
            }
        }
        log.info("[DevCapture] Enabled={}, Limit={}", enabled, limit);
    }

    public record CaptureRecord(
            String id, String sessionId, String requestBody, String responseBody,
            boolean truncated, Instant timestamp) {
    }

    /**
     * Called by ChatRuntimeService at stream completion to persist a capture record.
     */
    public void push(String sessionId, String reqBody, String accumulatedResp) {
        if (!enabled)
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
