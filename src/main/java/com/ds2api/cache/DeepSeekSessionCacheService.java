package com.ds2api.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Caches DeepSeek native session info for multi-turn conversation support.
 * Stores chat_session_id and the last response_message_id for continuation.
 *
 * Key design:
 * - conversationId: external identifier (from client or auto-generated)
 * - Value: SessionInfo containing chat_session_id and response_message_id
 * - TTL: 30 minutes by default
 */
@Service
public class DeepSeekSessionCacheService {

    private static final Logger log = LoggerFactory.getLogger(DeepSeekSessionCacheService.class);

    private static final long DEFAULT_TTL_MINUTES = 30;

    private final ConcurrentHashMap<String, SessionEntry> cache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "session-cache-evictor");
        t.setDaemon(true);
        return t;
    });

    public DeepSeekSessionCacheService() {
        // Run cleanup every 5 minutes
        scheduler.scheduleAtFixedRate(this::evictExpired, 5, 5, TimeUnit.MINUTES);
    }

    /**
     * Session information for DeepSeek native multi-turn conversation.
     */
    public record SessionInfo(
        String chatSessionId,
        Integer lastResponseMessageId,
        String accountIdentifier
    ) {}

    private record SessionEntry(SessionInfo info, long createdAtMillis) {}

    /**
     * Get session info for a conversation.
     * @param conversationId external conversation identifier
     * @return session info if exists and not expired
     */
    public SessionInfo get(String conversationId) {
        if (conversationId == null) return null;
        SessionEntry entry = cache.get(conversationId);
        if (entry == null) return null;

        // Check if expired
        if (isExpired(entry)) {
            cache.remove(conversationId);
            log.debug("[SessionCache] Expired entry for conversation: {}", conversationId);
            return null;
        }

        return entry.info();
    }

    /**
     * Store or update session info for a conversation.
     * @param conversationId external conversation identifier
     * @param chatSessionId DeepSeek chat_session_id
     * @param responseMessageId last response_message_id from SSE stream
     * @param accountIdentifier the account that owns this session (email or mobile)
     */
    public void put(String conversationId, String chatSessionId, Integer responseMessageId, String accountIdentifier) {
        if (conversationId == null || chatSessionId == null) return;

        SessionInfo info = new SessionInfo(chatSessionId, responseMessageId, accountIdentifier);
        SessionEntry entry = new SessionEntry(info, System.currentTimeMillis());
        cache.put(conversationId, entry);

        log.debug("[SessionCache] Stored session for conversation={}, chatSessionId={}, responseMessageId={}, account={}",
                conversationId, chatSessionId, responseMessageId, accountIdentifier);
    }

    /**
     * Update only the response_message_id for an existing conversation.
     * @param conversationId external conversation identifier
     * @param responseMessageId new response_message_id from SSE stream
     */
    public void updateResponseMessageId(String conversationId, Integer responseMessageId) {
        if (conversationId == null) return;

        SessionEntry existing = cache.get(conversationId);
        if (existing == null) {
            log.debug("[SessionCache] Cannot update non-existent conversation: {}", conversationId);
            return;
        }

        SessionInfo updatedInfo = new SessionInfo(
                existing.info().chatSessionId(),
                responseMessageId,
                existing.info().accountIdentifier()
        );
        SessionEntry updatedEntry = new SessionEntry(updatedInfo, existing.createdAtMillis());
        cache.put(conversationId, updatedEntry);

        log.debug("[SessionCache] Updated responseMessageId={} for conversation={}",
                responseMessageId, conversationId);
    }

    /**
     * Remove session info for a conversation.
     * @param conversationId external conversation identifier
     */
    public void remove(String conversationId) {
        if (conversationId != null) {
            cache.remove(conversationId);
            log.debug("[SessionCache] Removed session for conversation: {}", conversationId);
        }
    }

    /**
     * Check if a conversation has cached session info.
     * @param conversationId external conversation identifier
     * @return true if exists and not expired
     */
    public boolean contains(String conversationId) {
        return get(conversationId) != null;
    }

    private boolean isExpired(SessionEntry entry) {
        long elapsed = System.currentTimeMillis() - entry.createdAtMillis();
        return elapsed > TimeUnit.MINUTES.toMillis(DEFAULT_TTL_MINUTES);
    }

    private void evictExpired() {
        int before = cache.size();
        cache.entrySet().removeIf(entry -> isExpired(entry.getValue()));
        int after = cache.size();
        if (before != after) {
            log.debug("[SessionCache] Evicted {} expired entries", before - after);
        }
    }
}
