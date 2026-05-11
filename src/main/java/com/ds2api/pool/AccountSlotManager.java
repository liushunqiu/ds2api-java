package com.ds2api.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

/**
 * Per-account concurrency slot manager.
 *
 * Each account has a fixed number of in-flight slots (accountMaxInflight)
 * and a bounded wait queue. When slots are full, callers are queued.
 * When the queue is also full, callers receive an immediate 429.
 *
 * This manager is intentionally non-blocking -- no Thread.sleep or
 * BlockingQueue.take() that would pin Netty event-loop threads.
 */
public class AccountSlotManager {

    private static final Logger log = LoggerFactory.getLogger(AccountSlotManager.class);

    private final String accountIdentifier;
    private final Semaphore semaphore;
    private final int maxQueueSize;
    private final int maxInflight;
    private final Queue<MonoSink<AccountLease>> waitQueue = new ConcurrentLinkedQueue<>();

    public AccountSlotManager(String accountIdentifier, int maxInflight, int maxQueueSize) {
        this.accountIdentifier = accountIdentifier;
        this.semaphore = new Semaphore(maxInflight);
        this.maxQueueSize = maxQueueSize;
        this.maxInflight = maxInflight;
    }

    /**
     * Acquire a slot non-blockingly, or join the wait queue.
     *
     * @return Mono that completes with a lease when a slot is available,
     *         or errors with 429 if the queue is full.
     */
    public Mono<AccountLease> acquire() {
        return Mono.create(sink -> {
            if (semaphore.tryAcquire()) {
                sink.success(new AccountLease(accountIdentifier, this::release));
                return;
            }

            // Slot full -- try to queue
            if (waitQueue.size() >= maxQueueSize) {
                sink.error(new ResponseStatusException(
                    HttpStatus.TOO_MANY_REQUESTS,
                    "Account queue full: " + accountIdentifier));
                return;
            }

            waitQueue.add(sink);
            sink.onCancel(() -> {
                waitQueue.remove(sink);
                log.debug("[Pool] Cancelled wait for {}", accountIdentifier);
            });
        });
    }

    /**
     * Release a slot and wake up the next queued waiter, if any.
     * Called by the lease holder (or doFinally) when the upstream call completes.
     */
    private void release() {
        semaphore.release();
        MonoSink<AccountLease> next = waitQueue.poll();
        if (next != null && semaphore.tryAcquire()) {
            next.success(new AccountLease(accountIdentifier, this::release));
        }
    }

    // -- monitoring probes --

    public int availablePermits() {
        return semaphore.availablePermits();
    }

    public int maxInflight() {
        return maxInflight;
    }

    public int queueSize() {
        return waitQueue.size();
    }

    public String accountIdentifier() {
        return accountIdentifier;
    }
}
