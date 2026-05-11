package com.ds2api.pool;

/**
 * Lightweight lease record tied to one account slot.
 * Call {@link #release()} to return the slot to the pool.
 */
public record AccountLease(String accountIdentifier, Runnable releaseCallback) {

    /** Return the slot to the account pool. Idempotent via the manager. */
    public void release() {
        if (releaseCallback != null) {
            releaseCallback.run();
        }
    }
}
