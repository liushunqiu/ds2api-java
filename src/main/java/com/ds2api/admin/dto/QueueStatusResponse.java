package com.ds2api.admin.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Map;

/**
 * Response DTO for GET /admin/queue/status.
 * Provides a real-time snapshot of the account pool: total accounts,
 * slot limits, global 429 threshold, and per-account slot/queue/in-flight metrics.
 */
@Data
public class QueueStatusResponse {

    @JsonProperty("total_accounts")
    private int totalAccounts;

    @JsonProperty("max_inflight_per_account")
    private int maxInflightPerAccount;

    @JsonProperty("max_queue_per_account")
    private int maxQueuePerAccount;

    @JsonProperty("global_429_threshold")
    private int global429Threshold;

    @JsonProperty("accounts")
    private Map<String, AccountSlotStatus> accounts;

    @Data
    public static class AccountSlotStatus {

        @JsonProperty("available_slots")
        private int availableSlots;

        @JsonProperty("queued_requests")
        private int queuedRequests;

        @JsonProperty("in_flight")
        private int inFlight;
    }
}
