package com.ds2api.model;

/**
 * Unified internal stream event model.
 * All upstream DeepSeek SSE chunks are normalized into these sealed events
 * before being translated to protocol-specific output by adapters.
 */
public sealed interface InternalStreamEvent {

    /** Transport-layer session created; carries upstream session metadata. */
    record SessionCreated(String sessionId) implements InternalStreamEvent {}

    /** Visible text token delta (may be empty string for no-op frames). */
    record TextDelta(String chunk) implements InternalStreamEvent {}

    /** Thinking/reasoning token delta (for chain-of-thought display). */
    record ThinkingDelta(String chunk) implements InternalStreamEvent {}

    /** Tool call started. */
    record ToolCallStart(String callId, String name, int toolIndex) implements InternalStreamEvent {}

    /** Tool call arguments delta (incremental JSON fragment). */
    record ToolCallDelta(String callId, int toolIndex, String argumentsDelta) implements InternalStreamEvent {}

    /** Tool call completed. */
    record ToolCallEnd(String callId, int toolIndex) implements InternalStreamEvent {}

    /** Generation stream finished. Reason: "stop", "length", "content_filter", etc. */
    record Finish(String reason) implements InternalStreamEvent {}

    /** Error event with message and HTTP status code. */
    record Error(String message, int code) implements InternalStreamEvent {}
}
