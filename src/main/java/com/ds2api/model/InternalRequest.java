package com.ds2api.model;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

/**
 * Normalized internal request model, produced by protocol adapters
 * from raw JSON request bodies.
 */
public record InternalRequest(
    String model,
    List<Message> messages,
    boolean stream,
    JsonNode tools,
    String toolChoice,
    String conversationId,
    Map<String, Object> passThrough
) {
    public record Message(String role, String content) {}

    /** Default model when none specified in the request. */
    public static final String DEFAULT_MODEL = "deepseek-v4-flash";

    /** Pass-through parameter names that are forwarded to DeepSeek. */
    private static final List<String> PASS_THROUGH_KEYS = List.of(
        "temperature", "top_p", "max_tokens", "frequency_penalty",
        "presence_penalty", "stop", "n", "logprobs", "top_logprobs"
    );

    public InternalRequest {
        if (toolChoice == null) {
            toolChoice = "auto";
        }
        if (passThrough == null) {
            passThrough = Map.of();
        }
    }

    /** Convenience constructor without passThrough. */
    public InternalRequest(String model, List<Message> messages, boolean stream,
                           JsonNode tools, String toolChoice, String conversationId) {
        this(model, messages, stream, tools, toolChoice, conversationId, Map.of());
    }

    /** Create a copy with a different model (used after alias resolution). */
    public InternalRequest withModel(String newModel) {
        return new InternalRequest(newModel, messages, stream, tools, toolChoice, conversationId, passThrough);
    }

    /** Create a copy with transformed messages (used by compat pipeline). */
    public InternalRequest withMessages(List<Message> newMessages) {
        return new InternalRequest(model, newMessages, stream, tools, toolChoice, conversationId, passThrough);
    }

    /** Create a copy with a conversationId (used for session continuation). */
    public InternalRequest withConversationId(String newConversationId) {
        return new InternalRequest(model, messages, stream, tools, toolChoice, newConversationId, passThrough);
    }

    /** Extract pass-through parameters from a raw request body. */
    public static Map<String, Object> extractPassThrough(JsonNode body) {
        java.util.Map<String, Object> result = new java.util.LinkedHashMap<>();
        for (String key : PASS_THROUGH_KEYS) {
            JsonNode node = body.path(key);
            if (!node.isMissingNode() && !node.isNull()) {
                if (node.isNumber()) result.put(key, node.numberValue());
                else if (node.isBoolean()) result.put(key, node.booleanValue());
                else if (node.isTextual()) result.put(key, node.textValue());
                else result.put(key, node.toString());
            }
        }
        return result;
    }
}
