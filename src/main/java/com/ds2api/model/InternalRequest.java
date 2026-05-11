package com.ds2api.model;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * Normalized internal request model, produced by protocol adapters
 * from raw JSON request bodies.
 */
public record InternalRequest(
    String model,
    List<Message> messages,
    boolean stream,
    JsonNode tools,
    String toolChoice
) {
    public record Message(String role, String content) {}

    /** Default model when none specified in the request. */
    public static final String DEFAULT_MODEL = "deepseek-v4-flash";

    public InternalRequest {
        if (toolChoice == null) {
            toolChoice = "auto";
        }
    }

    /** Create a copy with a different model (used after alias resolution). */
    public InternalRequest withModel(String newModel) {
        return new InternalRequest(newModel, messages, stream, tools, toolChoice);
    }

    /** Create a copy with transformed messages (used by compat pipeline). */
    public InternalRequest withMessages(List<Message> newMessages) {
        return new InternalRequest(model, newMessages, stream, tools, toolChoice);
    }
}
