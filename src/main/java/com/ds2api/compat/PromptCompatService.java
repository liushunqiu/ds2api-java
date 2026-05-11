package com.ds2api.compat;

import com.ds2api.config.ConfigLoaderService;
import com.ds2api.config.Ds2Config;
import com.ds2api.model.InternalRequest;
import com.ds2api.model.InternalRequest.Message;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Prompt compatibility layer: converts OpenAI-style tool definitions to DSML
 * prompt blocks and injects thinking prompts for DeepSeek web context.
 *
 * Aligned with ds2api knowledge-base semantics:
 * - thinking_injection: append prompt to latest user message
 * - tools: convert JsonNode array to DSML/XML prompt injected into system message
 */
@Service
public class PromptCompatService {

    private static final Logger log = LoggerFactory.getLogger(PromptCompatService.class);
    private static final String DEFAULT_THINKING_PROMPT =
        "\n\n[System Enhancement] Please reason step-by-step before responding. "
        + "If tools are provided, use strict DSML/XML format for tool calls.";

    private final ConfigLoaderService configLoader;

    public PromptCompatService(ConfigLoaderService configLoader) {
        this.configLoader = configLoader;
    }

    /**
     * Synchronous compat layer:
     * 1. thinking_injection appended to latest user message
     * 2. tools definitions converted to DSML prompt block injected into system prompt
     */
    public InternalRequest applyCompat(InternalRequest req) {
        Ds2Config config = configLoader.getConfig();
        List<Message> messages = new ArrayList<>(req.messages());

        // 1. Thinking injection
        if (config.getThinkingInjection().isEnabled()) {
            messages = injectThinkingPrompt(messages, config);
        }

        // 2. Tool definitions -> DSML prompt (DeepSeek Web doesn't natively support OpenAI tools array)
        if (req.tools() != null && !req.tools().isNull() && req.tools().isArray()
                && req.tools().size() > 0) {
            messages = injectToolDefinitions(messages, req.tools(), config);
        }

        return req.withMessages(messages);
    }

    private List<Message> injectThinkingPrompt(List<Message> messages, Ds2Config config) {
        for (int i = messages.size() - 1; i >= 0; i--) {
            Message msg = messages.get(i);
            if ("user".equals(msg.role())) {
                String prompt = config.getThinkingInjection().getPrompt();
                if (prompt == null || prompt.isBlank()) {
                    prompt = DEFAULT_THINKING_PROMPT;
                }
                messages.set(i, new Message(msg.role(), msg.content() + prompt));
                log.debug("[Compat] Thinking prompt injected to latest user message");
                break; // only inject into last user message
            }
        }
        return messages;
    }

    private List<Message> injectToolDefinitions(List<Message> messages, JsonNode tools,
                                                 Ds2Config config) {
        StringBuilder toolPrompt = new StringBuilder(
            "\n\n[Available Tools Specification]\n<|DSML|tool_calls>\n");
        for (JsonNode tool : tools) {
            JsonNode func = tool.path("function");
            String name = func.path("name").asText("unknown");
            String desc = func.path("description").asText("");
            toolPrompt.append("<|DSML|invoke name=\"").append(name).append("\">\n");
            if (!desc.isBlank()) {
                toolPrompt.append("<!-- ").append(desc).append(" -->\n");
            }
            toolPrompt.append("</|DSML|invoke>\n");
        }
        toolPrompt.append("</|DSML|tool_calls>\n")
                  .append("Please use the above DSML/XML format for tool calls.");

        String toolBlock = toolPrompt.toString();
        boolean hasSystem = false;
        for (int i = 0; i < messages.size(); i++) {
            Message msg = messages.get(i);
            if ("system".equals(msg.role())) {
                messages.set(i, new Message(msg.role(), msg.content() + toolBlock));
                hasSystem = true;
                break;
            }
        }
        if (!hasSystem) {
            messages.add(0, new Message("system", toolBlock));
        }
        log.debug("[Compat] Tool definitions injected as DSML prompt block");
        return messages;
    }
}
