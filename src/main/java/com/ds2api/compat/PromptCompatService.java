package com.ds2api.compat;

import com.ds2api.config.ConfigLoaderService;
import com.ds2api.config.Ds2Config;
import com.ds2api.model.InternalRequest;
import com.ds2api.model.InternalRequest.Message;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final ObjectMapper mapper;

    public PromptCompatService(ConfigLoaderService configLoader, ObjectMapper mapper) {
        this.configLoader = configLoader;
        this.mapper = mapper;
    }

    /**
     * Synchronous compat layer:
     * 1. Thinking injection is DISABLED (aligned with Go reference - thinking_enabled is payload-level only)
     * 2. tools definitions converted to DSML prompt block injected into system prompt
     */
    public InternalRequest applyCompat(InternalRequest req) {
        Ds2Config config = configLoader.getConfig();
        List<Message> messages = new ArrayList<>(req.messages());

        // 1. Thinking injection: DISABLED (Go reference does not inject thinking prompts into messages)
        // thinking_enabled is controlled at the payload level only

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
        StringBuilder toolSchemas = new StringBuilder();
        List<String> toolNames = new ArrayList<>();

        for (JsonNode tool : tools) {
            JsonNode func = tool.path("function");
            String name = func.path("name").asText("");
            if (name.isBlank()) continue;
            String desc = func.path("description").asText("No description available");
            JsonNode params = func.path("parameters");
            toolNames.add(name);

            toolSchemas.append("Tool: ").append(name).append("\n");
            toolSchemas.append("Description: ").append(desc).append("\n");
            toolSchemas.append("Parameters: ");
            if (!params.isMissingNode() && !params.isNull()) {
                try {
                    toolSchemas.append(mapper.writeValueAsString(params));
                } catch (Exception e) {
                    toolSchemas.append("{}");
                }
            } else {
                toolSchemas.append("{}");
            }
            toolSchemas.append("\n\n");
        }

        String toolCallInstructions = buildToolCallInstructions(toolNames);
        String toolBlock = "\n\nYou have access to these tools:\n\n"
            + toolSchemas + "\n" + toolCallInstructions;

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
        log.debug("[Compat] Tool definitions injected");
        return messages;
    }

    private String buildToolCallInstructions(List<String> toolNames) {
        return """
            TOOL CALL FORMAT — FOLLOW EXACTLY:

            <|DSML|tool_calls>
              <|DSML|invoke name="TOOL_NAME_HERE">
                <|DSML|parameter name="PARAMETER_NAME"><![CDATA[PARAMETER_VALUE]]></|DSML|parameter>
              </|DSML|invoke>
            </|DSML|tool_calls>

            RULES:
            1) Use the <|DSML|tool_calls> wrapper format.
            2) Put one or more <|DSML|invoke> entries under a single <|DSML|tool_calls> root.
            3) Put the tool name in the invoke name attribute: <|DSML|invoke name="TOOL_NAME">.
            4) All string values must use <![CDATA[...]]>, even short ones.
            5) Every top-level argument must be a <|DSML|parameter name="ARG_NAME">...</|DSML|parameter> node.
            6) Numbers, booleans, and null stay plain text.
            7) Use only the parameter names in the tool schema. Do not invent fields.
            8) Do NOT wrap XML in markdown fences.
            9) If you call a tool, the first non-whitespace characters of that tool block must be exactly <|DSML|tool_calls>.

            Remember: The ONLY valid way to use tools is the <|DSML|tool_calls>...</|DSML|tool_calls> block at the end of your response.
            """;
    }
}
