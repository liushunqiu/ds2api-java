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
            // Extract tool name - check multiple locations (like Go's ExtractToolMeta)
            String name = extractToolName(tool);
            if (name.isBlank()) continue;
            String desc = extractToolDesc(tool);
            JsonNode params = extractToolSchema(tool);
            toolNames.add(name);

            toolSchemas.append("Tool: ").append(name).append("\n");
            toolSchemas.append("Description: ").append(desc).append("\n");
            toolSchemas.append("Parameters: ");
            if (params != null && !params.isMissingNode() && !params.isNull()) {
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
            + "CRITICAL: You MUST use EXACTLY the tool names listed below. Do NOT invent, rename, or substitute tool names.\n"
            + "Available tool names: " + String.join(", ", toolNames) + "\n\n"
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

    /**
     * Extract tool name from various Codex/OpenAI tool formats.
     * Checks: tool.name, tool.function.name
     */
    private String extractToolName(JsonNode tool) {
        String name = tool.path("name").asText("").trim();
        if (!name.isEmpty()) return name;
        name = tool.path("function").path("name").asText("").trim();
        return name;
    }

    /**
     * Extract tool description from various formats.
     * Checks: tool.description, tool.function.description
     */
    private String extractToolDesc(JsonNode tool) {
        String desc = tool.path("description").asText("").trim();
        if (!desc.isEmpty()) return desc;
        desc = tool.path("function").path("description").asText("").trim();
        if (!desc.isEmpty()) return desc;
        return "No description available";
    }

    /**
     * Extract tool parameter schema from various formats.
     * Checks: tool.parameters, tool.input_schema, tool.inputSchema, tool.schema,
     *         tool.function.parameters, tool.function.input_schema, etc.
     */
    private JsonNode extractToolSchema(JsonNode tool) {
        // Check top-level first
        for (String key : new String[]{"parameters", "input_schema", "inputSchema", "schema"}) {
            JsonNode node = tool.path(key);
            if (!node.isMissingNode() && !node.isNull()) return node;
        }
        // Check function wrapper
        JsonNode func = tool.path("function");
        if (!func.isMissingNode()) {
            for (String key : new String[]{"parameters", "input_schema", "inputSchema", "schema"}) {
                JsonNode node = func.path(key);
                if (!node.isMissingNode() && !node.isNull()) return node;
            }
        }
        return null;
    }

    private String buildToolCallInstructions(List<String> toolNames) {
        String examples = buildToolExamples(toolNames);
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
            4) All string values must use <![CDATA[...]]>, even short ones. This includes code, scripts, file contents, prompts, paths, names, and queries.
            5) Every top-level argument must be a <|DSML|parameter name="ARG_NAME">...</|DSML|parameter> node.
            6) Objects use nested XML elements inside the parameter body. Arrays may repeat <item> children.
            7) Numbers, booleans, and null stay plain text.
            8) Use only the parameter names in the tool schema. Do not invent fields.
            9) Do NOT wrap XML in markdown fences. Do NOT output explanations, role markers, or internal monologue.
            10) If you call a tool, the first non-whitespace characters of that tool block must be exactly <|DSML|tool_calls>.
            11) Never omit the opening <|DSML|tool_calls> tag, even if you already plan to close with </|DSML|tool_calls>.
            12) Compatibility note: the runtime also accepts the legacy XML tags <tool_calls> / <invoke> / <parameter>, but prefer the DSML-prefixed form above.
            13) CRITICAL for file writing tools (write_stdin, write_file, etc.): The parameter value must contain ONLY the raw file content. Do NOT wrap content in shell commands like heredoc (<< 'EOF' ... EOF), cat, echo, or any other shell syntax. Just provide the pure content directly.

            PARAMETER SHAPES:
            - string => <|DSML|parameter name="x"><![CDATA[value]]></|DSML|parameter>
            - object => <|DSML|parameter name="x"><field>...</field></|DSML|parameter>
            - array => <|DSML|parameter name="x"><item>...</item><item>...</item></|DSML|parameter>
            - number/bool/null => <|DSML|parameter name="x">plain_text</|DSML|parameter>

            【WRONG — Do NOT do these】:

            Wrong 1 — mixed text after XML:
              <|DSML|tool_calls>...</|DSML|tool_calls> I hope this helps.
            Wrong 2 — Markdown code fences:
              ```xml
              <|DSML|tool_calls>...</|DSML|tool_calls>
              ```
            Wrong 3 — missing opening wrapper:
              <|DSML|invoke name="TOOL_NAME">...</|DSML|invoke>
              </|DSML|tool_calls>

            Remember: The ONLY valid way to use tools is the <|DSML|tool_calls>...</|DSML|tool_calls> block at the end of your response.

            """ + examples;
    }

    private String buildToolExamples(List<String> toolNames) {
        List<String> examples = new ArrayList<>();

        // Find a basic single-tool example
        String basicExample = findBasicExample(toolNames);
        if (basicExample != null) {
            examples.add("Example A — Single tool:\n" + basicExample);
        }

        // Find a parallel two-tool example
        List<String> parallelExamples = findParallelExamples(toolNames, 2);
        if (parallelExamples.size() >= 2) {
            examples.add("Example B — Two tools in parallel:\n" +
                "<|DSML|tool_calls>\n" +
                String.join("\n", parallelExamples) + "\n" +
                "</|DSML|tool_calls>");
        }

        if (examples.isEmpty()) {
            return "";
        }
        return "【CORRECT EXAMPLES】:\n\n" + String.join("\n\n", examples) + "\n\n";
    }

    private String findBasicExample(List<String> toolNames) {
        for (String name : toolNames) {
            String params = getExampleParams(name);
            if (params != null) {
                return "<|DSML|tool_calls>\n" + params + "\n</|DSML|tool_calls>";
            }
        }
        return null;
    }

    private List<String> findParallelExamples(List<String> toolNames, int count) {
        List<String> results = new ArrayList<>();
        for (String name : toolNames) {
            String params = getExampleParams(name);
            if (params != null) {
                results.add(params);
                if (results.size() >= count) break;
            }
        }
        return results;
    }

    private String getExampleParams(String name) {
        return switch (name) {
            case "Bash", "execute_command", "exec_command" ->
                "  <|DSML|invoke name=\"" + name + "\">\n" +
                "    <|DSML|parameter name=\"command\"><![CDATA[pwd]]></|DSML|parameter>\n" +
                "  </|DSML|invoke>";
            case "Read", "read_file" ->
                "  <|DSML|invoke name=\"" + name + "\">\n" +
                "    <|DSML|parameter name=\"file_path\"><![CDATA[README.md]]></|DSML|parameter>\n" +
                "  </|DSML|invoke>";
            case "Glob", "list_files" ->
                "  <|DSML|invoke name=\"" + name + "\">\n" +
                "    <|DSML|parameter name=\"pattern\"><![CDATA[**/*.go]]></|DSML|parameter>\n" +
                "    <|DSML|parameter name=\"path\"><![CDATA[.]]></|DSML|parameter>\n" +
                "  </|DSML|invoke>";
            case "search_files" ->
                "  <|DSML|invoke name=\"" + name + "\">\n" +
                "    <|DSML|parameter name=\"query\"><![CDATA[tool call parser]]></|DSML|parameter>\n" +
                "  </|DSML|invoke>";
            case "Write", "write_to_file" ->
                "  <|DSML|invoke name=\"" + name + "\">\n" +
                "    <|DSML|parameter name=\"file_path\"><![CDATA[notes.txt]]></|DSML|parameter>\n" +
                "    <|DSML|parameter name=\"content\"><![CDATA[Hello world]]></|DSML|parameter>\n" +
                "  </|DSML|invoke>";
            case "Edit" ->
                "  <|DSML|invoke name=\"" + name + "\">\n" +
                "    <|DSML|parameter name=\"file_path\"><![CDATA[README.md]]></|DSML|parameter>\n" +
                "    <|DSML|parameter name=\"old_string\"><![CDATA[foo]]></|DSML|parameter>\n" +
                "    <|DSML|parameter name=\"new_string\"><![CDATA[bar]]></|DSML|parameter>\n" +
                "  </|DSML|invoke>";
            default -> null;
        };
    }
}
