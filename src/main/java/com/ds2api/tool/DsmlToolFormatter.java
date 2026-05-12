package com.ds2api.tool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

/**
 * Formats OpenAI tool_calls into DSML prompt-visible history blocks.
 * Equivalent to Go's prompt/tool_calls.go FormatToolCallsForPrompt.
 *
 * Renders nested objects as XML elements, arrays as repeated <item> children,
 * and strings wrapped in CDATA.
 */
public final class DsmlToolFormatter {

    private DsmlToolFormatter() {}

    /**
     * Convert a JsonNode array of OpenAI tool_calls into DSML XML string
     * suitable for embedding in the prompt sent to DeepSeek.
     */
    public static String convertToolCallsToDSML(JsonNode toolCallsNode, ObjectMapper mapper) {
        if (toolCallsNode == null || !toolCallsNode.isArray() || toolCallsNode.isEmpty()) {
            return "";
        }

        List<String> blocks = new ArrayList<>();
        for (JsonNode tc : toolCallsNode) {
            String block = formatSingleToolCall(tc, mapper);
            if (!block.isEmpty()) {
                blocks.add(block);
            }
        }
        if (blocks.isEmpty()) {
            return "";
        }
        return "<|DSML|tool_calls>\n" + String.join("\n", blocks) + "\n</|DSML|tool_calls>";
    }

    private static String formatSingleToolCall(JsonNode call, ObjectMapper mapper) {
        if (call == null) return "";

        String name = "";
        JsonNode argsRaw = null;

        // Handle OpenAI format: { "id": "...", "type": "function", "function": { "name": "...", "arguments": "..." } }
        JsonNode func = call.path("function");
        if (!func.isMissingNode()) {
            name = func.path("name").asText("");
            argsRaw = func.path("arguments");
        }

        // Also handle direct name/arguments
        if (name.isEmpty()) {
            name = call.path("name").asText("");
        }
        if (argsRaw == null || argsRaw.isMissingNode()) {
            argsRaw = call.path("arguments");
        }
        if (argsRaw == null || argsRaw.isMissingNode()) {
            argsRaw = call.path("input");
        }

        name = name.trim();
        if (name.isEmpty()) return "";

        String parameters = formatParametersForPrompt(argsRaw, mapper);
        if (parameters.isEmpty()) {
            return "  <|DSML|invoke name=\"" + escapeXmlAttr(name) + "\"></|DSML|invoke>";
        }
        return "  <|DSML|invoke name=\"" + escapeXmlAttr(name) + "\">\n" +
               parameters + "\n" +
               "  </|DSML|invoke>";
    }

    private static String formatParametersForPrompt(JsonNode argsRaw, ObjectMapper mapper) {
        if (argsRaw == null || argsRaw.isMissingNode()) {
            return "";
        }

        // If it's a text string, try to parse as JSON first
        if (argsRaw.isTextual()) {
            String text = argsRaw.asText("").trim();
            if (text.isEmpty()) return "";
            try {
                JsonNode parsed = mapper.readTree(text);
                if (parsed.isObject()) {
                    return renderObjectParams(parsed, "    ");
                }
            } catch (Exception e) {
                // Not valid JSON, render as single content parameter
            }
            return "    <|DSML|parameter name=\"content\">" + renderCdata(text) + "</|DSML|parameter>";
        }

        if (argsRaw.isObject()) {
            if (argsRaw.isEmpty()) return "";
            return renderObjectParams(argsRaw, "    ");
        }

        if (argsRaw.isArray()) {
            return renderArrayParams(argsRaw, "    ");
        }

        // Primitive
        return "    <|DSML|parameter name=\"value\">" + renderCdata(argsRaw.asText()) + "</|DSML|parameter>";
    }

    private static String renderObjectParams(JsonNode obj, String indent) {
        List<String> lines = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> fields = obj.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String rendered = renderParameterNode(entry.getKey(), entry.getValue(), indent);
            if (!rendered.isEmpty()) {
                lines.add(rendered);
            }
        }
        return String.join("\n", lines);
    }

    private static String renderArrayParams(JsonNode arr, String indent) {
        List<String> lines = new ArrayList<>();
        for (JsonNode item : arr) {
            String rendered = renderParameterNode("item", item, indent);
            if (!rendered.isEmpty()) {
                lines.add(rendered);
            }
        }
        return String.join("\n", lines);
    }

    private static String renderParameterNode(String name, JsonNode value, String indent) {
        if (name == null || name.trim().isEmpty()) return "";
        name = name.trim();

        if (value == null || value.isNull()) {
            return indent + "<|DSML|parameter name=\"" + escapeXmlAttr(name) + "\"></|DSML|parameter>";
        }

        if (value.isObject()) {
            if (value.isEmpty()) {
                return indent + "<|DSML|parameter name=\"" + escapeXmlAttr(name) + "\"></|DSML|parameter>";
            }
            String inner = renderObjectParams(value, indent + "  ");
            if (inner.trim().isEmpty()) {
                return indent + "<|DSML|parameter name=\"" + escapeXmlAttr(name) + "\"></|DSML|parameter>";
            }
            return indent + "<|DSML|parameter name=\"" + escapeXmlAttr(name) + "\">\n" +
                   inner + "\n" +
                   indent + "</|DSML|parameter>";
        }

        if (value.isArray()) {
            if (value.isEmpty()) {
                return indent + "<|DSML|parameter name=\"" + escapeXmlAttr(name) + "\"></|DSML|parameter>";
            }
            List<String> itemLines = new ArrayList<>();
            for (JsonNode item : value) {
                String rendered = renderParameterNode("item", item, indent + "  ");
                if (!rendered.isEmpty()) {
                    itemLines.add(rendered);
                }
            }
            if (itemLines.isEmpty()) {
                return indent + "<|DSML|parameter name=\"" + escapeXmlAttr(name) + "\"></|DSML|parameter>";
            }
            return indent + "<|DSML|parameter name=\"" + escapeXmlAttr(name) + "\">\n" +
                   String.join("\n", itemLines) + "\n" +
                   indent + "</|DSML|parameter>";
        }

        // Primitive: wrap string in CDATA, numbers/booleans as plain text
        String textValue;
        if (value.isTextual()) {
            textValue = renderCdata(value.asText());
        } else {
            textValue = escapeXmlText(value.asText());
        }
        return indent + "<|DSML|parameter name=\"" + escapeXmlAttr(name) + "\">" + textValue + "</|DSML|parameter>";
    }

    private static String renderCdata(String text) {
        if (text == null || text.isEmpty()) return "";
        if (text.contains("]]>")) {
            return "<![CDATA[" + text.replace("]]>", "]]]]><![CDATA[>") + "]]>";
        }
        return "<![CDATA[" + text + "]]>";
    }

    private static String escapeXmlAttr(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("\"", "&quot;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;");
    }

    private static String escapeXmlText(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;");
    }
}
