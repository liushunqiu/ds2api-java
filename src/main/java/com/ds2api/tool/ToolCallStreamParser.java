package com.ds2api.tool;

import com.ds2api.model.InternalStreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ToolCallStreamParser {

    private static final Logger log = LoggerFactory.getLogger(ToolCallStreamParser.class);

    private static final int MAX_CAPTURE_LEN = 8192;

    private static final Pattern CODE_FENCE = Pattern.compile("```");
    private static final Pattern TOOL_CALLS_START = Pattern.compile("<\\|?DSML\\|?tool_calls\\s*>", Pattern.CASE_INSENSITIVE);
    private static final Pattern TOOL_CALLS_END = Pattern.compile("</\\|?DSML\\|?tool_calls\\s*>", Pattern.CASE_INSENSITIVE);
    private static final Pattern INVOKE_START = Pattern.compile("<\\|?DSML\\|?invoke\\s+name=\"([^\"]+)\"\\s*>", Pattern.CASE_INSENSITIVE);
    private static final Pattern INVOKE_END = Pattern.compile("</\\|?DSML\\|?invoke\\s*>", Pattern.CASE_INSENSITIVE);
    private static final Pattern PARAM_START = Pattern.compile("<\\|?DSML\\|?parameter\\s+name=\"([^\"]+)\"[^>]*>", Pattern.CASE_INSENSITIVE);
    private static final Pattern PARAM_END = Pattern.compile("</\\|?DSML\\|?parameter\\s*>", Pattern.CASE_INSENSITIVE);

    private enum State { IDLE, IN_CODE_BLOCK, CAPTURING }

    private State state = State.IDLE;
    private final StringBuilder buffer = new StringBuilder();

    public List<InternalStreamEvent> processChunk(String chunk) {
        if (chunk == null || chunk.isEmpty()) return List.of();
        log.debug("[ToolParser] processChunk: state={} chunkLen={} chunk={}", state, chunk.length(),
            chunk.length() > 200 ? chunk.substring(0, 200) + "..." : chunk);
        buffer.append(chunk);
        return drain();
    }

    public List<InternalStreamEvent> flushAndReset() {
        List<InternalStreamEvent> events = new ArrayList<>();
        if (!buffer.isEmpty()) {
            log.info("[ToolParser] flushAndReset: releasing buffered text ({} chars): {}",
                buffer.length(),
                buffer.length() > 300 ? buffer.substring(0, 300) + "..." : buffer.toString());
            events.add(new InternalStreamEvent.TextDelta(buffer.toString()));
        }
        reset();
        return events;
    }

    public void reset() {
        state = State.IDLE;
        buffer.setLength(0);
    }

    private List<InternalStreamEvent> drain() {
        List<InternalStreamEvent> events = new ArrayList<>();
        boolean madeProgress = true;
        while (madeProgress && !buffer.isEmpty()) {
            madeProgress = false;
            switch (state) {
                case IDLE -> {
                    if (processIdle(events)) madeProgress = true;
                }
                case IN_CODE_BLOCK -> {
                    if (processCodeBlock(events)) madeProgress = true;
                }
                case CAPTURING -> {
                    if (processCapturing(events)) madeProgress = true;
                }
            }
        }
        return events;
    }

    private boolean processIdle(List<InternalStreamEvent> events) {
        String text = buffer.toString();

        Matcher fence = CODE_FENCE.matcher(text);
        Matcher tcStart = TOOL_CALLS_START.matcher(text);

        boolean hasFence = fence.find();
        boolean hasTcStart = tcStart.find();
        int fenceIdx = hasFence ? fence.start() : Integer.MAX_VALUE;
        int tcIdx = hasTcStart ? tcStart.start() : Integer.MAX_VALUE;

        if (fenceIdx < tcIdx && fenceIdx != Integer.MAX_VALUE) {
            if (fenceIdx > 0) events.add(new InternalStreamEvent.TextDelta(text.substring(0, fenceIdx)));
            buffer.delete(0, fenceIdx + fence.group().length());
            state = State.IN_CODE_BLOCK;
            return true;
        } else if (tcIdx != Integer.MAX_VALUE) {
            if (tcIdx > 0) events.add(new InternalStreamEvent.TextDelta(text.substring(0, tcIdx)));
            int afterStart = tcIdx + tcStart.group().length();
            buffer.delete(0, afterStart);
            state = State.CAPTURING;
            log.info("[ToolParser] Entered CAPTURING after <tool_calls> tag found at idx={}", tcIdx);
            return true;
        } else {
            int safeEnd = findSafeEnd(text);
            if (safeEnd > 0) {
                events.add(new InternalStreamEvent.TextDelta(text.substring(0, safeEnd)));
                buffer.delete(0, safeEnd);
                return true;
            }
            return false;
        }
    }

    private boolean processCodeBlock(List<InternalStreamEvent> events) {
        String text = buffer.toString();
        Matcher fence = CODE_FENCE.matcher(text);
        if (fence.find()) {
            if (fence.start() > 0) events.add(new InternalStreamEvent.TextDelta(text.substring(0, fence.start())));
            buffer.delete(0, fence.end());
            state = State.IDLE;
            return true;
        }
        return false;
    }

    private boolean processCapturing(List<InternalStreamEvent> events) {
        String text = buffer.toString();

        if (text.length() > MAX_CAPTURE_LEN) {
            log.debug("[ToolCallStreamParser] Capture overflow, releasing as text");
            events.add(new InternalStreamEvent.TextDelta(text));
            buffer.setLength(0);
            state = State.IDLE;
            return true;
        }

        Matcher tcEnd = TOOL_CALLS_END.matcher(text);
        if (tcEnd.find()) {
            String captured = text.substring(0, tcEnd.start());
            log.debug("[ToolCallStreamParser] Found </tool_calls>, parsing {} chars", captured.length());
            parseAndEmitToolCalls(captured, events);
            buffer.delete(0, tcEnd.end());
            state = State.IDLE;
            return true;
        }
        return false;
    }

    private void parseAndEmitToolCalls(String captured, List<InternalStreamEvent> events) {
        Matcher invokeMatcher = INVOKE_START.matcher(captured);
        int pos = 0;
        int toolIndex = 0;
        while (invokeMatcher.find(pos)) {
            String toolName = invokeMatcher.group(1);
            String callId = "call_" + UUID.randomUUID().toString().substring(0, 8);
            
            // Emit ToolCallStart with name
            events.add(new InternalStreamEvent.ToolCallStart(callId, toolName, toolIndex));
            log.debug("[ToolCallStreamParser] ToolCallStart: {} ({}) index={}", toolName, callId, toolIndex);

            int invokeStart = invokeMatcher.end();
            Matcher invokeEndMatcher = INVOKE_END.matcher(captured);
            if (invokeEndMatcher.find(invokeStart)) {
                // Parse and emit arguments as incremental deltas
                parseInvokeBodyIncremental(captured.substring(invokeStart, invokeEndMatcher.start()), callId, toolIndex, events);
                pos = invokeEndMatcher.end();
            } else {
                String body = captured.substring(invokeStart);
                if (!body.isEmpty()) {
                    // Emit arguments as a single delta
                    String argsJson = buildArgumentsJson(body);
                    events.add(new InternalStreamEvent.ToolCallDelta(callId, toolIndex, argsJson));
                }
                pos = captured.length();
            }
            events.add(new InternalStreamEvent.ToolCallEnd(callId, toolIndex));
            log.debug("[ToolCallStreamParser] ToolCallEnd: {} index={}", callId, toolIndex);
            toolIndex++;
        }
    }

    private void parseInvokeBodyIncremental(String body, String callId, int toolIndex, List<InternalStreamEvent> events) {
        // Use proper XML parsing for nested structures
        java.util.Map<String, Object> params = parseParametersAsStructured(body);

        // Build JSON arguments and emit as delta
        if (!params.isEmpty()) {
            String argsJson = buildStructuredJson(params);
            log.info("[ToolCallStreamParser] ToolCallDelta: {} index={} args={}", callId, toolIndex,
                argsJson.length() > 200 ? argsJson.substring(0, 200) + "..." : argsJson);
            events.add(new InternalStreamEvent.ToolCallDelta(callId, toolIndex, argsJson));
        }
    }

    /**
     * Parse DSML parameter XML into structured map.
     * Handles nested <parameter> elements recursively.
     * Returns Map<String, Object> where values can be String, Map, or List.
     */
    private java.util.Map<String, Object> parseParametersAsStructured(String body) {
        java.util.Map<String, Object> result = new java.util.LinkedHashMap<>();
        // Sanitize body: escape special chars inside CDATA that break XML parsing
        String sanitized = sanitizeXmlContent(body);
        // Wrap in a root element for XML parsing
        String xml = "<root>" + sanitized + "</root>";
        try {
            javax.xml.parsers.DocumentBuilderFactory factory = javax.xml.parsers.DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(false);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            javax.xml.parsers.DocumentBuilder builder = factory.newDocumentBuilder();
            org.w3c.dom.Document doc = builder.parse(new org.xml.sax.InputSource(new java.io.StringReader(xml)));
            org.w3c.dom.Element root = doc.getDocumentElement();
            parseChildParameters(root, result);
        } catch (Exception e) {
            log.warn("[ToolCallStreamParser] XML parse failed, falling back to regex: {}", e.getMessage());
            // Fallback to regex-based parsing
            java.util.Map<String, Object> fallback = new java.util.LinkedHashMap<>();
            fallback.putAll(parseParametersAsFlatMap(body));
            return fallback;
        }
        return result;
    }

    /**
     * Sanitize XML content by escaping special characters inside CDATA sections
     * that would cause XML parsing to fail (e.g., ]]> sequences).
     */
    private String sanitizeXmlContent(String content) {
        if (content == null) return "";
        // Fix CDATA sections that contain ]]> by splitting them
        return content.replaceAll("]]>", "]]]]><![CDATA[>");
    }

    private void parseChildParameters(org.w3c.dom.Element parent, java.util.Map<String, Object> result) {
        org.w3c.dom.NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            org.w3c.dom.Node node = children.item(i);
            if (!(node instanceof org.w3c.dom.Element elem)) continue;
            if (!"parameter".equalsIgnoreCase(elem.getTagName()) && !"item".equalsIgnoreCase(elem.getTagName())) continue;

            String name = elem.getAttribute("name");
            if (name.isEmpty() && "item".equalsIgnoreCase(elem.getTagName())) {
                name = "item";
            }
            if (name.isEmpty()) continue;

            // Check if this parameter has nested <parameter> children
            boolean hasNestedParams = false;
            org.w3c.dom.NodeList elemChildren = elem.getChildNodes();
            for (int j = 0; j < elemChildren.getLength(); j++) {
                org.w3c.dom.Node child = elemChildren.item(j);
                if (child instanceof org.w3c.dom.Element childElem) {
                    if ("parameter".equalsIgnoreCase(childElem.getTagName()) || "item".equalsIgnoreCase(childElem.getTagName())) {
                        hasNestedParams = true;
                        break;
                    }
                }
            }

            if (hasNestedParams) {
                // Check if children are all "item" elements (array)
                boolean allItems = true;
                java.util.List<Object> items = new java.util.ArrayList<>();
                java.util.Map<String, Object> nested = new java.util.LinkedHashMap<>();

                for (int j = 0; j < elemChildren.getLength(); j++) {
                    org.w3c.dom.Node child = elemChildren.item(j);
                    if (!(child instanceof org.w3c.dom.Element childElem)) continue;
                    if (!"parameter".equalsIgnoreCase(childElem.getTagName()) && !"item".equalsIgnoreCase(childElem.getTagName())) continue;

                    String childName = childElem.getAttribute("name");
                    if (childName.isEmpty() && "item".equalsIgnoreCase(childElem.getTagName())) {
                        childName = "item";
                    }

                    if ("item".equalsIgnoreCase(childElem.getTagName()) || "item".equals(childName)) {
                        // This is an array item
                        java.util.Map<String, Object> itemMap = new java.util.LinkedHashMap<>();
                        parseChildParameters(childElem, itemMap);
                        items.add(itemMap.isEmpty() ? getElementText(childElem) : itemMap);
                    } else {
                        allItems = false;
                        java.util.Map<String, Object> childMap = new java.util.LinkedHashMap<>();
                        parseChildParameters(childElem, childMap);
                        if (childMap.isEmpty()) {
                            nested.put(childName, getElementText(childElem));
                        } else {
                            nested.put(childName, childMap);
                        }
                    }
                }

                if (!items.isEmpty()) {
                    // Array: use the parent name as key
                    if (result.containsKey(name)) {
                        // Merge with existing
                        Object existing = result.get(name);
                        if (existing instanceof java.util.List<?> existingList) {
                            java.util.List<Object> merged = new java.util.ArrayList<>(existingList);
                            merged.addAll(items);
                            result.put(name, merged);
                        }
                    } else {
                        result.put(name, items);
                    }
                } else if (!nested.isEmpty()) {
                    result.put(name, nested);
                }
            } else {
                // Leaf node - extract text content
                String text = getElementText(elem);
                result.put(name, stripCDATA(text));
            }
        }
    }

    private String getElementText(org.w3c.dom.Element elem) {
        StringBuilder sb = new StringBuilder();
        org.w3c.dom.NodeList children = elem.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            org.w3c.dom.Node child = children.item(i);
            if (child.getNodeType() == org.w3c.dom.Node.TEXT_NODE || child.getNodeType() == org.w3c.dom.Node.CDATA_SECTION_NODE) {
                sb.append(child.getTextContent());
            }
        }
        return sb.toString().trim();
    }

    /**
     * Fallback: parse parameters as flat map using regex (for non-nested cases).
     */
    private java.util.Map<String, String> parseParametersAsFlatMap(String body) {
        java.util.Map<String, String> params = new java.util.LinkedHashMap<>();
        Matcher paramMatcher = PARAM_START.matcher(body);
        int pos = 0;
        while (paramMatcher.find(pos)) {
            String paramName = paramMatcher.group(1);
            int paramValueStart = paramMatcher.end();
            Matcher paramEndMatcher = PARAM_END.matcher(body);
            if (paramEndMatcher.find(paramValueStart)) {
                String value = stripCDATA(body.substring(paramValueStart, paramEndMatcher.start()));
                params.put(paramName, value);
                pos = paramEndMatcher.end();
            } else {
                String value = stripCDATA(body.substring(paramValueStart));
                params.put(paramName, value);
                pos = body.length();
            }
        }
        return params;
    }

    /**
     * Build JSON from structured map (supports nested objects and arrays).
     * Auto-parses JSON string values into structured objects.
     */
    private String buildStructuredJson(java.util.Map<String, Object> params) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
            // Pre-process: parse JSON string values into structured objects
            java.util.Map<String, Object> processed = new java.util.LinkedHashMap<>();
            for (var entry : params.entrySet()) {
                processed.put(entry.getKey(), unwrapJsonValue(entry.getValue()));
            }
            return om.writeValueAsString(processed);
        } catch (Exception e) {
            log.warn("[ToolCallStreamParser] Failed to build structured JSON: {}", e.getMessage());
            return "{}";
        }
    }

    /**
     * If a value is a JSON string (starts with { or [), parse it into a structured object.
     * Also converts numeric strings to numbers and boolean strings to booleans.
     */
    private Object unwrapJsonValue(Object value) {
        if (value instanceof String str) {
            String trimmed = str.trim();
            // JSON object or array
            if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
                try {
                    com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
                    return om.readValue(trimmed, Object.class);
                } catch (Exception e) {
                    // Not valid JSON, keep as string
                }
            }
            // Boolean
            if ("true".equalsIgnoreCase(trimmed)) return Boolean.TRUE;
            if ("false".equalsIgnoreCase(trimmed)) return Boolean.FALSE;
            if ("null".equalsIgnoreCase(trimmed)) return null;
            // Number conversion
            try {
                if (trimmed.contains(".")) {
                    return Double.parseDouble(trimmed);
                } else {
                    long longVal = Long.parseLong(trimmed);
                    // Fit into int if possible for cleaner JSON
                    if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
                        return (int) longVal;
                    }
                    return longVal;
                }
            } catch (NumberFormatException e) {
                // Not a number, keep as string
            }
        }
        return value;
    }

    private String buildArgumentsJson(String body) {
        java.util.Map<String, String> params = new java.util.LinkedHashMap<>();
        Matcher paramMatcher = PARAM_START.matcher(body);
        int pos = 0;
        while (paramMatcher.find(pos)) {
            String paramName = paramMatcher.group(1);
            int paramValueStart = paramMatcher.end();
            Matcher paramEndMatcher = PARAM_END.matcher(body);
            if (paramEndMatcher.find(paramValueStart)) {
                params.put(paramName, stripCDATA(body.substring(paramValueStart, paramEndMatcher.start())));
                pos = paramEndMatcher.end();
            } else {
                params.put(paramName, stripCDATA(body.substring(paramValueStart)));
                pos = body.length();
            }
        }
        return buildArgumentsJsonFromMap(params);
    }

    /**
     * Build JSON arguments with proper type inference.
     * Numbers and booleans are not quoted, strings are quoted.
     */
    private String buildArgumentsJsonFromMapTyped(java.util.Map<String, String> params) {
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (var entry : params.entrySet()) {
            if (!first) json.append(",");
            String key = entry.getKey();
            String value = entry.getValue();
            json.append("\"").append(escapeJson(key)).append("\":");
            json.append(inferJsonValue(value));
            first = false;
        }
        json.append("}");
        return json.toString();
    }

    /**
     * Infer JSON value type from string:
     * - "true"/"false" -> boolean
     * - Numeric string -> number
     * - JSON object/array -> as-is
     * - Otherwise -> quoted string
     */
    private String inferJsonValue(String value) {
        if (value == null || value.isEmpty()) {
            return "\"\"";
        }
        // Boolean
        if ("true".equalsIgnoreCase(value)) return "true";
        if ("false".equalsIgnoreCase(value)) return "false";
        // Null
        if ("null".equalsIgnoreCase(value)) return "null";
        // JSON object or array
        String trimmed = value.trim();
        if ((trimmed.startsWith("{") && trimmed.endsWith("}")) ||
            (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
            return trimmed; // Assume valid JSON
        }
        // Number check
        try {
            if (trimmed.contains(".")) {
                Double.parseDouble(trimmed);
                return trimmed;
            } else {
                Long.parseLong(trimmed);
                return trimmed;
            }
        } catch (NumberFormatException e) {
            // Not a number, fall through to string
        }
        // Default: quoted string
        return "\"" + escapeJson(value) + "\"";
    }

    private String buildArgumentsJsonFromMap(java.util.Map<String, String> params) {
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (var entry : params.entrySet()) {
            if (!first) json.append(",");
            json.append("\"").append(escapeJson(entry.getKey())).append("\":\"")
                .append(escapeJson(entry.getValue())).append("\"");
            first = false;
        }
        json.append("}");
        return json.toString();
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private static String stripCDATA(String s) {
        if (s == null) return "";
        String t = s.trim();
        if (t.startsWith("<![CDATA[")) t = t.substring(9);
        if (t.endsWith("]]>")) t = t.substring(0, t.length() - 3);
        return t;
    }

    private static int findSafeEnd(String text) {
        int lastLt = text.lastIndexOf('<');
        if (lastLt < 0) return text.length();
        String afterLt = text.substring(lastLt);
        if (afterLt.length() == 1) return lastLt;
        if (couldBePartialToolTag(afterLt)) return lastLt;
        return text.length();
    }

    private static boolean couldBePartialToolTag(String s) {
        if (s.isEmpty()) return false;
        String lower = s.toLowerCase();
        if (lower.startsWith("<|dsml|")) return true;
        if (lower.startsWith("<|dsm")) return true;
        if (lower.startsWith("<|ds")) return true;
        if (lower.startsWith("<|d")) return true;
        if (lower.startsWith("<|")) return true;
        if (lower.startsWith("<![cdata[")) return true;
        if (lower.startsWith("</")) return true;
        if (lower.equals("<")) return true;
        if (lower.equals("<!")) return true;
        return false;
    }
}
