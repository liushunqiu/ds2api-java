package com.ds2api.adapter;

import com.ds2api.model.InternalRequest;
import com.ds2api.model.InternalStreamEvent;
import com.ds2api.tool.DsmlToolFormatter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.*;

/**
 * OpenAI Responses API protocol adapter.
 * Follows the official Responses event lifecycle strictly:
 *
 *   response.created
 *     -> response.output_item.added (per output item)
 *       -> response.content_part.added (text) / function_call delta stream
 *         -> response.content_part.delta
 *       -> response.output_item.done
 *     -> response.completed (or response.failed on tool_choice=required violation)
 *
 * Per-subscription state machine (ResponsesStreamState) tracks
 * text-item-creation and active tool call registrations.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OpenAiResponsesAdapter implements ProtocolAdapter {

    private final ObjectMapper mapper;

    @Override
    public Mono<InternalRequest> normalizeRequest(JsonNode body) {
        String model = body.path("model").asText(InternalRequest.DEFAULT_MODEL);
        String toolChoice = body.path("tool_choice").asText("auto");

        // Support conversation_id for session continuation
        String conversationId = null;
        if (body.has("conversation_id")) {
            conversationId = body.get("conversation_id").asText(null);
        }

        List<InternalRequest.Message> messages = extractInputMessages(body);
        var passThrough = InternalRequest.extractPassThrough(body);
        return Mono.just(new InternalRequest(model, messages, true, body.path("tools"), toolChoice, conversationId, passThrough));
    }

    private List<InternalRequest.Message> extractInputMessages(JsonNode body) {
        List<InternalRequest.Message> msgs = new ArrayList<>();
        JsonNode input = body.path("input");
        if (input.isTextual()) {
            msgs.add(new InternalRequest.Message("user", input.asText()));
        } else if (input.isArray()) {
            for (JsonNode node : input) {
                String role = node.path("role").asText("");
                String type = node.path("type").asText("");

                switch (type) {
                    case "function_call", "tool_call" -> {
                        String name = node.path("name").asText("");
                        // Support both "arguments" and "input" fields (like Go version)
                        JsonNode argsNode = node.get("arguments");
                        if (argsNode == null) argsNode = node.get("input");
                        String args;
                        if (argsNode == null) {
                            args = "{}";
                        } else if (argsNode.isTextual()) {
                            args = argsNode.asText("{}");
                        } else {
                            args = argsNode.toString();
                        }
                        String callId = node.path("call_id").asText(node.path("id").asText(""));
                        if (name.isBlank()) continue;
                        ObjectNode toolCall = mapper.createObjectNode();
                        toolCall.put("id", callId);
                        toolCall.put("type", "function");
                        ObjectNode fn = toolCall.putObject("function");
                        fn.put("name", name);
                        fn.put("arguments", args);
                        ArrayNode toolCallsArr = mapper.createArrayNode();
                        toolCallsArr.add(toolCall);
                        String dsml = DsmlToolFormatter.convertToolCallsToDSML(toolCallsArr, mapper);
                        msgs.add(new InternalRequest.Message("assistant", dsml));
                    }
                    case "function_call_output", "tool_result" -> {
                        String output = node.path("output").asText(node.path("content").asText(""));
                        String callId = node.path("call_id").asText(node.path("tool_call_id").asText(""));
                        msgs.add(new InternalRequest.Message("tool", output));
                    }
                    default -> {
                        if ("message".equals(type) || type.isEmpty()) {
                            String msgRole = role.isEmpty() ? "user" : role;
                            JsonNode contentNode = node.path("content");
                            if (contentNode.isTextual()) {
                                msgs.add(new InternalRequest.Message(msgRole, contentNode.asText()));
                            } else if (contentNode.isArray()) {
                                StringBuilder sb = new StringBuilder();
                                for (JsonNode part : contentNode) {
                                    String partType = part.path("type").asText("");
                                    if ("input_text".equals(partType) || "output_text".equals(partType)) {
                                        sb.append(part.path("text").asText());
                                    }
                                }
                                if (!sb.isEmpty()) {
                                    msgs.add(new InternalRequest.Message(msgRole, sb.toString()));
                                }
                            }
                        }
                    }
                }
            }
        }
        return msgs;
    }

    @Override
    public Flux<ServerSentEvent<String>> toSse(Flux<InternalStreamEvent> events,
                                                InternalRequest request,
                                                String responseId, boolean stream) {
        long createdAt = Instant.now().getEpochSecond();

        ServerSentEvent<String> createdEvent = sse("response.created", Map.of(
            "type", "response.created",
            "response", Map.of(
                "id", responseId,
                "object", "response",
                "status", "in_progress",
                "created_at", createdAt,
                "model", request.model()
            )
        ));

        ResponsesStreamState state = new ResponsesStreamState();

        // Build tool name case mapping: lowercase -> original name from Codex tools definition
        Map<String, String> toolNameCaseMap = buildToolNameCaseMap(request.tools());
        // Build parameter name mapping per tool: toolName -> {aliasParamName -> actualParamName}
        Map<String, Map<String, String>> toolParamNameMap = buildToolParamNameMap(request.tools());
        log.info("[Responses] Tool name case map: {}", toolNameCaseMap);
        log.info("[Responses] Tool param name map: {}", toolParamNameMap);

        Flux<ServerSentEvent<String>> bodyEvents = events.concatMap(
            event -> convertResponseEvent(event, responseId, state, toolNameCaseMap, toolParamNameMap));

        // Terminal: text.done, part.done, output_item.done, completed
        Flux<ServerSentEvent<String>> tailEvents = Mono.fromSupplier(() -> {
            List<ServerSentEvent<String>> tail = new ArrayList<>();

            boolean isRequired = "required".equalsIgnoreCase(request.toolChoice());
            if (isRequired && !state.hasAnyToolCall()) {
                tail.add(sse("response.failed", Map.of(
                    "type", "response.failed",
                    "response_id", responseId,
                    "error", Map.of("message", "tool_choice=required but no tool call was generated", "code", 422)
                )));
                return tail;
            }

            // Close reasoning item if it was created but not closed
            if (state.isReasoningItemCreated() && !state.isReasoningItemClosed()) {
                String finalReasoning = state.getAccumulatedReasoning();
                tail.add(sse("response.reasoning_summary_text.done", Map.of(
                    "type", "response.reasoning_summary_text.done",
                    "item_id", "reasoning_0",
                    "response_id", responseId,
                    "output_index", 0,
                    "summary_index", 0,
                    "text", finalReasoning
                )));
                tail.add(sse("response.output_item.done", Map.of(
                    "type", "response.output_item.done",
                    "item_id", "reasoning_0",
                    "response_id", responseId,
                    "output_index", 0,
                    "item", Map.of(
                        "type", "reasoning", "id", "reasoning_0", "status", "completed",
                        "summary", List.of(Map.of("type", "summary_text", "text", finalReasoning))
                    )
                )));
                state.markReasoningItemClosed();
            }

            if (state.isTextItemCreated()) {
                String finalText = state.getAccumulatedText();
                int textOutputIndex = state.isReasoningItemCreated() ? 1 : 0;
                tail.add(sse("response.output_text.done", Map.of(
                    "type", "response.output_text.done",
                    "item_id", "msg_0",
                    "response_id", responseId,
                    "output_index", textOutputIndex,
                    "content_index", 0,
                    "text", finalText
                )));
                tail.add(sse("response.content_part.done", Map.of(
                    "type", "response.content_part.done",
                    "item_id", "msg_0",
                    "response_id", responseId,
                    "output_index", textOutputIndex,
                    "content_index", 0,
                    "part", Map.of("type", "output_text", "text", finalText)
                )));
                tail.add(sse("response.output_item.done", Map.of(
                    "type", "response.output_item.done",
                    "item_id", "msg_0",
                    "response_id", responseId,
                    "output_index", textOutputIndex,
                    "item", Map.of(
                        "type", "message", "id", "msg_0", "role", "assistant",
                        "status", "completed",
                        "content", List.of(Map.of("type", "output_text", "text", finalText))
                    )
                )));
            }

            tail.add(sse("response.completed", Map.of(
                "type", "response.completed",
                "response_id", responseId,
                "response", Map.of(
                    "id", responseId, "object", "response", "status", "completed", "model", request.model()
                )
            )));
            return tail;
        }).flux().flatMap(Flux::fromIterable);

        // [DONE] sentinel (empty data line)
        Flux<ServerSentEvent<String>> doneEvent = Flux.just(
            ServerSentEvent.builder((String) null).build()
        );

        return Flux.concat(Flux.just(createdEvent), bodyEvents, tailEvents, doneEvent);
    }

    private Flux<ServerSentEvent<String>> convertResponseEvent(InternalStreamEvent event,
                                                                String respId,
                                                                ResponsesStreamState state,
                                                                Map<String, String> toolNameCaseMap,
                                                                Map<String, Map<String, String>> toolParamNameMap) {
        List<ServerSentEvent<String>> out = new ArrayList<>(2);

        if (event instanceof InternalStreamEvent.ThinkingDelta td) {
            // Handle thinking/reasoning delta - emit as reasoning summary text
            if (!state.isReasoningItemCreated()) {
                out.add(sse("response.output_item.added", Map.of(
                    "type", "response.output_item.added",
                    "item_id", "reasoning_0",
                    "response_id", respId,
                    "output_index", 0,
                    "item", Map.of("type", "reasoning", "status", "in_progress")
                )));
                state.markReasoningItemCreated();
            }
            state.appendReasoning(td.chunk());
            out.add(sse("response.reasoning_summary_text.delta", Map.of(
                "type", "response.reasoning_summary_text.delta",
                "item_id", "reasoning_0",
                "response_id", respId,
                "output_index", 0,
                "summary_index", 0,
                "delta", td.chunk()
            )));
        } else if (event instanceof InternalStreamEvent.TextDelta t) {
            // Close reasoning item if it was open
            if (state.isReasoningItemCreated() && !state.isReasoningItemClosed()) {
                out.add(sse("response.reasoning_summary_text.done", Map.of(
                    "type", "response.reasoning_summary_text.done",
                    "item_id", "reasoning_0",
                    "response_id", respId,
                    "output_index", 0,
                    "summary_index", 0,
                    "text", state.getAccumulatedReasoning()
                )));
                out.add(sse("response.output_item.done", Map.of(
                    "type", "response.output_item.done",
                    "item_id", "reasoning_0",
                    "response_id", respId,
                    "output_index", 0,
                    "item", Map.of(
                        "type", "reasoning", "id", "reasoning_0", "status", "completed",
                        "summary", List.of(Map.of("type", "summary_text", "text", state.getAccumulatedReasoning()))
                    )
                )));
                state.markReasoningItemClosed();
            }

            if (!state.isTextItemCreated()) {
                out.add(sse("response.output_item.added", Map.of(
                    "type", "response.output_item.added",
                    "item_id", "msg_0",
                    "response_id", respId,
                    "output_index", 1,
                    "item", Map.of("type", "message", "role", "assistant", "status", "in_progress")
                )));
                out.add(sse("response.content_part.added", Map.of(
                    "type", "response.content_part.added",
                    "item_id", "msg_0",
                    "response_id", respId,
                    "output_index", 1,
                    "content_index", 0,
                    "part", Map.of("type", "output_text", "text", "")
                )));
                state.markTextItemCreated();
            }
            state.appendText(t.chunk());
            out.add(sse("response.output_text.delta", Map.of(
                "type", "response.output_text.delta",
                "item_id", "msg_0",
                "response_id", respId,
                "output_index", 1,
                "content_index", 0,
                "delta", t.chunk()
            )));
        } else if (event instanceof InternalStreamEvent.ToolCallStart s) {
            state.registerToolCall(s.callId());
            // Fix tool name: DeepSeek may hallucinate tool names not in Codex's definition
            String deepSeekName = s.name();
            String originalName = toolNameCaseMap.get(deepSeekName.toLowerCase());
            if (originalName == null) {
                originalName = toolNameCaseMap.getOrDefault("__default__", deepSeekName);
            }
            boolean wasRemapped = !deepSeekName.equals(originalName);
            state.setToolCallName(s.callId(), originalName);
            if (wasRemapped) {
                log.info("[Responses] >>> ToolCallStart: REMAPPED '{}' -> '{}' callId={} toolIndex={}",
                    deepSeekName, originalName, s.callId(), s.toolIndex());
            } else {
                log.info("[Responses] >>> ToolCallStart: name={} callId={} toolIndex={}",
                    originalName, s.callId(), s.toolIndex());
            }
            out.add(sse("response.output_item.added", Map.of(
                "type", "response.output_item.added",
                "item_id", s.callId(),
                "response_id", respId,
                "item", Map.of(
                    "type", "function_call",
                    "id", s.callId(),
                    "call_id", s.callId(),
                    "name", originalName,
                    "status", "in_progress"
                )
            )));
        } else if (event instanceof InternalStreamEvent.ToolCallDelta d) {
            if (state.hasToolCall(d.callId())) {
                String toolName = state.getToolCallName(d.callId());
                String fixedArgs = fixParameterNames(toolName, d.argumentsDelta(), toolParamNameMap);
                state.appendToolCallArguments(d.callId(), fixedArgs);
                log.info("[Responses] >>> ToolCallDelta: callId={} toolName={} delta={}", d.callId(), toolName,
                    fixedArgs.length() > 200 ? fixedArgs.substring(0, 200) + "..." : fixedArgs);
                out.add(sse("response.function_call_arguments.delta", Map.of(
                    "type", "response.function_call_arguments.delta",
                    "item_id", d.callId(),
                    "response_id", respId,
                    "delta", fixedArgs
                )));
            }
        } else if (event instanceof InternalStreamEvent.ToolCallEnd e) {
            if (state.hasToolCall(e.callId())) {
                String callId = e.callId();
                String name = state.getToolCallName(callId);
                String args = state.getToolCallArguments(callId);
                log.info("[Responses] >>> ToolCallEnd: name={} callId={} fullArgs={}", name, callId,
                    args != null && args.length() > 500 ? args.substring(0, 500) + "..." : args);
                out.add(sse("response.function_call_arguments.done", Map.of(
                    "type", "response.function_call_arguments.done",
                    "item_id", callId,
                    "response_id", respId,
                    "call_id", callId,
                    "name", name != null ? name : "",
                    "arguments", args != null ? args : "{}"
                )));
                out.add(sse("response.output_item.done", Map.of(
                    "type", "response.output_item.done",
                    "item_id", callId,
                    "response_id", respId,
                    "item", Map.of(
                        "type", "function_call",
                        "id", callId,
                        "call_id", callId,
                        "name", name != null ? name : "",
                        "arguments", args != null ? args : "{}",
                        "status", "completed"
                    )
                )));
            }
        } else if (event instanceof InternalStreamEvent.Finish f) {
            // response.completed/failed is emitted by the outer concat, not here
        } else if (event instanceof InternalStreamEvent.Error e) {
            out.add(sse("response.failed", Map.of(
                "type", "response.failed",
                "response_id", respId,
                "error", Map.of("message", e.message(), "code", e.code())
            )));
        }
        // SessionCreated: silently skip
        return Flux.fromIterable(out);
    }

    /**
     * Build a cached aggregated response for GET /v1/responses/{id}.
     */
    public JsonNode buildCachedResponse(String responseId, List<InternalStreamEvent> events) {
        ObjectNode resp = mapper.createObjectNode();
        resp.put("id", responseId);
        resp.put("object", "response");
        resp.put("status", "completed");
        resp.put("created_at", Instant.now().getEpochSecond());
        resp.put("model", "deepseek-v4-flash");

        ArrayNode output = resp.putArray("output");

        StringBuilder fullText = new StringBuilder();
        Map<String, ObjectNode> toolCallOutputs = new LinkedHashMap<>();

        for (InternalStreamEvent e : events) {
            if (e instanceof InternalStreamEvent.TextDelta t) {
                fullText.append(t.chunk());
            } else if (e instanceof InternalStreamEvent.ToolCallStart s) {
                ObjectNode tc = mapper.createObjectNode();
                tc.put("id", s.callId());
                tc.put("type", "function_call");
                tc.put("name", s.name());
                tc.put("status", "completed");
                tc.put("arguments", "");
                toolCallOutputs.put(s.callId(), tc);
            } else if (e instanceof InternalStreamEvent.ToolCallDelta d) {
                ObjectNode tc = toolCallOutputs.get(d.callId());
                if (tc != null) {
                    tc.put("arguments", tc.path("arguments").asText("") + d.argumentsDelta());
                }
            }
        }

        if (!fullText.isEmpty()) {
            output.addObject()
                .put("type", "message")
                .put("role", "assistant")
                .putArray("content").addObject()
                    .put("type", "output_text")
                    .put("text", fullText.toString());
        }

        for (ObjectNode tc : toolCallOutputs.values()) {
            output.add(tc);
        }

        return resp;
    }

    private ServerSentEvent<String> sse(String eventType, Object data) {
        try {
            String json = (data instanceof String s) ? s : mapper.writeValueAsString(data);
            return ServerSentEvent.builder(json).event(eventType).build();
        } catch (Exception e) {
            throw new RuntimeException("SSE serialize error", e);
        }
    }

    /**
     * Build parameter name mapping per tool from the tool schema.
     * Maps common aliases to actual parameter names.
     */
    private Map<String, Map<String, String>> buildToolParamNameMap(JsonNode tools) {
        Map<String, Map<String, String>> result = new HashMap<>();
        if (tools == null || !tools.isArray()) return result;

        for (JsonNode tool : tools) {
            String name = extractToolName(tool);
            if (name.isEmpty()) continue;

            JsonNode props = extractToolProperties(tool);
            if (props == null || !props.isObject()) continue;

            Map<String, String> paramMap = new HashMap<>();
            List<String> actualNames = new ArrayList<>();
            props.fieldNames().forEachRemaining(actualNames::add);

            // For each actual parameter name, add common aliases
            for (String paramName : actualNames) {
                paramMap.put(paramName.toLowerCase(), paramName);
                String lower = paramName.toLowerCase();

                // cmd <-> command
                if (lower.equals("cmd")) {
                    paramMap.put("command", paramName);
                } else if (lower.equals("command")) {
                    paramMap.put("cmd", paramName);
                }

                // file_path <-> path
                if (lower.equals("file_path") || lower.equals("filepath")) {
                    paramMap.put("path", paramName);
                    paramMap.put("file", paramName);
                } else if (lower.equals("path")) {
                    paramMap.put("file_path", paramName);
                    paramMap.put("filepath", paramName);
                }

                // content <-> text <-> data
                if (lower.equals("content")) {
                    paramMap.put("text", paramName);
                    paramMap.put("data", paramName);
                    paramMap.put("body", paramName);
                } else if (lower.equals("text")) {
                    paramMap.put("content", paramName);
                    paramMap.put("data", paramName);
                }

                // description <-> desc
                if (lower.equals("description")) {
                    paramMap.put("desc", paramName);
                } else if (lower.equals("desc")) {
                    paramMap.put("description", paramName);
                }
            }

            if (!paramMap.isEmpty()) {
                result.put(name, paramMap);
                result.put(name.toLowerCase(), paramMap);
                log.info("[Responses] Built param map for tool '{}': {}", name, paramMap);
            }
        }
        return result;
    }

    /**
     * Fix parameter names in the arguments JSON to match Codex's tool schema.
     * DeepSeek may output "command" but Codex expects "cmd", etc.
     */
    private String fixParameterNames(String toolName, String argsJson, Map<String, Map<String, String>> toolParamNameMap) {
        if (toolName == null || argsJson == null || argsJson.isBlank()) return argsJson;

        Map<String, String> paramMap = toolParamNameMap.get(toolName);
        if (paramMap == null) paramMap = toolParamNameMap.get(toolName.toLowerCase());
        log.info("[Responses] fixParameterNames: toolName='{}', paramMap={}, argsJson={}", toolName, paramMap,
            argsJson.length() > 200 ? argsJson.substring(0, 200) + "..." : argsJson);
        if (paramMap == null || paramMap.isEmpty()) return argsJson;

        try {
            JsonNode argsNode = mapper.readTree(argsJson);
            if (!argsNode.isObject()) return argsJson;

            ObjectNode fixed = mapper.createObjectNode();
            Iterator<Map.Entry<String, JsonNode>> fields = argsNode.fields();
            boolean changed = false;
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String paramName = entry.getKey();
                String mapped = paramMap.get(paramName.toLowerCase());
                if (mapped != null && !mapped.equals(paramName)) {
                    fixed.set(mapped, entry.getValue());
                    changed = true;
                    log.info("[Responses]   Param REMAPPED: '{}' -> '{}' for tool '{}'", paramName, mapped, toolName);
                } else {
                    fixed.set(paramName, entry.getValue());
                    if (mapped == null) {
                        log.info("[Responses]   Param NO MAPPING: '{}' for tool '{}' (available: {})", paramName, toolName, paramMap.keySet());
                    }
                }
            }
            String result = changed ? mapper.writeValueAsString(fixed) : argsJson;
            log.info("[Responses] fixParameterNames result: {}", result.length() > 200 ? result.substring(0, 200) + "..." : result);
            return result;
        } catch (Exception e) {
            log.warn("[Responses] Failed to fix parameter names: {}", e.getMessage());
            return argsJson;
        }
    }

    /**
     * Extract tool name from various Codex/OpenAI tool formats.
     */
    private String extractToolName(JsonNode tool) {
        String name = tool.path("name").asText("").trim();
        if (!name.isEmpty()) return name;
        name = tool.path("function").path("name").asText("").trim();
        return name;
    }

    /**
     * Extract tool properties schema from various formats.
     */
    private JsonNode extractToolProperties(JsonNode tool) {
        JsonNode schema = extractToolSchemaNode(tool);
        if (schema == null) return null;
        JsonNode props = schema.path("properties");
        if (!props.isMissingNode() && props.isObject()) return props;
        return null;
    }

    private JsonNode extractToolSchemaNode(JsonNode tool) {
        for (String key : new String[]{"parameters", "input_schema", "inputSchema", "schema"}) {
            JsonNode node = tool.path(key);
            if (!node.isMissingNode() && !node.isNull()) return node;
        }
        JsonNode func = tool.path("function");
        if (!func.isMissingNode()) {
            for (String key : new String[]{"parameters", "input_schema", "inputSchema", "schema"}) {
                JsonNode node = func.path(key);
                if (!node.isMissingNode() && !node.isNull()) return node;
            }
        }
        return null;
    }

    /**
     * Build a case-insensitive mapping from lowercase tool name to original tool name
     * as defined by Codex. DeepSeek may return lowercase tool names (e.g., "bash")
     * while Codex expects the original casing (e.g., "Bash").
     *
     * Also includes common aliases for DeepSeek hallucinated tool names.
     */
    private Map<String, String> buildToolNameCaseMap(JsonNode tools) {
        Map<String, String> map = new HashMap<>();
        List<String> originalNames = new ArrayList<>();
        if (tools == null || !tools.isArray()) return map;
        for (JsonNode tool : tools) {
            String name = extractToolName(tool);
            if (!name.isEmpty()) {
                map.put(name.toLowerCase(), name);
                originalNames.add(name);
            }
        }
        // Add common DeepSeek -> Codex aliases based on semantic matching
        // Find the primary "exec" tool (one that accepts command/cmd parameter)
        String execTool = null;
        for (String name : originalNames) {
            String lower = name.toLowerCase();
            if (lower.contains("exec") || lower.contains("command") || lower.contains("bash") || lower.contains("shell") || lower.contains("run")) {
                execTool = name;
                break;
            }
        }

        for (String name : originalNames) {
            String lower = name.toLowerCase();
            if (lower.contains("exec") || lower.contains("command") || lower.contains("bash") || lower.contains("shell") || lower.contains("run")) {
                map.putIfAbsent("bash", name);
                map.putIfAbsent("shell", name);
                map.putIfAbsent("terminal", name);
                map.putIfAbsent("run", name);
                map.putIfAbsent("execute", name);
                map.putIfAbsent("cmd", name);
            }
            if (lower.contains("read") || lower.contains("file") || lower.contains("view") || lower.contains("open") || lower.contains("cat")) {
                map.putIfAbsent("read_file", name);
                map.putIfAbsent("readfile", name);
                map.putIfAbsent("open_file", name);
                map.putIfAbsent("cat", name);
                map.putIfAbsent("read", name);
            }
            if (lower.contains("list") || lower.contains("dir") || lower.contains("ls") || lower.contains("glob") || lower.contains("find")) {
                map.putIfAbsent("list_files", name);
                map.putIfAbsent("listfiles", name);
                map.putIfAbsent("ls", name);
                map.putIfAbsent("find", name);
                map.putIfAbsent("glob", name);
                map.putIfAbsent("list", name);
            }
            if (lower.contains("write") || lower.contains("save") || lower.contains("create")) {
                map.putIfAbsent("write_file", name);
                map.putIfAbsent("writefile", name);
                map.putIfAbsent("save_file", name);
                map.putIfAbsent("create_file", name);
                map.putIfAbsent("write", name);
                map.putIfAbsent("write_stdin", name);
            }
            if (lower.contains("edit") || lower.contains("patch") || lower.contains("modify") || lower.contains("update")) {
                map.putIfAbsent("edit_file", name);
                map.putIfAbsent("apply_patch", name);
                map.putIfAbsent("patch", name);
                map.putIfAbsent("edit", name);
            }
            if (lower.contains("search") || lower.contains("grep") || lower.contains("find_text")) {
                map.putIfAbsent("search", name);
                map.putIfAbsent("grep", name);
                map.putIfAbsent("search_files", name);
            }
        }

        // Store execTool name as default fallback for any unknown tool
        if (execTool != null) {
            map.put("__default__", execTool);
        }
        return map;
    }

    /** Per-subscription state machine. */
    private static class ResponsesStreamState {
        private boolean textItemCreated = false;
        private boolean reasoningItemCreated = false;
        private boolean reasoningItemClosed = false;
        private final Set<String> activeToolCalls = new HashSet<>();
        private final Map<String, String> toolCallNames = new HashMap<>();
        private final Map<String, StringBuilder> toolCallArguments = new HashMap<>();
        private final StringBuilder accumulatedText = new StringBuilder();
        private final StringBuilder accumulatedReasoning = new StringBuilder();

        void markTextItemCreated() { this.textItemCreated = true; }
        boolean isTextItemCreated() { return textItemCreated; }
        void appendText(String chunk) { accumulatedText.append(chunk); }
        String getAccumulatedText() { return accumulatedText.toString(); }

        void markReasoningItemCreated() { this.reasoningItemCreated = true; }
        boolean isReasoningItemCreated() { return reasoningItemCreated; }
        void markReasoningItemClosed() { this.reasoningItemClosed = true; }
        boolean isReasoningItemClosed() { return reasoningItemClosed; }
        void appendReasoning(String chunk) { accumulatedReasoning.append(chunk); }
        String getAccumulatedReasoning() { return accumulatedReasoning.toString(); }

        void registerToolCall(String id) {
            activeToolCalls.add(id);
            toolCallArguments.put(id, new StringBuilder());
        }
        boolean hasToolCall(String id) { return activeToolCalls.contains(id); }
        boolean hasAnyToolCall() { return !activeToolCalls.isEmpty(); }

        void setToolCallName(String id, String name) { toolCallNames.put(id, name); }
        String getToolCallName(String id) { return toolCallNames.get(id); }

        void appendToolCallArguments(String id, String delta) {
            StringBuilder sb = toolCallArguments.get(id);
            if (sb != null) {
                sb.append(delta);
            }
        }
        String getToolCallArguments(String id) {
            StringBuilder sb = toolCallArguments.get(id);
            return sb != null ? sb.toString() : null;
        }
    }
}
