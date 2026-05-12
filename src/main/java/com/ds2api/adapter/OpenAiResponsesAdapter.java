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
                        String args = node.path("arguments").asText("{}");
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

        Flux<ServerSentEvent<String>> bodyEvents = events.concatMap(
            event -> convertResponseEvent(event, responseId, state));

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
                                                                ResponsesStreamState state) {
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
            state.setToolCallName(s.callId(), s.name());
            out.add(sse("response.output_item.added", Map.of(
                "type", "response.output_item.added",
                "item_id", s.callId(),
                "response_id", respId,
                "item", Map.of(
                    "type", "function_call",
                    "id", s.callId(),
                    "call_id", s.callId(),
                    "name", s.name(),
                    "status", "in_progress"
                )
            )));
        } else if (event instanceof InternalStreamEvent.ToolCallDelta d) {
            if (state.hasToolCall(d.callId())) {
                state.appendToolCallArguments(d.callId(), d.argumentsDelta());
                out.add(sse("response.function_call_arguments.delta", Map.of(
                    "type", "response.function_call_arguments.delta",
                    "item_id", d.callId(),
                    "response_id", respId,
                    "delta", d.argumentsDelta()
                )));
            }
        } else if (event instanceof InternalStreamEvent.ToolCallEnd e) {
            if (state.hasToolCall(e.callId())) {
                String callId = e.callId();
                String name = state.getToolCallName(callId);
                String args = state.getToolCallArguments(callId);
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
