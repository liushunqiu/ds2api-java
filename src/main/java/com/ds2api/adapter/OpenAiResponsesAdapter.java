package com.ds2api.adapter;

import com.ds2api.model.InternalRequest;
import com.ds2api.model.InternalStreamEvent;
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
        List<InternalRequest.Message> messages = extractInputMessages(body);
        return Mono.just(new InternalRequest(model, messages, true, body.path("tools"), toolChoice));
    }

    private List<InternalRequest.Message> extractInputMessages(JsonNode body) {
        List<InternalRequest.Message> msgs = new ArrayList<>();
        JsonNode input = body.path("input");
        if (input.isTextual()) {
            msgs.add(new InternalRequest.Message("user", input.asText()));
        } else if (input.isArray()) {
            for (JsonNode node : input) {
                String role = node.path("role").asText("user");
                String type = node.path("type").asText("message");
                JsonNode contentNode = node.path("content");
                if ("message".equals(type)) {
                    if (contentNode.isTextual()) {
                        msgs.add(new InternalRequest.Message(role, contentNode.asText()));
                    } else if (contentNode.isArray()) {
                        StringBuilder sb = new StringBuilder();
                        for (JsonNode part : contentNode) {
                            if ("input_text".equals(part.path("type").asText())) {
                                sb.append(part.path("text").asText());
                            }
                        }
                        msgs.add(new InternalRequest.Message(role, sb.toString()));
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

        // 1. Mandatory first frame: response.created
        ServerSentEvent<String> createdEvent = sse("response.created", Map.of(
            "type", "response.created",
            "response", Map.of(
                "id", responseId,
                "status", "in_progress",
                "created_at", createdAt
            )
        ));

        ResponsesStreamState state = new ResponsesStreamState();

        Flux<ServerSentEvent<String>> bodyEvents = events.concatMap(
            event -> convertResponseEvent(event, responseId, state));

        // 2. Terminal frame: response.completed or response.failed
        Flux<ServerSentEvent<String>> tailEvent = Mono.fromSupplier(() -> {
            boolean isRequired = "required".equalsIgnoreCase(request.toolChoice());
            boolean hasToolCall = state.hasAnyToolCall();

            if (isRequired && !hasToolCall) {
                return sse("response.failed", Map.of(
                    "type", "response.failed",
                    "response_id", responseId,
                    "error", Map.of("message", "tool_choice=required but no tool call was generated", "code", 422)
                ));
            }
            return sse("response.completed", Map.of(
                "type", "response.completed",
                "response", Map.of("id", responseId, "status", "completed")
            ));
        }).flux();

        return Flux.concat(Flux.just(createdEvent), bodyEvents, tailEvent);
    }

    private Flux<ServerSentEvent<String>> convertResponseEvent(InternalStreamEvent event,
                                                                String respId,
                                                                ResponsesStreamState state) {
        List<ServerSentEvent<String>> out = new ArrayList<>(2);

        if (event instanceof InternalStreamEvent.TextDelta t) {
            if (!state.isTextItemCreated()) {
                out.add(sse("response.output_item.added", Map.of(
                    "type", "response.output_item.added",
                    "item_id", "msg_0",
                    "response_id", respId,
                    "item", Map.of("type", "message", "role", "assistant", "status", "in_progress")
                )));
                out.add(sse("response.content_part.added", Map.of(
                    "type", "response.content_part.added",
                    "item_id", "msg_0",
                    "response_id", respId,
                    "part", Map.of("type", "output_text")
                )));
                state.markTextItemCreated();
            }
            out.add(sse("response.content_part.delta", Map.of(
                "type", "response.content_part.delta",
                "item_id", "msg_0",
                "response_id", respId,
                "delta", t.chunk()
            )));
        } else if (event instanceof InternalStreamEvent.ToolCallStart s) {
            state.registerToolCall(s.callId());
            out.add(sse("response.output_item.added", Map.of(
                "type", "response.output_item.added",
                "item_id", s.callId(),
                "response_id", respId,
                "item", Map.of(
                    "type", "function_call",
                    "id", s.callId(),
                    "name", s.name(),
                    "status", "in_progress"
                )
            )));
        } else if (event instanceof InternalStreamEvent.ToolCallDelta d) {
            if (state.hasToolCall(d.callId())) {
                out.add(sse("response.function_call_arguments.delta", Map.of(
                    "type", "response.function_call_arguments.delta",
                    "item_id", d.callId(),
                    "response_id", respId,
                    "delta", d.argumentsDelta()
                )));
            }
        } else if (event instanceof InternalStreamEvent.ToolCallEnd e) {
            if (state.hasToolCall(e.callId())) {
                out.add(sse("response.output_item.done", Map.of(
                    "type", "response.output_item.done",
                    "item_id", e.callId(),
                    "response_id", respId,
                    "item", Map.of(
                        "type", "function_call",
                        "id", e.callId(),
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
        private final Set<String> activeToolCalls = new HashSet<>();

        void markTextItemCreated() { this.textItemCreated = true; }
        boolean isTextItemCreated() { return textItemCreated; }
        void registerToolCall(String id) { activeToolCalls.add(id); }
        boolean hasToolCall(String id) { return activeToolCalls.contains(id); }

        /** Whether any tool call has been seen during this stream. */
        boolean hasAnyToolCall() { return !activeToolCalls.isEmpty(); }
    }
}
