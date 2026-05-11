package com.ds2api.adapter;

import com.ds2api.model.InternalRequest;
import com.ds2api.model.InternalStreamEvent;
import com.ds2api.usage.UsageCalculator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * OpenAI Chat Completions protocol adapter.
 * Per-subscription state machine (ChatStreamState) ensures correct
 * tool_calls index assignment and incremental argument merging.
 *
 * P2: UsageCalculator integration for prompt/completion token counting.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OpenAiChatAdapter implements ProtocolAdapter {

    private final ObjectMapper mapper;

    @Override
    public Mono<InternalRequest> normalizeRequest(JsonNode body) {
        String model = body.path("model").asText(InternalRequest.DEFAULT_MODEL);
        boolean stream = body.path("stream").asBoolean(true);
        String toolChoice = body.path("tool_choice").asText("auto");

        List<InternalRequest.Message> messages;
        JsonNode msgsNode = body.path("messages");
        if (msgsNode.isArray()) {
            messages = new ArrayList<>();
            for (JsonNode msg : msgsNode) {
                String role = msg.path("role").asText("user");
                String content;
                JsonNode contentNode = msg.path("content");
                if (contentNode.isTextual()) {
                    content = contentNode.asText();
                } else if (contentNode.isArray()) {
                    StringBuilder sb = new StringBuilder();
                    for (JsonNode part : contentNode) {
                        if ("text".equals(part.path("type").asText())) {
                            sb.append(part.path("text").asText());
                        }
                    }
                    content = sb.toString();
                } else {
                    content = "";
                }
                messages.add(new InternalRequest.Message(role, content));
            }
        } else {
            messages = List.of();
        }

        JsonNode tools = body.path("tools");
        return Mono.just(new InternalRequest(model, messages, stream, tools, toolChoice));
    }

    @Override
    public Flux<ServerSentEvent<String>> toSse(Flux<InternalStreamEvent> events,
                                                InternalRequest request,
                                                String requestId, boolean stream) {
        if (!stream) {
            return events.collectList()
                .flatMapMany(list -> {
                    // Non-stream: validate tool_choice=required
                    if ("required".equalsIgnoreCase(request.toolChoice())) {
                        boolean hasToolCall = list.stream().anyMatch(
                            e -> e instanceof InternalStreamEvent.ToolCallStart);
                        if (!hasToolCall) {
                            return Flux.error(new ResponseStatusException(
                                HttpStatus.UNPROCESSABLE_ENTITY,
                                "tool_choice=required but no tool call was generated"));
                        }
                    }
                    return Flux.just(buildNonStreamResponse(list, request, requestId));
                })
                .onErrorResume(ResponseStatusException.class, e -> {
                    ObjectNode err = mapper.createObjectNode();
                    err.put("message", e.getReason());
                    err.put("code", e.getStatusCode().value());
                    return Flux.just(sse("error", err));
                });
        }

        long created = Instant.now().getEpochSecond();
        // Build prompt text for usage calculation
        String promptText = buildPromptText(request.messages());
        ChatStreamState state = new ChatStreamState(promptText);

        return events.concatMap(event -> convertStreamEvent(event, requestId, created, state))
                     .concatWith(Flux.just(sse(null, "[DONE]")));
    }

    private Flux<ServerSentEvent<String>> convertStreamEvent(InternalStreamEvent event,
                                                              String reqId, long created,
                                                              ChatStreamState state) {
        List<ServerSentEvent<String>> out = new ArrayList<>(1);
        ObjectNode chunk = mapper.createObjectNode();
        chunk.put("id", reqId);
        chunk.put("object", "chat.completion.chunk");
        chunk.put("created", created);
        chunk.put("model", "deepseek-v4-flash");

        ArrayNode choices = chunk.putArray("choices");
        ObjectNode choice = choices.addObject().put("index", 0);
        ObjectNode delta = choice.putObject("delta");

        if (event instanceof InternalStreamEvent.TextDelta t) {
            state.appendCompletionText(t.chunk());
            delta.put("content", t.chunk());
            choice.putNull("finish_reason");
            out.add(sse("chat.completion.chunk", chunk));
        } else if (event instanceof InternalStreamEvent.ToolCallStart s) {
            int idx = state.registerToolCall(s.callId());
            ObjectNode tc = delta.putArray("tool_calls").addObject();
            tc.put("index", idx);
            tc.put("id", s.callId());
            tc.put("type", "function");
            tc.putObject("function").put("name", s.name());
            choice.putNull("finish_reason");
            out.add(sse("chat.completion.chunk", chunk));
        } else if (event instanceof InternalStreamEvent.ToolCallDelta d) {
            Integer idx = state.getToolCallIndex(d.callId());
            if (idx != null) {
                ObjectNode tc = delta.putArray("tool_calls").addObject();
                tc.put("index", idx);
                tc.putObject("function").put("arguments", d.argumentsDelta());
                choice.putNull("finish_reason");
                out.add(sse("chat.completion.chunk", chunk));
            }
        } else if (event instanceof InternalStreamEvent.ToolCallEnd e) {
            // OpenAI Chat streaming does not emit an end event per tool
        } else if (event instanceof InternalStreamEvent.Finish f) {
            choice.put("finish_reason", f.reason().equals("tool_calls") ? "tool_calls" : "stop");
            // P2: inject usage in final chunk (OpenAI allows this)
            UsageCalculator.Usage usage = state.calculateUsage();
            chunk.putObject("usage")
                .put("prompt_tokens", usage.promptTokens())
                .put("completion_tokens", usage.completionTokens())
                .put("total_tokens", usage.totalTokens());
            out.add(sse("chat.completion.chunk", chunk));
        } else if (event instanceof InternalStreamEvent.Error e) {
            ObjectNode err = mapper.createObjectNode();
            err.put("message", e.message()).put("code", e.code());
            out.add(sse("error", err));
        }
        // SessionCreated and unknown: silently skip
        return Flux.fromIterable(out);
    }

    private ServerSentEvent<String> buildNonStreamResponse(List<InternalStreamEvent> events,
                                                            InternalRequest request,
                                                            String reqId) {
        ObjectNode resp = mapper.createObjectNode();
        resp.put("id", reqId);
        resp.put("object", "chat.completion");
        resp.put("created", Instant.now().getEpochSecond());
        resp.put("model", "deepseek-v4-flash");

        StringBuilder content = new StringBuilder();
        Map<String, NonStreamToolCall> toolCalls = new LinkedHashMap<>();
        String finishReason = "stop";

        for (InternalStreamEvent e : events) {
            if (e instanceof InternalStreamEvent.TextDelta t) {
                content.append(t.chunk());
            } else if (e instanceof InternalStreamEvent.ToolCallStart s) {
                toolCalls.put(s.callId(), new NonStreamToolCall(s.callId(), s.name(), new StringBuilder()));
            } else if (e instanceof InternalStreamEvent.ToolCallDelta d) {
                NonStreamToolCall tc = toolCalls.get(d.callId());
                if (tc != null) tc.args.append(d.argumentsDelta());
            } else if (e instanceof InternalStreamEvent.Finish f) {
                finishReason = f.reason().equals("tool_calls") ? "tool_calls" : "stop";
            }
        }

        ArrayNode choices = resp.putArray("choices");
        ObjectNode choice = choices.addObject().put("index", 0).put("finish_reason", finishReason);
        ObjectNode msg = choice.putObject("message").put("role", "assistant");

        if (!toolCalls.isEmpty()) {
            msg.putNull("content");
            ArrayNode tcArr = msg.putArray("tool_calls");
            toolCalls.values().forEach(tc -> {
                ObjectNode node = tcArr.addObject();
                node.put("id", tc.id).put("type", "function");
                node.putObject("function").put("name", tc.name).put("arguments", tc.args.toString());
            });
        } else {
            msg.put("content", content.toString());
        }

        // P2: calculate actual token usage
        String promptText = buildPromptText(request.messages());
        UsageCalculator.Usage usage = UsageCalculator.calculate(promptText, content.toString());
        resp.putObject("usage")
            .put("prompt_tokens", usage.promptTokens())
            .put("completion_tokens", usage.completionTokens())
            .put("total_tokens", usage.totalTokens());
        return sse(null, resp);
    }

    /**
     * Build a concatenated prompt text from request messages for token estimation.
     */
    private String buildPromptText(List<InternalRequest.Message> messages) {
        StringBuilder sb = new StringBuilder();
        for (InternalRequest.Message m : messages) {
            if (m.content() != null) {
                sb.append(m.content());
            }
        }
        return sb.toString();
    }

    private ServerSentEvent<String> sse(String eventType, Object data) {
        try {
            String json = (data instanceof String s) ? s : mapper.writeValueAsString(data);
            return ServerSentEvent.builder(json).event(eventType).build();
        } catch (Exception e) {
            throw new RuntimeException("SSE serialize error", e);
        }
    }

    /** Per-subscription state machine -- no shared mutable state across requests. */
    private static class ChatStreamState {
        private final Map<String, Integer> toolCallIndices = new HashMap<>();
        private final AtomicInteger nextIndex = new AtomicInteger(0);
        private final String promptText;
        private final StringBuilder completionText = new StringBuilder();

        ChatStreamState(String promptText) {
            this.promptText = promptText;
        }

        int registerToolCall(String callId) {
            return toolCallIndices.computeIfAbsent(callId, k -> nextIndex.getAndIncrement());
        }

        Integer getToolCallIndex(String callId) {
            return toolCallIndices.get(callId);
        }

        void appendCompletionText(String chunk) {
            completionText.append(chunk);
        }

        UsageCalculator.Usage calculateUsage() {
            return UsageCalculator.calculate(promptText, completionText.toString());
        }
    }

    private record NonStreamToolCall(String id, String name, StringBuilder args) {}
}
