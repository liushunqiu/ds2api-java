package com.ds2api.adapter;

import com.ds2api.model.InternalRequest;
import com.ds2api.model.InternalStreamEvent;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Protocol adapter: translates between external API protocols (OpenAI Chat, Responses)
 * and the unified internal event model.
 */
public interface ProtocolAdapter {

    /** Normalize a raw JSON request body into the internal request model. */
    Mono<InternalRequest> normalizeRequest(JsonNode body);

    /** Convert internal events into protocol-specific SSE output. */
    Flux<ServerSentEvent<String>> toSse(Flux<InternalStreamEvent> events, InternalRequest request,
                                        String requestId, boolean stream);
}
