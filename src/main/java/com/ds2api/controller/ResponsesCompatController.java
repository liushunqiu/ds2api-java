package com.ds2api.controller;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Compatibility controller for clients that call /responses directly
 * without the /v1 prefix (e.g. Codex wire_api=responses).
 */
@RestController
@RequiredArgsConstructor
public class ResponsesCompatController {

    private final OpenAiController delegate;

    @PostMapping("/responses")
    public Flux<ServerSentEvent<String>> createResponse(
            @RequestBody JsonNode body, ServerWebExchange exchange) {
        return delegate.createResponse(body, exchange);
    }

    @GetMapping("/responses/{id}")
    public Mono<ResponseEntity<JsonNode>> getResponse(@PathVariable String id) {
        return delegate.getResponse(id);
    }
}
