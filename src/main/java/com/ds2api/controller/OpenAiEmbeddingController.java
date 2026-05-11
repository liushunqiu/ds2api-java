package com.ds2api.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Placeholder embeddings endpoint.
 * DeepSeek Web does not offer an embeddings API. This returns a fixed
 * zero-vector response to prevent SDK 404 errors during capability probing.
 */
@RestController
@RequestMapping("/v1")
public class OpenAiEmbeddingController {

    private final JsonNodeFactory json = JsonNodeFactory.instance;

    @PostMapping("/embeddings")
    public Mono<ObjectNode> createEmbedding(@RequestBody JsonNode body) {
        int dims = body.path("dimensions").asInt(1536);
        ArrayNode vector = json.arrayNode();
        for (int i = 0; i < dims; i++) {
            vector.add(0.0);
        }

        ObjectNode data = json.objectNode()
            .put("object", "embedding")
            .put("index", 0);
        data.set("embedding", vector);

        ObjectNode usage = json.objectNode()
            .put("prompt_tokens", 0)
            .put("total_tokens", 0);

        ObjectNode response = json.objectNode()
            .put("object", "list");
        response.set("data", json.arrayNode().add(data));
        response.set("usage", usage);

        return Mono.just(response);
    }
}
