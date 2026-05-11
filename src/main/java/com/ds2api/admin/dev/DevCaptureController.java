package com.ds2api.admin.dev;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

/**
 * Dev packet capture REST endpoints under /admin/dev.
 * Requires X-Admin-Key header matching DS2API_ADMIN_KEY env/config value.
 */
@RestController
@RequestMapping("/admin/dev")
@RequiredArgsConstructor
public class DevCaptureController {

    private final PacketCaptureService captureService;
    private final JsonNodeFactory json = JsonNodeFactory.instance;

    @Value("${DS2API_ADMIN_KEY:}")
    private String adminKey;

    @GetMapping("/captures")
    public Mono<ObjectNode> listCaptures(
            @RequestHeader(value = "X-Admin-Key", required = false) String key) {
        validateAdminKey(key);
        ObjectNode resp = json.objectNode();
        resp.put("object", "list");
        var arr = resp.putArray("data");
        captureService.list().forEach(r -> arr.add(toNode(r)));
        return Mono.just(resp);
    }

    @DeleteMapping("/captures")
    public Mono<ResponseEntity<Void>> clearCaptures(
            @RequestHeader(value = "X-Admin-Key", required = false) String key) {
        validateAdminKey(key);
        captureService.clear();
        return Mono.just(ResponseEntity.ok().build());
    }

    @GetMapping("/raw-samples/query")
    public Mono<ObjectNode> querySamples(
            @RequestHeader(value = "X-Admin-Key", required = false) String key,
            @RequestParam String q,
            @RequestParam(defaultValue = "20") int limit) {
        validateAdminKey(key);
        ObjectNode resp = json.objectNode();
        resp.put("object", "list");
        var arr = resp.putArray("data");
        captureService.query(q, limit).forEach(r -> arr.add(toNode(r)));
        return Mono.just(resp);
    }

    @PostMapping("/raw-samples/save")
    public Mono<ResponseEntity<ObjectNode>> saveSample(
            @RequestHeader(value = "X-Admin-Key", required = false) String key,
            @RequestBody JsonNode body) {
        validateAdminKey(key);
        String captureId = body.path("capture_id").asText(null);
        String sampleId = body.path("sample_id").asText("saved_sample");

        if (captureId == null) {
            return Mono.error(
                    new ResponseStatusException(HttpStatus.BAD_REQUEST, "capture_id required"));
        }

        return Mono.justOrEmpty(captureService.getById(captureId))
                .map(rec -> {
                    ObjectNode resp = json.objectNode();
                    resp.put("saved", true)
                            .put("sample_id", sampleId)
                            .put("capture_id", captureId);
                    return ResponseEntity.ok(resp);
                })
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    private ObjectNode toNode(PacketCaptureService.CaptureRecord r) {
        return json.objectNode()
                .put("id", r.id())
                .put("chat_session_id", r.sessionId())
                .put("request_body", r.requestBody())
                .put("response_body", r.responseBody())
                .put("response_truncated", r.truncated())
                .put("timestamp", r.timestamp().getEpochSecond());
    }

    private void validateAdminKey(String providedKey) {
        if (adminKey == null || adminKey.isBlank() || !adminKey.equals(providedKey)) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid admin key");
        }
    }
}
