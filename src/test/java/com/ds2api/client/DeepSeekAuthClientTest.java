package com.ds2api.client;

import com.ds2api.config.Ds2Config;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.client.reactive.MockClientHttpRequest;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeepSeekAuthClientTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void authClientUsesWebHeaders() {
        AtomicReference<ClientRequest> capturedRequest = new AtomicReference<>();
        ExchangeFunction exchange = request -> {
            capturedRequest.set(request);
            return Mono.just(ClientResponse.create(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body("""
                    {"data":{"biz_data":{"user":{"token":"fresh-token"}}}}
                    """)
                .build());
        };
        DeepSeekAuthClient client = new DeepSeekAuthClient(WebClient.builder().exchangeFunction(exchange),
            mapper);

        String token = client.login("user@example.com", null, "password", null,
            "Bdevice", null).block();

        assertEquals("fresh-token", token);
        var headers = capturedRequest.get().headers();
        assertEquals("*/*", headers.getFirst("Accept"));
        assertEquals("zh-CN,zh;q=0.9", headers.getFirst("Accept-Language"));
        assertEquals("https://chat.deepseek.com", headers.getFirst("Origin"));
        assertEquals("2.0.0", headers.getFirst("x-app-version"));
        assertEquals("zh_CN", headers.getFirst("x-client-locale"));
        assertEquals("web", headers.getFirst("x-client-platform"));
        assertEquals("28800", headers.getFirst("x-client-timezone-offset"));
        assertEquals("2.0.0", headers.getFirst("x-client-version"));
        assertTrue(headers.getFirst("User-Agent").startsWith("Mozilla/5.0"));
    }

    @Test
    void loginDerivesDeviceIdFromThumbcacheCookieAndPassesBrowserCookie() throws Exception {
        AtomicReference<ClientRequest> capturedRequest = new AtomicReference<>();
        ExchangeFunction exchange = request -> {
            capturedRequest.set(request);
            return Mono.just(ClientResponse.create(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body("""
                    {"data":{"biz_data":{"user":{"token":"fresh-token"}}}}
                    """)
                .build());
        };
        DeepSeekAuthClient client = new DeepSeekAuthClient(WebClient.builder().exchangeFunction(exchange),
            mapper);
        String cookie = "HWWAFSESTIME=1778827182420; "
            + ".thumbcache_6b2e5483f9d858d7c661c5e276b6a6ae=abc%2B123%3D%3D; "
            + "ds_session_id=session";

        client.login(null, "17665140425", "password", null, null, cookie).block();

        ClientRequest request = capturedRequest.get();
        assertEquals(cookie, request.headers().getFirst("Cookie"));
        assertEquals("https://chat.deepseek.com/sign_in", request.headers().getFirst("Referer"));

        JsonNode body = requestBody(request);
        assertEquals("17665140425", body.path("mobile").asText());
        assertEquals("+86", body.path("area_code").asText());
        assertEquals("Babc+123==", body.path("device_id").asText());
        assertEquals("web", body.path("os").asText());
    }

    @Test
    void explicitDeviceIdOverridesThumbcacheCookie() throws Exception {
        AtomicReference<ClientRequest> capturedRequest = new AtomicReference<>();
        ExchangeFunction exchange = request -> {
            capturedRequest.set(request);
            return Mono.just(ClientResponse.create(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body("""
                    {"data":{"biz_data":{"user":{"token":"fresh-token"}}}}
                    """)
                .build());
        };
        DeepSeekAuthClient client = new DeepSeekAuthClient(WebClient.builder().exchangeFunction(exchange),
            mapper);

        client.login(null, "17665140425", "password", "+86",
            "Bexplicit-device", ".thumbcache_x=ignored").block();

        JsonNode body = requestBody(capturedRequest.get());
        assertEquals("Bexplicit-device", body.path("device_id").asText());
    }

    @Test
    void loginFetchesDeviceIdFromDeviceProfilePayloadWhenNoCookieDeviceIdExists() throws Exception {
        List<ClientRequest> capturedRequests = new ArrayList<>();
        ExchangeFunction exchange = request -> {
            capturedRequests.add(request);
            if (request.url().getHost().equals("fp-it-acc.portal101.cn")) {
                return Mono.just(ClientResponse.create(HttpStatus.OK)
                    .header("Content-Type", "application/json")
                    .body("""
                        {"code":1100,"detail":{"c":0,"deviceId":"profile-device==","t":0}}
                        """)
                    .build());
            }
            return Mono.just(ClientResponse.create(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body("""
                    {"data":{"biz_data":{"user":{"token":"fresh-token"}}}}
                    """)
                .build());
        };
        DeepSeekAuthClient client = new DeepSeekAuthClient(WebClient.builder().exchangeFunction(exchange),
            mapper);
        Ds2Config.DeviceProfilePayload profilePayload = new Ds2Config.DeviceProfilePayload();
        profilePayload.setEp("ep-value");
        profilePayload.setData("data-value");

        String token = client.login(null, "17665140425", "password", "+86",
            null, null, profilePayload).block();

        assertEquals("fresh-token", token);
        assertEquals("fp-it-acc.portal101.cn", capturedRequests.get(0).url().getHost());
        assertEquals("/deviceprofile/v4", capturedRequests.get(0).url().getPath());
        JsonNode profileBody = requestBody(capturedRequests.get(0));
        assertEquals("default", profileBody.path("appId").asText());
        assertEquals("P9usCUBauxft8eAmUXaZ", profileBody.path("organization").asText());
        assertEquals("ep-value", profileBody.path("ep").asText());
        assertEquals("data-value", profileBody.path("data").asText());
        assertEquals(5, profileBody.path("encode").asInt());
        assertEquals(2, profileBody.path("compress").asInt());

        JsonNode loginBody = requestBody(capturedRequests.get(1));
        assertEquals("Bprofile-device==", loginBody.path("device_id").asText());
    }

    private JsonNode requestBody(ClientRequest request) throws Exception {
        MockClientHttpRequest httpRequest = new MockClientHttpRequest(request.method(),
            URI.create(request.url().getScheme() + "://" + request.url().getHost()
                + request.url().getPath()));
        request.body().insert(httpRequest, new org.springframework.web.reactive.function.BodyInserter.Context() {
            @Override
            public java.util.List<org.springframework.http.codec.HttpMessageWriter<?>> messageWriters() {
                return org.springframework.web.reactive.function.client.ExchangeStrategies.withDefaults()
                    .messageWriters();
            }

            @Override
            public java.util.Optional<org.springframework.http.server.reactive.ServerHttpRequest> serverRequest() {
                return java.util.Optional.empty();
            }

            @Override
            public java.util.Map<String, Object> hints() {
                return java.util.Map.of();
            }
        }).block();
        String json = httpRequest.getBody()
            .reduce(new DefaultDataBufferFactory().allocateBuffer(), (previous, current) -> {
                previous.write(current);
                return previous;
            })
            .map(buffer -> buffer.toString(java.nio.charset.StandardCharsets.UTF_8))
            .block();
        return mapper.readTree(json);
    }
}
