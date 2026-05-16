package com.ds2api.config;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.function.client.WebClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WebClientConfigTest {

    @Test
    void deepSeekWebClientUsesWebHeadersForPowRequests() {
        Ds2ApiProperties properties = new Ds2ApiProperties();
        WebClient client = new WebClientConfig().deepSeekWebClient(properties);

        HttpHeaders headers = (HttpHeaders) ReflectionTestUtils.getField(client, "defaultHeaders");

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
}
