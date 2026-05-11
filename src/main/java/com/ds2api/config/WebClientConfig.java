package com.ds2api.config;

import com.ds2api.client.DeepSeekPowRetryFilter;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Configuration
public class WebClientConfig {

    @Bean
    public WebClient deepSeekWebClient(Ds2ApiProperties properties) {
        HttpClient httpClient = HttpClient.create()
                .responseTimeout(Duration.ofSeconds(60))
                .keepAlive(true)
                .doOnConnected(conn ->
                        conn.addHandlerLast(new ReadTimeoutHandler(60, TimeUnit.SECONDS))
                            .addHandlerLast(new WriteTimeoutHandler(30, TimeUnit.SECONDS)));

        return WebClient.builder()
                .baseUrl(properties.getUpstream().getBaseUrl())
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .filter(new DeepSeekPowRetryFilter())
                .defaultHeader("User-Agent",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
                .build();
    }
}
