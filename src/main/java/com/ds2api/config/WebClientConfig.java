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
                .defaultHeader("Host", "chat.deepseek.com")
                .defaultHeader("Accept", "application/json")
                .defaultHeader("Content-Type", "application/json")
                .defaultHeader("accept-charset", "UTF-8")
                .defaultHeader("User-Agent", "DeepSeek/2.0.4 Android/35")
                .defaultHeader("x-client-platform", "android")
                .defaultHeader("x-client-version", "2.0.4")
                .defaultHeader("x-client-locale", "zh_CN")
                .build();
    }
}
