package com.ds2api.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import reactor.core.publisher.Mono;

/**
 * PoW is now handled proactively by DeepSeekPowClient.
 * This filter is kept as a no-op pass-through for backward compatibility.
 */
public final class DeepSeekPowRetryFilter implements ExchangeFilterFunction {

    private static final Logger log = LoggerFactory.getLogger(DeepSeekPowRetryFilter.class);

    @Override
    public Mono<ClientResponse> filter(ClientRequest request, ExchangeFunction next) {
        return next.exchange(request);
    }
}
