package com.ds2api.filter;

import org.reactivestreams.Subscription;
import org.slf4j.MDC;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;

import java.util.UUID;

/**
 * Assigns a unique request ID to every inbound HTTP request.
 * Propagates X-Request-ID through Reactor Context and bridges to Logback MDC.
 */
@Component
@Order(-100) // Run before all other filters
public class RequestIdFilter implements WebFilter {

    static {
        Hooks.onEachOperator("mdc",
                Operators.lift((scannable, coreSubscriber) -> new MdcLifter<>(coreSubscriber)));
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String incoming = exchange.getRequest().getHeaders().getFirst("X-Request-ID");
        final String reqId = (incoming != null && !incoming.isBlank())
                ? incoming
                : UUID.randomUUID().toString().replace("-", "");

        exchange.getResponse().getHeaders().set("X-Request-ID", reqId);

        return chain.filter(exchange)
                .contextWrite(ctx -> ctx.put("requestId", reqId));
    }

    /**
     * Bridges Reactor Context "requestId" into Logback MDC on each signal.
     * Implements CoreSubscriber to satisfy Operators.lift type contract.
     */
    private static class MdcLifter<T> implements CoreSubscriber<T> {
        private final CoreSubscriber<? super T> delegate;

        MdcLifter(CoreSubscriber<? super T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onSubscribe(Subscription s) {
            delegate.onSubscribe(s);
        }

        @Override
        public void onNext(T t) {
            delegate.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            delegate.onError(t);
        }

        @Override
        public void onComplete() {
            delegate.onComplete();
        }
    }
}
