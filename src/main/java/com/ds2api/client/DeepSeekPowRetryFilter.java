package com.ds2api.client;

import com.ds2api.pow.DeepSeekHashV1Solver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import reactor.core.publisher.Mono;

/**
 * Reactive ExchangeFilterFunction that transparently handles DeepSeek PoW challenges.
 *
 * Flow:
 * 1. Request sent normally
 * 2. If upstream returns x-ds-pow header and request has no x-ds-pow-response yet,
 *    solve the challenge and retry exactly once.
 * 3. On retry failure, propagate the original error.
 */
public final class DeepSeekPowRetryFilter implements ExchangeFilterFunction {

    private static final Logger log = LoggerFactory.getLogger(DeepSeekPowRetryFilter.class);

    private static final String POW_HEADER = "x-ds-pow";
    private static final String POW_RESPONSE_HEADER = "x-ds-pow-response";

    private final DeepSeekHashV1Solver solver = new DeepSeekHashV1Solver();

    @Override
    public Mono<ClientResponse> filter(ClientRequest request, ExchangeFunction next) {
        return next.exchange(request)
            .flatMap(response -> {
                String powChallenge = response.headers().header(POW_HEADER)
                        .stream().findFirst().orElse(null);

                if (powChallenge != null && !request.headers().containsKey(POW_RESPONSE_HEADER)) {
                    log.debug("[DeepSeek] PoW challenge received: {}", powChallenge);
                    return handlePowChallenge(request, next, powChallenge);
                }
                return Mono.just(response);
            });
    }

    private Mono<ClientResponse> handlePowChallenge(ClientRequest originalRequest,
                                                     ExchangeFunction next,
                                                     String challengeHeader) {
        try {
            DeepSeekHashV1Solver.PowChallenge challenge =
                    DeepSeekHashV1Solver.parseChallenge(challengeHeader);
            long start = System.currentTimeMillis();
            DeepSeekHashV1Solver.PowResult result =
                    solver.solve(challenge.challenge(), challenge.salt(), challenge.difficulty());
            log.info("[DeepSeek] PoW solved in {}ms (nonce={}, difficulty={})",
                    System.currentTimeMillis() - start, result.nonce(), challenge.difficulty());

            ClientRequest retryRequest = ClientRequest.from(originalRequest)
                    .headers(headers -> headers.set(POW_RESPONSE_HEADER, result.token()))
                    .build();

            return next.exchange(retryRequest);
        } catch (Exception e) {
            log.error("[DeepSeek] PoW solve failed", e);
            return Mono.error(new RuntimeException("PoW calculation failed", e));
        }
    }
}
