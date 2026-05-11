package com.ds2api.runtime;

import org.springframework.http.HttpStatusCode;

/** Exception representing an upstream DeepSeek API error. */
public class UpstreamException extends RuntimeException {
    private final HttpStatusCode statusCode;

    public UpstreamException(HttpStatusCode statusCode, String body) {
        super("Upstream error " + statusCode.value() + ": " + body);
        this.statusCode = statusCode;
    }

    public HttpStatusCode getStatusCode() {
        return statusCode;
    }
}
