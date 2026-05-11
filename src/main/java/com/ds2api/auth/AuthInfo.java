package com.ds2api.auth;

import org.springframework.lang.Nullable;

/**
 * Authentication result placed into exchange attributes.
 *
 * Modes:
 *   MANAGED  - key matched config.keys[] or config.api_keys[], proxy manages account pool.
 *   DIRECT   - token is a raw DeepSeek token, pass through directly.
 */
public record AuthInfo(
    Mode mode,
    @Nullable String token,     // raw DeepSeek token (only for DIRECT)
    @Nullable String keyName,   // friendly name of managed key
    @Nullable String targetAccount // from X-Ds2-Target-Account header
) {
    public enum Mode { MANAGED, DIRECT }

    public static AuthInfo managed(String keyName) {
        return new AuthInfo(Mode.MANAGED, null, keyName, null);
    }

    public static AuthInfo direct(String token) {
        return new AuthInfo(Mode.DIRECT, token, null, null);
    }

    public AuthInfo withTargetAccount(String account) {
        return new AuthInfo(mode, token, keyName, account);
    }

    /** Effective token to use when calling upstream. */
    public String effectiveToken() {
        if (mode == Mode.DIRECT) return token;
        return ""; // MANAGED mode: token comes from account pool
    }
}
