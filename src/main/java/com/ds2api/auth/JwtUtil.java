package com.ds2api.auth;

import com.ds2api.config.ConfigLoaderService;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * JWT utility for admin authentication.
 * Mirrors the Go implementation in session-proxy/internal/auth/jwt.go.
 *
 * Uses HS256 (HMAC-SHA256) with the admin_key from config.json.
 */
public final class JwtUtil {

    private static final String ALG = "HmacSHA256";
    private static final int DEFAULT_EXPIRE_HOURS = 24;
    private static final Base64.Encoder B64 = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder B64D = Base64.getUrlDecoder();

    private JwtUtil() {}

    /**
     * Create a signed JWT with the given admin key.
     */
    public static String createJWT(String adminKey, int expireHours) {
        if (expireHours <= 0) expireHours = DEFAULT_EXPIRE_HOURS;
        long now = System.currentTimeMillis() / 1000;
        long exp = now + expireHours * 3600L;

        String headerB64 = b64encode("{\"alg\":\"HS256\",\"typ\":\"JWT\"}");
        String payloadB64 = b64encode(
            "{\"iat\":" + now + ",\"exp\":" + exp + ",\"type\":\"admin\"}");
        String signingInput = headerB64 + "." + payloadB64;
        String sig = sign(signingInput, adminKey);
        return signingInput + "." + sig;
    }

    /**
     * Verify a JWT token and return its payload claims.
     */
    public static Map<String, Object> verifyJWT(String token, String adminKey) {
        String[] parts = token.split("\\.");
        if (parts.length != 3) {
            throw new JwtException("invalid token format");
        }
        String signingInput = parts[0] + "." + parts[1];
        String expectedSig = sign(signingInput, adminKey);
        if (!constantTimeEquals(expectedSig, parts[2])) {
            throw new JwtException("invalid token signature");
        }
        byte[] payloadBytes = b64decode(parts[1]);
        String payloadStr = new String(payloadBytes, StandardCharsets.UTF_8);
        Map<String, Object> claims = parseJson(payloadStr);
        Number exp = (Number) claims.get("exp");
        if (exp == null || exp.longValue() < System.currentTimeMillis() / 1000) {
            throw new JwtException("token expired");
        }
        return claims;
    }

    private static String sign(String input, String key) {
        try {
            Mac mac = Mac.getInstance(ALG);
            SecretKeySpec spec = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), ALG);
            mac.init(spec);
            byte[] sig = mac.doFinal(input.getBytes(StandardCharsets.UTF_8));
            return B64.encodeToString(sig);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException("JWT signing failed", e);
        }
    }

    private static String b64encode(String s) {
        return B64.encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] b64decode(String s) {
        return B64D.decode(s);
    }

    private static Map<String, Object> parseJson(String json) {
        // Lightweight JSON parser for simple flat payload
        Map<String, Object> map = new LinkedHashMap<>();
        String stripped = json.trim();
        if (!stripped.startsWith("{") || !stripped.endsWith("}")) return map;
        String inner = stripped.substring(1, stripped.length() - 1);
        for (String pair : inner.split(",")) {
            String[] kv = pair.split(":", 2);
            if (kv.length != 2) continue;
            String key = kv[0].trim().replaceAll("^\"|\"$", "");
            String val = kv[1].trim();
            if (val.startsWith("\"")) {
                map.put(key, val.replaceAll("^\"|\"$", ""));
            } else if (val.equals("true") || val.equals("false")) {
                map.put(key, Boolean.parseBoolean(val));
            } else if (val.contains(".")) {
                map.put(key, Double.parseDouble(val));
            } else {
                map.put(key, Long.parseLong(val));
            }
        }
        return map;
    }

    private static boolean constantTimeEquals(String a, String b) {
        if (a.length() != b.length()) return false;
        int result = 0;
        for (int i = 0; i < a.length(); i++) {
            result |= a.charAt(i) ^ b.charAt(i);
        }
        return result == 0;
    }

    public static class JwtException extends RuntimeException {
        public JwtException(String message) { super(message); }
    }
}
