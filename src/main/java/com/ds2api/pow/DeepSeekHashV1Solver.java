package com.ds2api.pow;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DeepSeekHashV1 PoW solver using SHA-256.
 * Difficulty unit: leading zero hex characters (1 hex char = 4 bits).
 * Aligned with ds2api internal/pow/deepseek_hash_v1.go.
 */
public final class DeepSeekHashV1Solver {

    private static final HexFormat HEX = HexFormat.of();
    private static final ThreadLocal<MessageDigest> SHA256 = ThreadLocal.withInitial(() -> {
        try { return MessageDigest.getInstance("SHA-256"); } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not supported", e);
        }
    });

    // Parses "challenge=...;salt=...;difficulty=4"
    private static final Pattern POW_HEADER_PATTERN =
            Pattern.compile("challenge=([^;]+);salt=([^;]+);difficulty=(\\d+)");

    public DeepSeekHashV1Solver() {}

    public record PowChallenge(String challenge, String salt, int difficulty) {}
    public record PowResult(long nonce, String hashHex, String token) {}

    /**
     * Parse the x-ds-pow response header into a structured challenge.
     */
    public static PowChallenge parseChallenge(String powHeader) {
        if (powHeader == null || powHeader.isBlank()) return null;
        Matcher m = POW_HEADER_PATTERN.matcher(powHeader);
        if (!m.find())
            throw new IllegalArgumentException("Invalid x-ds-pow header: " + powHeader);
        return new PowChallenge(m.group(1), m.group(2), Integer.parseInt(m.group(3)));
    }

    /**
     * Solve the PoW by finding a nonce whose SHA-256 hash has enough leading zero hex chars.
     * @return result with nonce and "nonce:hashHex" token for x-ds-pow-response
     */
    public PowResult solve(String challenge, String salt, int difficulty) {
        byte[] saltBytes = salt.getBytes(StandardCharsets.UTF_8);
        byte[] challengeBytes = challenge.getBytes(StandardCharsets.UTF_8);
        int zeroHexChars = difficulty;
        int zeroBytes = zeroHexChars / 2;
        boolean hasHalfByte = (zeroHexChars % 2) != 0;
        byte halfByteMask = (byte) 0xF0;

        long nonce = 0;
        MessageDigest md = SHA256.get();

        while (true) {
            byte[] nonceBytes = String.valueOf(nonce).getBytes(StandardCharsets.UTF_8);
            md.update(saltBytes);
            md.update(challengeBytes);
            md.update(nonceBytes);
            byte[] hash = md.digest();

            if (checkLeadingZeros(hash, zeroBytes, hasHalfByte, halfByteMask)) {
                String hashHex = HEX.formatHex(hash);
                return new PowResult(nonce, hashHex, nonce + ":" + hashHex);
            }
            nonce++;
        }
    }

    private static boolean checkLeadingZeros(byte[] hash, int zeroBytes,
                                             boolean hasHalfByte, byte mask) {
        for (int i = 0; i < zeroBytes; i++) {
            if (hash[i] != 0) return false;
        }
        return !hasHalfByte || (hash[zeroBytes] & mask) == 0;
    }

    /** Verify utility for tests. */
    public boolean verify(String challenge, String salt, long nonce, int difficulty) {
        byte[] data = (salt + challenge + nonce).getBytes(StandardCharsets.UTF_8);
        byte[] hash = SHA256.get().digest(data);
        int zeroBytes = difficulty / 2;
        boolean hasHalfByte = (difficulty % 2) != 0;
        return checkLeadingZeros(hash, zeroBytes, hasHalfByte, (byte) 0xF0);
    }
}
