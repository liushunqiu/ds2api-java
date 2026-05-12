package com.ds2api.pow;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

/**
 * DeepSeekHashV1 PoW solver.
 * DeepSeekHashV1 = SHA3-256 but skipping Keccak-f[1600] round 0 (only rounds 1..23).
 * Aligned with ds2api internal/pow/deepseek_hash.go and deepseek_pow.go.
 */
public final class DeepSeekHashV1Solver {

    private static final HexFormat HEX = HexFormat.of();

    private static final long[] RC = {
        0x0000000000000001L, 0x0000000000008082L, 0x800000000000808AL, 0x8000000080008000L,
        0x000000000000808BL, 0x0000000080000001L, 0x8000000080008081L, 0x8000000000008009L,
        0x000000000000008AL, 0x0000000000000088L, 0x0000000080008009L, 0x000000008000000AL,
        0x000000008000808BL, 0x800000000000008BL, 0x8000000000008089L, 0x8000000000008003L,
        0x8000000000008002L, 0x8000000000000080L, 0x000000000000800AL, 0x800000008000000AL,
        0x8000000080008081L, 0x8000000000008080L, 0x0000000080000001L, 0x8000000080008008L,
    };

    public DeepSeekHashV1Solver() {}

    /**
     * Keccak-f[1600] with only rounds 1..23 (skipping round 0).
     */
    private static void keccakF23(long[] s) {
        long a0 = s[0], a1 = s[1], a2 = s[2], a3 = s[3], a4 = s[4];
        long a5 = s[5], a6 = s[6], a7 = s[7], a8 = s[8], a9 = s[9];
        long a10 = s[10], a11 = s[11], a12 = s[12], a13 = s[13], a14 = s[14];
        long a15 = s[15], a16 = s[16], a17 = s[17], a18 = s[18], a19 = s[19];
        long a20 = s[20], a21 = s[21], a22 = s[22], a23 = s[23], a24 = s[24];

        for (int r = 1; r < 24; r++) {
            long c0 = a0 ^ a5 ^ a10 ^ a15 ^ a20;
            long c1 = a1 ^ a6 ^ a11 ^ a16 ^ a21;
            long c2 = a2 ^ a7 ^ a12 ^ a17 ^ a22;
            long c3 = a3 ^ a8 ^ a13 ^ a18 ^ a23;
            long c4 = a4 ^ a9 ^ a14 ^ a19 ^ a24;
            long d0 = c4 ^ Long.rotateLeft(c1, 1);
            long d1 = c0 ^ Long.rotateLeft(c2, 1);
            long d2 = c1 ^ Long.rotateLeft(c3, 1);
            long d3 = c2 ^ Long.rotateLeft(c4, 1);
            long d4 = c3 ^ Long.rotateLeft(c0, 1);
            a0 ^= d0; a5 ^= d0; a10 ^= d0; a15 ^= d0; a20 ^= d0;
            a1 ^= d1; a6 ^= d1; a11 ^= d1; a16 ^= d1; a21 ^= d1;
            a2 ^= d2; a7 ^= d2; a12 ^= d2; a17 ^= d2; a22 ^= d2;
            a3 ^= d3; a8 ^= d3; a13 ^= d3; a18 ^= d3; a23 ^= d3;
            a4 ^= d4; a9 ^= d4; a14 ^= d4; a19 ^= d4; a24 ^= d4;

            long b0 = a0;
            long b10 = Long.rotateLeft(a1, 1);
            long b20 = Long.rotateLeft(a2, 62);
            long b5 = Long.rotateLeft(a3, 28);
            long b15 = Long.rotateLeft(a4, 27);
            long b16 = Long.rotateLeft(a5, 36);
            long b1 = Long.rotateLeft(a6, 44);
            long b11 = Long.rotateLeft(a7, 6);
            long b21 = Long.rotateLeft(a8, 55);
            long b6 = Long.rotateLeft(a9, 20);
            long b7 = Long.rotateLeft(a10, 3);
            long b17 = Long.rotateLeft(a11, 10);
            long b2 = Long.rotateLeft(a12, 43);
            long b12 = Long.rotateLeft(a13, 25);
            long b22 = Long.rotateLeft(a14, 39);
            long b23 = Long.rotateLeft(a15, 41);
            long b8 = Long.rotateLeft(a16, 45);
            long b18 = Long.rotateLeft(a17, 15);
            long b3 = Long.rotateLeft(a18, 21);
            long b13 = Long.rotateLeft(a19, 8);
            long b14 = Long.rotateLeft(a20, 18);
            long b24 = Long.rotateLeft(a21, 2);
            long b9 = Long.rotateLeft(a22, 61);
            long b19 = Long.rotateLeft(a23, 56);
            long b4 = Long.rotateLeft(a24, 14);

            a0 = b0 ^ (~b1 & b2); a1 = b1 ^ (~b2 & b3); a2 = b2 ^ (~b3 & b4);
            a3 = b3 ^ (~b4 & b0); a4 = b4 ^ (~b0 & b1);
            a5 = b5 ^ (~b6 & b7); a6 = b6 ^ (~b7 & b8); a7 = b7 ^ (~b8 & b9);
            a8 = b8 ^ (~b9 & b5); a9 = b9 ^ (~b5 & b6);
            a10 = b10 ^ (~b11 & b12); a11 = b11 ^ (~b12 & b13); a12 = b12 ^ (~b13 & b14);
            a13 = b13 ^ (~b14 & b10); a14 = b14 ^ (~b10 & b11);
            a15 = b15 ^ (~b16 & b17); a16 = b16 ^ (~b17 & b18); a17 = b17 ^ (~b18 & b19);
            a18 = b18 ^ (~b19 & b15); a19 = b19 ^ (~b15 & b16);
            a20 = b20 ^ (~b21 & b22); a21 = b21 ^ (~b22 & b23); a22 = b22 ^ (~b23 & b24);
            a23 = b23 ^ (~b24 & b20); a24 = b24 ^ (~b20 & b21);

            a0 ^= RC[r];
        }

        s[0] = a0; s[1] = a1; s[2] = a2; s[3] = a3; s[4] = a4;
        s[5] = a5; s[6] = a6; s[7] = a7; s[8] = a8; s[9] = a9;
        s[10] = a10; s[11] = a11; s[12] = a12; s[13] = a13; s[14] = a14;
        s[15] = a15; s[16] = a16; s[17] = a17; s[18] = a18; s[19] = a19;
        s[20] = a20; s[21] = a21; s[22] = a22; s[23] = a23; s[24] = a24;
    }

    /**
     * DeepSeekHashV1: SHA3-256 with 23 rounds (skip round 0).
     * Returns 32-byte digest.
     */
    public static byte[] deepSeekHashV1(byte[] data) {
        final int rate = 136; // SHA3-256 rate in bytes
        long[] s = new long[25];

        int off = 0;
        ByteBuffer buf = ByteBuffer.allocate(rate).order(ByteOrder.LITTLE_ENDIAN);
        while (off + rate <= data.length) {
            buf.clear();
            buf.put(data, off, rate);
            buf.flip();
            for (int i = 0; i < rate / 8; i++) {
                s[i] ^= buf.getLong();
            }
            keccakF23(s);
            off += rate;
        }

        // Final block with padding
        byte[] finalBlock = new byte[rate];
        int tailLen = data.length - off;
        System.arraycopy(data, off, finalBlock, 0, tailLen);
        finalBlock[tailLen] = 0x06;
        finalBlock[rate - 1] |= (byte) 0x80;

        buf.clear();
        buf.put(finalBlock);
        buf.flip();
        for (int i = 0; i < rate / 8; i++) {
            s[i] ^= buf.getLong();
        }
        keccakF23(s);

        byte[] out = new byte[32];
        ByteBuffer outBuf = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN);
        outBuf.putLong(s[0]);
        outBuf.putLong(s[1]);
        outBuf.putLong(s[2]);
        outBuf.putLong(s[3]);
        return out;
    }

    /**
     * Build prefix: "<salt>_<expire_at>_"
     */
    private static String buildPrefix(String salt, long expireAt) {
        return salt + "_" + expireAt + "_";
    }

    /**
     * Solve PoW: find nonce in [0, difficulty) such that
     * DeepSeekHashV1(prefix + str(nonce)) == challenge.
     */
    public long solvePow(String challengeHex, String salt, long expireAt, long difficulty) {
        if (challengeHex.length() != 64) {
            throw new PowException("challenge must be 64 hex chars, got " + challengeHex.length());
        }
        byte[] target = HEX.parseHex(challengeHex);

        String prefix = buildPrefix(salt, expireAt);
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);

        for (long n = 0; n < difficulty; n++) {
            String input = prefix + n;
            byte[] hash = deepSeekHashV1(input.getBytes(StandardCharsets.UTF_8));
            if (java.util.Arrays.equals(hash, target)) {
                return n;
            }
        }
        throw new PowException("no solution within difficulty " + difficulty);
    }

    /**
     * Build the x-ds-pow-response header value.
     * Format: base64(JSON of {algorithm, challenge, salt, answer, signature, target_path})
     */
    public String buildPowHeader(String algorithm, String challenge, String salt,
                                  long answer, String signature, String targetPath) {
        String json = String.format(
            "{\"algorithm\":\"%s\",\"challenge\":\"%s\",\"salt\":\"%s\",\"answer\":%d,\"signature\":\"%s\",\"target_path\":\"%s\"}",
            algorithm, challenge, salt, answer, signature, targetPath);
        return java.util.Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }
}
