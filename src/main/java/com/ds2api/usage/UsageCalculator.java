package com.ds2api.usage;

/**
 * Approximate token counting for monitoring / billing SDK needs.
 * Empirical heuristic for mixed Chinese-English text: 1 token ~ 1.5 chars.
 */
public final class UsageCalculator {

    private UsageCalculator() {
    }

    public static int estimateTokens(String text) {
        if (text == null || text.isBlank())
            return 0;
        return Math.max(1, (int) Math.ceil(text.length() / 1.5));
    }

    public record Usage(int promptTokens, int completionTokens, int totalTokens) {
    }

    public static Usage calculate(String promptText, String completionText) {
        int p = estimateTokens(promptText);
        int c = estimateTokens(completionText);
        return new Usage(p, c, p + c);
    }
}
