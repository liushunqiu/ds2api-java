package com.ds2api.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves external model names (gpt-4, claude-sonnet-4-6, gemini-2.5-pro, etc.)
 * to DeepSeek internal model names.
 *
 * Supports:
 *   - Built-in alias table (sync'd with ds2api Go reference)
 *   - Custom aliases from config.json's model_aliases
 *   - -nothinking suffix to disable reasoning
 *   - /v1/models listing
 */
@Service
public class ModelAliasService {

    private static final Logger log = LoggerFactory.getLogger(ModelAliasService.class);

    private static final String NO_THINKING_SUFFIX = "-nothinking";

    /** DeepSeek models the proxy natively supports. */
    private static final Set<String> DEEPSEEK_MODELS = Set.of(
        "deepseek-v4-flash", "deepseek-v4-flash-search",
        "deepseek-v4-pro", "deepseek-v4-pro-search",
        "deepseek-v4-vision"
    );

    /** Built-in alias table, aligned with Go reference. */
    private static final Map<String, String> BUILTIN_ALIASES = new LinkedHashMap<>();

    static {
        // OpenAI
        BUILTIN_ALIASES.put("gpt-4", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-4-turbo", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-4o", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-4o-mini", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-4.1", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-4.1-mini", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-4.1-nano", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-5", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-5-chat", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-5.1", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-5.1-chat", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-5.2", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-5.2-chat", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-5.3-chat", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-5.4", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-5.5", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-5-mini", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-5-nano", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-5.4-mini", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-5.4-nano", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("gpt-5-pro", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("gpt-5.2-pro", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("gpt-5.4-pro", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("gpt-5.5-pro", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("gpt-5-codex", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("gpt-5.1-codex", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("gpt-5.1-codex-mini", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("gpt-5.1-codex-max", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("gpt-5.2-codex", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("gpt-5.3-codex", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("o1", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("o1-preview", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("o1-mini", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("o3", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("o3-mini", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("o3-pro", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("o4-mini", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("codex-mini-latest", "deepseek-v4-pro");
        // Claude
        BUILTIN_ALIASES.put("claude-sonnet-4-6", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("claude-sonnet-4-5", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("claude-opus-4-6", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("claude-opus-4-7", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("claude-haiku-4-5", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("claude-3-7-sonnet-latest", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("claude-3-5-sonnet-latest", "deepseek-v4-flash");
        // Gemini
        BUILTIN_ALIASES.put("gemini-2.5-pro", "deepseek-v4-pro");
        BUILTIN_ALIASES.put("gemini-2.5-flash", "deepseek-v4-flash");
        // DeepSeek direct
        BUILTIN_ALIASES.put("deepseek-chat", "deepseek-v4-flash");
        BUILTIN_ALIASES.put("deepseek-reasoner", "deepseek-v4-pro");
    }

    private final ConfigLoaderService configLoader;

    public ModelAliasService(ConfigLoaderService configLoader) {
        this.configLoader = configLoader;
    }

    /**
     * Resolve a model name to the internal DeepSeek model.
     * Handles -nothinking suffix, built-in aliases, custom aliases from config.json,
     * and claude-haiku prefix matching.
     *
     * @return resolved model name, or empty if not resolvable
     */
    public Optional<String> resolveModel(String requested) {
        if (requested == null) return Optional.empty();
        String model = requested.toLowerCase().trim();
        if (model.isEmpty()) return Optional.empty();

        // Strip -nothinking suffix, track whether it was present
        boolean noThinking = false;
        String base = model;
        if (model.endsWith(NO_THINKING_SUFFIX)) {
            base = model.substring(0, model.length() - NO_THINKING_SUFFIX.length());
            noThinking = true;
        }

        // 1. Already a known DeepSeek model (with or without -nothinking)
        if (isDeepSeekModel(model)) return Optional.of(model);

        // 2. Check built-in aliases
        String mapped = BUILTIN_ALIASES.get(base);
        if (mapped != null && isDeepSeekModel(mapped)) {
            return Optional.of(noThinking ? mapped + NO_THINKING_SUFFIX : mapped);
        }

        // 3. claude-haiku prefix matching
        if (base.startsWith("claude-haiku")) {
            return Optional.of("deepseek-v4-flash");
        }

        // 4. Custom aliases from config.json
        Map<String, String> custom = configLoader.getConfig().getModelAliases();
        if (custom != null) {
            String customMapped = custom.get(base);
            if (customMapped != null && isDeepSeekModel(customMapped)) {
                return Optional.of(noThinking ? customMapped + NO_THINKING_SUFFIX : customMapped);
            }
            // Try with -nothinking stripped base mapping
            if (noThinking) {
                String strippedMapped = custom.get(base);
                if (strippedMapped != null && isDeepSeekModel(strippedMapped)) {
                    return Optional.of(strippedMapped + NO_THINKING_SUFFIX);
                }
            }
        }

        return Optional.empty();
    }

    /**
     * Returns (thinkingEnabled, searchEnabled, ok) for a model.
     */
    public ModelConfig getModelConfig(String model) {
        String base = stripNoThinking(model);
        boolean noThinking = !base.equals(model);
        return switch (base) {
            case "deepseek-v4-flash", "deepseek-v4-pro", "deepseek-v4-vision" ->
                new ModelConfig(!noThinking, false, true);
            case "deepseek-v4-flash-search", "deepseek-v4-pro-search" ->
                new ModelConfig(!noThinking, true, true);
            default -> new ModelConfig(false, false, false);
        };
    }

    /**
     * Returns the model_type field for the DeepSeek completion payload.
     */
    public String getModelType(String model) {
        String base = stripNoThinking(model);
        return switch (base) {
            case "deepseek-v4-flash", "deepseek-v4-flash-search" -> "default";
            case "deepseek-v4-pro", "deepseek-v4-pro-search" -> "expert";
            case "deepseek-v4-vision" -> "vision";
            default -> "default";
        };
    }

    /**
     * Get the list of models for /v1/models endpoint.
     */
    public List<Map<String, Object>> modelList() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (String m : DEEPSEEK_MODELS) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("id", m);
            entry.put("object", "model");
            entry.put("owned_by", "deepseek");
            result.add(entry);
            // Also expose -nothinking variants
            Map<String, Object> ntEntry = new LinkedHashMap<>();
            ntEntry.put("id", m + NO_THINKING_SUFFIX);
            ntEntry.put("object", "model");
            ntEntry.put("owned_by", "deepseek");
            result.add(ntEntry);
        }
        // Add all custom aliases
        Map<String, String> custom = configLoader.getConfig().getModelAliases();
        if (custom != null) {
            for (String alias : custom.keySet()) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("id", alias);
                entry.put("object", "model");
                entry.put("owned_by", "ds2api");
                result.add(entry);
            }
        }
        return result;
    }

    /** Check if a model is natively supported after stripping -nothinking. */
    public boolean isDeepSeekModel(String model) {
        return DEEPSEEK_MODELS.contains(stripNoThinking(model));
    }

    private String stripNoThinking(String model) {
        if (model != null && model.endsWith(NO_THINKING_SUFFIX)) {
            return model.substring(0, model.length() - NO_THINKING_SUFFIX.length());
        }
        return model;
    }

    /** Immutable model configuration result. */
    public record ModelConfig(boolean thinkingEnabled, boolean searchEnabled, boolean ok) {}
}
