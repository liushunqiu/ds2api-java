package com.ds2api.registry;

import com.ds2api.model.ModelMeta;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
public class ModelRegistryService {

    private static final long CREATED_AT = Instant.parse("2024-01-01T00:00:00Z").getEpochSecond();

    // 10 DeepSeek v4 native model IDs, aligned with ds2api knowledge base.
    private static final List<ModelMeta> MODELS = List.of(
        new ModelMeta("deepseek-v4-flash", "default", CREATED_AT),
        new ModelMeta("deepseek-v4-flash-nothinking", "default", CREATED_AT),
        new ModelMeta("deepseek-v4-flash-search", "default", CREATED_AT),
        new ModelMeta("deepseek-v4-flash-search-nothinking", "default", CREATED_AT),
        new ModelMeta("deepseek-v4-pro", "expert", CREATED_AT),
        new ModelMeta("deepseek-v4-pro-nothinking", "expert", CREATED_AT),
        new ModelMeta("deepseek-v4-pro-search", "expert", CREATED_AT),
        new ModelMeta("deepseek-v4-pro-search-nothinking", "expert", CREATED_AT),
        new ModelMeta("deepseek-v4-vision", "vision", CREATED_AT),
        new ModelMeta("deepseek-v4-vision-nothinking", "vision", CREATED_AT)
    );

    public List<ModelMeta> getAll() {
        return MODELS;
    }

    public Optional<ModelMeta> getById(String id) {
        return MODELS.stream().filter(m -> m.id().equals(id)).findFirst();
    }
}
