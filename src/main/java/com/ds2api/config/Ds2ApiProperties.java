package com.ds2api.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "ds2api")
public class Ds2ApiProperties {
    private Upstream upstream = new Upstream();

    @Data
    public static class Upstream {
        private String baseUrl = "https://chat.deepseek.com";
        private String token = "";
    }
}
