package com.ds2api.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.DeserializationContext;
import lombok.Data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 1:1 mapping of config.json. All JSON snake_case keys are mapped via @JsonProperty
 * to Java camelCase fields, so Jackson deserializes the original config.json unchanged.
 *
 * This class is NOT a Spring @ConfigurationProperties bean; it is manually loaded
 * by ConfigLoaderService from the config.json file at startup and on file change.
 */
@Data
public class Ds2Config {

    @JsonProperty("keys")
    private List<ApiKey> keys = new ArrayList<>();

    @JsonProperty("api_keys")
    private List<ApiKey> apiKeys = new ArrayList<>();

    @JsonProperty("accounts")
    private List<Account> accounts = new ArrayList<>();

    @JsonProperty("model_aliases")
    private Map<String, String> modelAliases = new HashMap<>();

    @JsonProperty("runtime")
    private RuntimeConfig runtime = new RuntimeConfig();

    @JsonProperty("auto_delete")
    private AutoDeleteConfig autoDelete = new AutoDeleteConfig();

    @JsonProperty("current_input_file")
    private CurrentInputFileConfig currentInputFile = new CurrentInputFileConfig();

    @JsonProperty("thinking_injection")
    private ThinkingInjectionConfig thinkingInjection = new ThinkingInjectionConfig();

    @JsonProperty("admin_key")
    private String adminKey = "admin";

    @JsonProperty("dev")
    private DevConfig dev = new DevConfig();

    // --- inner config classes ---

    @Data
    @JsonDeserialize(using = ApiKey.ApiKeyDeserializer.class)
    public static class ApiKey {
        @JsonProperty("key") private String key;
        @JsonProperty("name") private String name;
        @JsonProperty("remark") private String remark;

        // 支持字符串反序列化
        public static class ApiKeyDeserializer extends JsonDeserializer<ApiKey> {
            @Override
            public ApiKey deserialize(com.fasterxml.jackson.core.JsonParser p, DeserializationContext ctxt) 
                    throws IOException, com.fasterxml.jackson.core.JsonProcessingException {
                if (p.currentToken() == com.fasterxml.jackson.core.JsonToken.VALUE_STRING) {
                    ApiKey ak = new ApiKey();
                    ak.setKey(p.getText());
                    return ak;
                }
                // 默认对象反序列化
                return ctxt.readValue(p, ApiKey.class);
            }
        }
    }

    @Data
    public static class Account {
        @JsonProperty("email") private String email;
        @JsonProperty("mobile") private String mobile;
        @JsonProperty("password") private String password;
        @JsonProperty("token") private String token;
        @JsonProperty("area_code") private String areaCode;
        @JsonProperty("device_id") private String deviceId;
        @JsonProperty("web_cookie") private String webCookie;
        @JsonProperty("device_profile") private DeviceProfilePayload deviceProfile;
        @JsonProperty("name") private String name;
        @JsonProperty("remark") private String remark;
        @JsonProperty("proxy") private String proxy;
    }

    @Data
    public static class DeviceProfilePayload {
        @JsonProperty("appId") private String appId = "default";
        @JsonProperty("organization") private String organization = "P9usCUBauxft8eAmUXaZ";
        @JsonProperty("ep") private String ep;
        @JsonProperty("data") private String data;
        @JsonProperty("os") private String os = "web";
        @JsonProperty("encode") private int encode = 5;
        @JsonProperty("compress") private int compress = 2;
    }

    @Data
    public static class RuntimeConfig {
        @JsonProperty("account_max_inflight")
        private int accountMaxInflight = 2;

        @JsonProperty("account_max_queue")
        private int accountMaxQueue = 0;

        @JsonProperty("auto_refresh_token")
        private boolean autoRefreshToken = true;
    }

    @Data
    public static class AutoDeleteConfig {
        @JsonProperty("mode") private String mode = "none"; // none / single / all
    }

    @Data
    public static class CurrentInputFileConfig {
        @JsonProperty("enabled") private boolean enabled = true;
        @JsonProperty("threshold") private int threshold = 0;
    }

    @Data
    public static class ThinkingInjectionConfig {
        @JsonProperty("enabled") private boolean enabled = false;
        @JsonProperty("prompt") private String prompt = "";
    }

    @Data
    public static class DevConfig {
        @JsonProperty("packet_capture")
        private boolean packetCapture = false;

        @JsonProperty("packet_capture_limit")
        private int packetCaptureLimit = 20;
    }
}
