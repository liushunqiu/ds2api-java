package com.ds2api.tool;

import com.ds2api.model.InternalStreamEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ToolCallStreamParserTest {

    private ToolCallStreamParser parser;

    @BeforeEach
    void setUp() {
        parser = new ToolCallStreamParser();
    }

    @Test
    void testHeredocExtraction() {
        String dsml = """
                <|DSML|tool_calls>
                  <|DSML|invoke name="write_stdin">
                    <|DSML|parameter name="session_id"><![CDATA[298403]]></|DSML|parameter>
                    <|DSML|parameter name="chars"><![CDATA[
                cat > /Users/liushunqiu/Desktop/new_ds2api/ds2api-java/docs/superpowers/plans/2026-05-13-controller-comments-zh.md << 'PLANEOF'
                # Controller 英文注释转中文 实现计划
                
                > **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。
                
                **目标：** 将 `controller/` 目录下 5 个 Java 文件中所有 Javadoc 注释从英文转为中文，遵循中文技术文档排版规范。
                
                **架构：** 纯注释翻译任务，不改动任何代码逻辑。每个 controller 文件独立修改，遵循 `chinese-documentation` 技能中的空格、标点、术语规范。翻译时保留技术名词英文原文（如 JWT、SSE、Docker/K8s），描述性文字使用自然中文。
                
                **技术栈：** Java 17 Javadoc 注释，无代码变更
                
                ---
                
                ## 文件结构
                
                | 文件 | 行数 | 英文注释数量 | 说明 |
                |------|------|-------------|------|
                | `AdminController.java` | 206 | 6 处 Javadoc | 类注释 + 4 个方法注释 + 1 个 inline 注释 |
                | `HealthController.java` | 40 | 1 处 Javadoc | 仅类注释 |
                | `OpenAiController.java` | 237 | 7 处 Javadoc | 类注释 + 5 个方法注释 + Routes 列表 |
                | `OpenAiEmbeddingController.java` | 48 | 1 处 Javadoc | 仅类注释 |
                | `ResponsesCompatController.java` | 32 | 1 处 Javadoc | 仅类注释 |
                
                ---
                
                ## 翻译规范要点
                
                依据 `chinese-documentation` 技能：
                
                1. **中英文之间加空格**：如「返回 JWT token」而非「返回JWT token」
                2. **中文与数字之间加空格**：如「过期时间 24 小时」
                3. **中文语境使用全角标点**：句号用「。」而非「.」
                4. **保留技术名词英文原文**：JWT、SSE、Docker、K8s、Bearer、API、SDK 等不翻译
                5. **避免机翻味**：不用被动语态，句式自然
                6. **保留英文的内容**：URL 路径（如 `/v1/models`）、代码标识符、HTTP 方法名保持英文
                
                ---
                
                ### 任务 1：AdminController.java 注释翻译
                
                **文件：**
                - 修改：`src/main/java/com/ds2api/controller/AdminController.java`
                
                - [ ] **步骤 1：翻译类级别 Javadoc**
                
                当前内容（第 21-29 行）：
                ```java
                /**
                 * Admin endpoints for JWT login, config management, and pool monitoring.
                 *
                 * Admin authentication: the AdminAuthFilter (registered on /admin/**)
                 * validates a JWT Bearer token on all admin endpoints except /admin/login.
                 * For direct API access without JWT, an X-Admin-Key header can be used on
                 * the queue/status endpoint as a lightweight alternative.
                 */
                ```
                
                改为：
                ```java
                /**
                 * 管理后台接口，提供 JWT 登录、配置管理及账户池监控功能。
                 *
                 * 鉴权说明：AdminAuthFilter（注册于 /admin/**）对所有管理接口
                 * （除 /admin/login 外）校验 JWT Bearer token。
                 * 对于不需要 JWT 的直接 API 访问，queue/status 接口支持通过
                 * X-Admin-Key 请求头作为轻量级替代方案。
                 */
                ```
                
                - [ ] **步骤 2：翻译 login() 方法 Javadoc**
                
                当前内容（第 34-37 行）：
                ```java
                    /**
                     * POST /admin/login
                     * Authenticate with admin_key and receive a JWT.
                     */
                ```
                
                改为：
                ```java
                    /**
                     * POST /admin/login
                     * 使用 admin_key 进行身份认证，成功后返回 JWT token。
                     */
                ```
                
                - [ ] **步骤 3：翻译 reloadConfig() 方法 Javadoc**
                
                当前内容（第 68-70 行）：
                ```java
                    /**
                     * POST /admin/reload-config
                     * Hot-reload config.json without restart.
                     */
                ```
                
                改为：
                ```java
                    /**
                     * POST /admin/reload-config
                     * 热重载 config.json，无需重启服务。
                     */
                ```
                
                - [ ] **步骤 4：翻译 getConfig() 方法 Javadoc**
                
                当前内容（第 83-85 行）：
                ```java
                    /**
                     * GET /admin/config
                     * Return the current effective config (keys/accounts masked).
                     */
                ```
                
                改为：
                ```java
                    /**
                     * GET /admin/config
                     * 返回当前生效的配置（密钥和账号信息已脱敏）。
                     */
                ```
                
                - [ ] **步骤 5：翻译 updateConfig() 方法 Javadoc**
                
                当前内容（第 100-108 行）：
                ```java
                    /**
                     * POST /admin/config
                     * Hot-reload config with partial JSON merge. Only the fields present in the
                     * request body are updated; all other fields retain their current values.
                     *
                     * Accepts snake_case keys matching config.json (e.g. "account_max_inflight").
                     * Returns the full updated config on success.
                     * Returns 422 with the validation error message on invalid payload.
                     */
                ```
                
                改为：
                ```java
                    /**
                     * POST /admin/config
                     * 通过局部 JSON 合并的方式热更新配置。仅更新请求体中包含的字段，
                     * 其余字段保持当前值不变。
                     *
                     * 接受与 config.json 匹配的 snake_case 键名（如 "account_max_inflight"）。
                     * 成功时返回完整的更新后配置。
                     * 当请求体不合法时返回 422 及校验错误信息。
                     */
                ```
                
                - [ ] **步骤 6：翻译 queueStatus() 方法 Javadoc**
                
                当前内容（第 123-129 行）：
                ```java
                    /**
                     * GET /admin/queue/status
                     * Real-time account pool status: per-account available slots, queue depth,
                     * in-flight count, plus global metrics like max inflight and 429 threshold.
                     *
                     * Supports optional X-Admin-Key header as a lightweight alternative to JWT
                     * for monitoring systems (Prometheus, Grafana, etc.).
                     */
                ```
                
                改为：
                ```java
                    /**
                     * GET /admin/queue/status
                     * 实时账户池状态：各账户的可用槽位、队列深度、处理中的请求数，
                     * 以及全局指标（如最大并发数和 429 阈值）。
                     *
                     * 支持可选的 X-Admin-Key 请求头作为 JWT 的轻量级替代方案，
                     * 便于监控系统（Prometheus、Grafana 等）接入。
                     */
                ```
                
                - [ ] **步骤 7：编译验证**
                
                ```bash
                mvn compile -Dmaven.repo.local=/tmp/m2repo -o 2>&1 | tail -5
                ```
                
                预期：BUILD SUCCESS
                
                ---
                
                ### 任务 2：HealthController.java 注释翻译
                
                **文件：**
                - 修改：`src/main/java/com/ds2api/controller/HealthController.java`
                
                - [ ] **步骤 1：翻译类级别 Javadoc**
                
                当前内容（第 12-14 行）：
                ```java
                /**
                 * Health and readiness probes for Docker/K8s orchestration.
                 */
                ```
                
                改为：
                ```java
                /**
                 * 健康检查与就绪探针，供 Docker/K8s 编排系统使用。
                 */
                ```
                
                - [ ] **步骤 2：编译验证**
                
                ```bash
                mvn compile -Dmaven.repo.local=/tmp/m2repo -o 2>&1 | tail -5
                ```
                
                预期：BUILD SUCCESS
                
                ---
                
                ### 任务 3：OpenAiController.java 注释翻译
                
                **文件：**
                - 修改：`src/main/java/com/ds2api/controller/OpenAiController.java`
                
                - [ ] **步骤 1：翻译类级别 Javadoc（含 Routes 列表）**
                
                当前内容（第 26-33 行）：
                ```java
                /**
                 * OpenAI-compatible API controller.
                 * Routes:
                 *   GET  /v1/models             - Model list (DeepSeek native IDs only)
                 *   GET  /v1/models/{id}        - Single model lookup
                 *   POST /v1/chat/completions   - Chat streaming
                 *   POST /v1/responses          - Responses API (streaming)
                 *   GET  /v1/responses/{id}     - Responses retrieval (cached)
                 */
                ```
                
                改为：
                ```java
                /**
                 * OpenAI 兼容 API 控制器。
                 * 路由说明：
                 *   GET  /v1/models             - 模型列表（仅返回 DeepSeek 原生 ID）
                 *   GET  /v1/models/{id}        - 单个模型查询
                 *   POST /v1/chat/completions   - 对话补全（流式）
                 *   POST /v1/responses          - Responses API（流式）
                 *   GET  /v1/responses/{id}     - 响应检索（从缓存获取）
                 */
                ```
                
                - [ ] **步骤 2：翻译 listModels() 方法 Javadoc**
                
                当前内容（第 43-46 行）：
                ```java
                    /**
                     * GET /v1/models
                     * Returns the 10 DeepSeek v4 native model IDs (no aliases).
                     */
                ```
                
                改为：
                ```java
                    /**
                     * GET /v1/models
                     * 返回 10 个 DeepSeek v4 原生模型 ID（不含别名）。
                     */
                ```
                
                - [ ] **步骤 3：翻译 getModel() 方法 Javadoc**
                
                当前内容（第 60-63 行）：
                ```java
                    /**
                     * GET /v1/models/{id}
                     * Look up a single model by its native DeepSeek ID.
                     */
                ```
                
                改为：
                ```java
                    /**
                     * GET /v1/models/{id}
                     * 通过 DeepSeek 原生 ID 查询单个模型。
                     */
                ```
                
                - [ ] **步骤 4：翻译 chatCompletions() 方法 Javadoc**
                
                当前内容（第 79-82 行）：
                ```java
                    /**
                     * POST /v1/chat/completions
                     * OpenAI Chat Completions endpoint with SSE streaming support.
                     */
                ```
                
                改为：
                ```java
                    /**
                     * POST /v1/chat/completions
                     * OpenAI Chat Completions 接口，支持 SSE 流式输出。
                     */
                ```
                
                - [ ] **步骤 5：翻译 createResponse() 方法 Javadoc**
                
                当前内容（第 109-112 行）：
                ```java
                    /**
                     * POST /v1/responses
                     * OpenAI Responses API - streaming with caching for later retrieval.
                     */
                ```
                
                改为：
                ```java
                    /**
                     * POST /v1/responses
                     * OpenAI Responses API 接口，流式输出并缓存以供后续检索。
                     */
                ```
                
                - [ ] **步骤 6：翻译 getResponse() 方法 Javadoc**
                
                当前内容（第 201-204 行）：
                ```java
                    /**
                     * GET /v1/responses/{id}
                     * Retrieves a previously cached response.
                     */
                ```
                
                改为：
                ```java
                    /**
                     * GET /v1/responses/{id}
                     * 检索之前缓存的响应数据。
                     */
                ```
                
                - [ ] **步骤 7：编译验证**
                
                ```bash
                mvn compile -Dmaven.repo.local=/tmp/m2repo -o 2>&1 | tail -5
                ```
                
                预期：BUILD SUCCESS
                
                ---
                
                ### 任务 4：OpenAiEmbeddingController.java 注释翻译
                
                **文件：**
                - 修改：`src/main/java/com/ds2api/controller/OpenAiEmbeddingController.java`
                
                - [ ] **步骤 1：翻译类级别 Javadoc**
                
                当前内容（第 13-16 行）：
                ```java
                /**
                 * Placeholder embeddings endpoint.
                 * DeepSeek Web does not offer an embeddings API. This returns a fixed
                 * zero-vector response to prevent SDK 404 errors during capability probing.
                 */
                ```
                
                改为：
                ```java
                /**
                 * 占位 Embeddings 接口。
                 * DeepSeek Web 不提供 Embeddings API，此接口返回固定的零向量响应，
                 * 以避免 SDK 在能力探测时出现 404 错误。
                 */
                ```
                
                - [ ] **步骤 2：编译验证**
                
                ```bash
                mvn compile -Dmaven.repo.local=/tmp/m2repo -o 2>&1 | tail -5
                ```
                
                预期：BUILD SUCCESS
                
                ---
                
                ### 任务 5：ResponsesCompatController.java 注释翻译
                
                **文件：**
                - 修改：`src/main/java/com/ds2api/controller/ResponsesCompatController.java`
                
                - [ ] **步骤 1：翻译类级别 Javadoc**
                
                当前内容（第 13-16 行）：
                ```java
                /**
                 * Compatibility controller for clients that call /responses directly
                 * without the /v1 prefix (e.g. Codex wire_api=responses).
                 */
                ```
                
                改为：
                ```java
                /**
                 * 兼容性控制器，供不使用 /v1 前缀直接调用 /responses 的客户端使用
                 * （例如 Codex 的 wire_api=responses 模式）。
                 */
                ```
                
                - [ ] **步骤 2：编译验证**
                
                ```bash
                mvn compile -Dmaven.repo.local=/tmp/m2repo -o 2>&1 | tail -5
                ```
                
                预期：BUILD SUCCESS
                
                ---
                
                ### 任务 6：最终验证 — 全量编译 + 测试
                
                **文件：** 无修改，仅验证
                
                - [ ] **步骤 1：全量编译**
                
                ```bash
                cd /Users/liushunqiu/Desktop/new_ds2api/ds2api-java && mvn compile -Dmaven.repo.local=/tmp/m2repo -o 2>&1 | tail -10
                ```
                
                预期：BUILD SUCCESS
                
                - [ ] **步骤 2：运行测试**
                
                ```bash
                cd /Users/liushunqiu/Desktop/new_ds2api/ds2api-java && mvn test -Dmaven.repo.local=/tmp/m2repo 2>&1 | tail -30
                ```
                
                预期：BUILD SUCCESS，所有测试通过
                
                - [ ] **步骤 3：Git 提交**
                
                ```bash
                git add src/main/java/com/ds2api/controller/AdminController.java \\
                        src/main/java/com/ds2api/controller/HealthController.java \\
                        src/main/java/com/ds2api/controller/OpenAiController.java \\
                        src/main/java/com/ds2api/controller/OpenAiEmbeddingController.java \\
                        src/main/java/com/ds2api/controller/ResponsesCompatController.java
                git commit -m "docs(controller): 将 controller 层 Javadoc 注释从英文翻译为中文"
                ```
                
                ---
                
                ## 自检
                
                1. **规格覆盖度**：5 个 controller 文件共 16 处英文 Javadoc 注释，全部覆盖 ✅
                2. **占位符扫描**：无 TODO / 待定 / 后续实现，每个步骤均有完整的中文翻译内容 ✅
                3. **类型一致性**：仅修改注释文本，不涉及方法签名或类型变更 ✅
                4. **翻译规范**：遵循 chinese-documentation 技能的空格、标点、术语规范 ✅
                PLANEOF
                echo "PLAN WRITTEN"
                ]]></|DSML|parameter>
                  </|DSML|invoke>
                </|DSML|tool_calls>
                """;

        List<InternalStreamEvent> events = parser.processChunk(dsml);
        parser.flushAndReset();

        InternalStreamEvent.ToolCallDelta delta = events.stream()
                .filter(e -> e instanceof InternalStreamEvent.ToolCallDelta)
                .map(e -> (InternalStreamEvent.ToolCallDelta) e)
                .findFirst()
                .orElse(null);

        assertNotNull(delta, "应该有 ToolCallDelta 事件");
        String args = delta.argumentsDelta();
        System.out.println("解析后的参数: " + args);

        assertTrue(args.contains("# Controller"), "应该包含计划标题");
        assertTrue(args.contains("实现计划"), "应该包含计划内容");
        assertFalse(args.contains("cat >"), "不应该包含 cat 命令");
        assertFalse(args.contains("PLANEOF"), "不应该包含 PLANEOF");
        assertFalse(args.contains("<<"), "不应该包含 heredoc 语法");
        assertFalse(args.contains("echo"), "不应该包含 echo 命令");
    }

    @Test
    void testNormalContent() {
        String dsml = "<|DSML|tool_calls>\n"
                + "  <|DSML|invoke name=\"write_stdin\">\n"
                + "    <|DSML|parameter name=\"session_id\"><![CDATA[12345]]></|DSML|parameter>\n"
                + "    <|DSML|parameter name=\"chars\"><![CDATA[这是普通文本内容]]></|DSML|parameter>\n"
                + "  </|DSML|invoke>\n"
                + "</|DSML|tool_calls>";

        List<InternalStreamEvent> events = parser.processChunk(dsml);
        parser.flushAndReset();

        InternalStreamEvent.ToolCallDelta delta = events.stream()
                .filter(e -> e instanceof InternalStreamEvent.ToolCallDelta)
                .map(e -> (InternalStreamEvent.ToolCallDelta) e)
                .findFirst()
                .orElse(null);

        assertNotNull(delta, "应该有 ToolCallDelta 事件");
        String args = delta.argumentsDelta();
        System.out.println("普通内容参数: " + args);

        assertTrue(args.contains("这是普通文本内容"), "应该包含原始内容");
    }

    @Test
    void testToolNameMapping() {
        String dsml = "<|DSML|tool_calls>\n"
                + "  <|DSML|invoke name=\"write_stdin\">\n"
                + "    <|DSML|parameter name=\"session_id\"><![CDATA[12345]]></|DSML|parameter>\n"
                + "    <|DSML|parameter name=\"chars\"><![CDATA[内容]]></|DSML|parameter>\n"
                + "  </|DSML|invoke>\n"
                + "</|DSML|tool_calls>";

        List<InternalStreamEvent> events = parser.processChunk(dsml);
        parser.flushAndReset();

        InternalStreamEvent.ToolCallStart start = events.stream()
                .filter(e -> e instanceof InternalStreamEvent.ToolCallStart)
                .map(e -> (InternalStreamEvent.ToolCallStart) e)
                .findFirst()
                .orElse(null);

        assertNotNull(start, "应该有 ToolCallStart 事件");
        assertEquals("write_stdin", start.name(), "工具名应该是 write_stdin");
    }

    @Test
    void testCdataWithSpecialChars() {
        String dsml = "<|DSML|tool_calls>\n"
                + "  <|DSML|invoke name=\"write_stdin\">\n"
                + "    <|DSML|parameter name=\"session_id\"><![CDATA[12345]]></|DSML|parameter>\n"
                + "    <|DSML|parameter name=\"chars\"><![CDATA[包含特殊字符: < > & \" ' 和换行\n"
                + "第二行内容]]></|DSML|parameter>\n"
                + "  </|DSML|invoke>\n"
                + "</|DSML|tool_calls>";

        List<InternalStreamEvent> events = parser.processChunk(dsml);
        parser.flushAndReset();

        InternalStreamEvent.ToolCallDelta delta = events.stream()
                .filter(e -> e instanceof InternalStreamEvent.ToolCallDelta)
                .map(e -> (InternalStreamEvent.ToolCallDelta) e)
                .findFirst()
                .orElse(null);

        assertNotNull(delta, "应该有 ToolCallDelta 事件");
        String args = delta.argumentsDelta();
        System.out.println("特殊字符参数: " + args);

        assertTrue(args.contains("&"), "应该包含 & 字符");
    }
}
