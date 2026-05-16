# DS2API (Java)

> DeepSeek API 代理服务 —— 提供 OpenAI 兼容接口，将请求转发至 DeepSeek 后端。

[![Java](https://img.shields.io/badge/Java-17-blue.svg)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.5-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## 目录

- [项目简介](#项目简介)
- [技术栈](#技术栈)
- [快速开始](#快速开始)
  - [本地运行](#本地运行)
  - [Docker 部署](#docker-部署)
- [配置说明](#配置说明)
  - [config.json](#configjson)
  - [application.yml](#applicationyml)
  - [环境变量](#环境变量)
- [API 接口](#api-接口)
  - [OpenAI 兼容接口](#openai-兼容接口)
  - [管理接口](#管理接口)
  - [健康检查](#健康检查)
- [项目结构](#项目结构)
- [构建与测试](#构建与测试)
- [部署](#部署)

---

## 项目简介

DS2API 是一个基于 Spring Boot WebFlux 的 API 代理服务，它将 OpenAI 兼容的 API 请求转换为 DeepSeek 后端协议，支持：

- ✅ OpenAI 兼容的 `/v1/chat/completions`、`/v1/models`、`/v1/embeddings` 端点
- ✅ 流式 (SSE) 与非流式聊天补全
- ✅ 多账号轮询与自动 Token 刷新
- ✅ DeepSeek 验证码 (PoW) 自动求解
- ✅ Caffeine 本地缓存（会话、响应）
- ✅ Docker 一键部署，健康检查开箱即用
- ✅ 管理后台：配置热加载、队列状态查看、抓包调试

---

## 技术栈

| 组件 | 版本/说明 |
|------|----------|
| Java | 17 |
| Spring Boot | 3.3.5 (WebFlux) |
| 响应式框架 | Project Reactor |
| 缓存 | Caffeine |
| 构建工具 | Maven 3.9+ |
| 容器化 | Docker + Docker Compose |
| 辅助库 | Lombok, Jackson, Apache Commons Lang3 |

---

## 快速开始

### 本地运行

**前置条件：** JDK 17、Maven 3.9+

```bash
# 1. 克隆项目
git clone <repo-url>
cd ds2api-java

# 2. 准备配置文件（填入真实账号信息）
vim config.json

# 3. 编译运行
mvn spring-boot:run

# 4. 验证
curl http://localhost:5001/healthz
```

### Docker 部署

```bash
# 1. 准备配置文件
vim config.json   # 填入你的 DeepSeek 账号信息

# 2. （可选）创建 .env 设置敏感变量
cat > .env << 'EOF'
DS2API_HOST_PORT=6011
DS2API_ADMIN_KEY=your_strong_admin_key
DS2API_UPSTREAM_TOKEN=your_upstream_token
EOF

# 3. 构建并启动
docker compose up --build -d

# 4. 检查服务健康
curl http://localhost:6011/healthz

# 5. 查看日志
docker compose logs -f
```

---

## 配置说明

本项目有 4 个配置相关文件，在 Docker 部署中各司其职：

| 文件 | 作用 | Docker 中的处理 |
|------|------|----------------|
| `config.json` | 运行时核心配置（账号、密码、模型别名等） | **volume 挂载**，不打包进镜像 |
| `application.yml` | Spring Boot 框架配置（端口、日志、缓存） | **打进 JAR 包**，通过环境变量覆盖 |
| `docker-compose.yml` | 容器编排 & 环境变量注入 | 部署时使用 |
| `.env` | 本地环境变量（敏感信息） | docker compose 自动读取 |

### config.json

运行时核心配置，包含 DeepSeek 账号、API key 等敏感信息。通过 volume 挂载进容器，**不入镜像**。

```json
{
  "keys": ["sk-xxx"],
  "api_keys": [],
  "accounts": [
    {
      "mobile": "13800138000",
      "password": "your_password",
      "area_code": "+86",
      "device_id": "B..."
    },
    {
      "mobile": "13900139000",
      "password": "your_password",
      "area_code": "+86",
      "device_id": "B..."
    }
  ],
  "model_aliases": {},
  "runtime": {
    "account_max_inflight": 2,
    "account_max_queue": 0,
    "auto_refresh_token": true
  },
  "auto_delete": {
    "mode": "none"
  },
  "thinking_injection": {
    "enabled": true,
    "prompt": ""
  },
  "admin_key": "admin",
  "dev": {
    "packet_capture": false,
    "packet_capture_limit": 20
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `keys` | string[] | API 鉴权 key 列表，客户端请求时通过 `Authorization: Bearer <key>` 传入 |
| `accounts` | object[] | DeepSeek 登录账号，支持 `mobile` 或 `email` + `password`；推荐配置共享 `device_id`，可选配置 `web_cookie` 或 `device_profile.ep/data` 增强 Web 指纹对齐 |
| `model_aliases` | object | 模型别名映射，例如 `{"gpt-4": "deepseek-chat"}` |
| `runtime.account_max_inflight` | int | 单个账号最大并发请求数 |
| `runtime.auto_refresh_token` | bool | 是否自动刷新过期 Token |
| `admin_key` | string | 管理后台鉴权密码 |
| `dev.packet_capture` | bool | 开启请求/响应抓包调试 |

账号配置优先级：

1. 显式 `device_id`：推荐方式，可多个账号复用同一个值，必须带 `B` 前缀。
2. `web_cookie`：可选，服务会从 `.thumbcache_*` 派生 `device_id`，并在 completion 请求中透传 Cookie。
3. `device_profile.ep/data`：可选，服务会调用数美 `deviceprofile/v4` 获取 `detail.deviceId`，再拼成 `B<deviceId>`。

如果只想降低配置成本，多个账号配置同一个 `device_id` 即可，不需要填写 `web_cookie`。

> **安全提示：** `config.json` 包含敏感凭据，务必加入 `.gitignore`，不要提交到 Git 仓库。

### application.yml

Spring Boot 框架配置，编译时已打包进 JAR。关键项通过环境变量覆盖：

```yaml
server:
  port: ${DS2API_PORT:5001}

ds2api:
  upstream:
    base-url: "https://chat.deepseek.com"
    token: "${DS2API_UPSTREAM_TOKEN:}"

logging:
  level:
    com.ds2api: ${DS2API_LOG_LEVEL:INFO}
```

有默认值的无需额外配置；敏感值（如 `DS2API_UPSTREAM_TOKEN`）通过 `docker-compose.yml` 的 `environment` 注入。

### 环境变量

| 变量 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `DS2API_HOST_PORT` | 否 | `6011` | Docker 宿主机映射端口 |
| `DS2API_ADMIN_KEY` | **是** | `change_me_strong_key` | 管理后台鉴权密钥 |
| `DS2API_UPSTREAM_TOKEN` | 否 | (空) | DeepSeek 上游 Token |
| `DS2API_LOG_LEVEL` | 否 | `INFO` | 日志级别 (DEBUG/INFO/WARN/ERROR) |
| `DS2API_CONFIG_PATH` | 否 | `/data/config.json` | 配置文件在容器内的路径 |

---

## API 接口

### OpenAI 兼容接口

所有 OpenAI 兼容接口以 `/v1` 为前缀。

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/v1/models` | 获取模型列表 |
| `GET` | `/v1/models/{id}` | 获取单个模型详情 |
| `POST` | `/v1/chat/completions` | 聊天补全（支持流式 SSE） |
| `POST` | `/v1/embeddings` | 文本向量化 |
| `POST` | `/v1/responses` | Responses API |
| `GET` | `/v1/responses/{id}` | 查询响应结果 |

**鉴权方式：** 请求头 `Authorization: Bearer <key>`，key 需匹配 `config.json` 中 `keys` 数组的任一值。

**流式调用示例：**

```bash
curl -X POST http://localhost:6011/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-xxx" \
  -d '{
    "model": "deepseek-chat",
    "messages": [{"role": "user", "content": "你好，介绍一下你自己"}],
    "stream": true
  }'
```

### 管理接口

所有管理接口以 `/admin` 为前缀，需 `admin-key` 鉴权。

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/admin/login` | 管理员登录 |
| `GET` | `/admin/config` | 查看当前运行时配置 |
| `POST` | `/admin/config` | 更新运行时配置 |
| `POST` | `/admin/reload-config` | 热重载配置（无需重启容器） |
| `GET` | `/admin/queue/status` | 查看各账号任务队列状态 |

**调用示例：**

```bash
# 查看队列状态
curl -X GET http://localhost:6011/admin/queue/status \
  -H "Authorization: Bearer admin"

# 热重载配置
curl -X POST http://localhost:6011/admin/reload-config \
  -H "Authorization: Bearer admin"
```

### 健康检查

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/healthz` | 存活检查（Docker healthcheck 使用） |
| `GET` | `/readyz` | 就绪检查 |

无需鉴权，返回 200 表示服务正常。

---

## 项目结构

```
ds2api-java/
├── src/main/java/com/ds2api/
│   ├── Ds2ApiApplication.java       # 应用入口
│   ├── adapter/                     # OpenAI ↔ DeepSeek 协议适配
│   ├── admin/                       # 管理后台逻辑
│   ├── auth/                        # 鉴权过滤器 & JWT 工具
│   ├── cache/                       # Caffeine 缓存（会话/响应）
│   ├── client/                      # DeepSeek HTTP 客户端（鉴权、会话、文件、PoW）
│   ├── compat/                      # 提示词兼容 & 文件拆分
│   ├── config/                      # 配置加载、模型别名、Ds2Config DTO
│   ├── controller/                  # REST 控制器
│   │   ├── HealthController.java    # 健康检查
│   │   ├── OpenAiController.java    # OpenAI 兼容接口
│   │   ├── OpenAiEmbeddingController.java  # 向量化接口
│   │   ├── ResponsesCompatController.java  # Responses 兼容
│   │   └── AdminController.java     # 管理接口
│   ├── filter/                      # WebFlux 过滤器 (Request ID)
│   ├── model/                       # 内部 DTO (InternalRequest, ModelMeta 等)
│   ├── pool/                        # 账号池 & Token 刷新管理
│   ├── pow/                         # DeepSeek 工作量证明求解器
│   ├── registry/                    # 模型注册服务
│   ├── runtime/                     # 核心聊天流式编排
│   ├── tool/                        # DSML 工具调用格式化 & 流解析
│   └── usage/                       # Token 用量计算
├── src/main/resources/
│   └── application.yml              # Spring Boot 配置
├── src/test/java/com/ds2api/        # 单元测试 (JUnit 5 + reactor-test)
├── config.json                       # 运行时配置（不入库，需自行创建）
├── Dockerfile                        # 多阶段 Docker 构建
├── docker-compose.yml                # Docker Compose 编排
└── pom.xml                           # Maven 构建描述
```

---

## 构建与测试

```bash
# 编译
mvn compile

# 运行测试
mvn test

# 打包（跳过测试）
mvn clean package -DskipTests

# 使用离线仓库加速依赖解析
mvn compile -Dmaven.repo.local=/tmp/m2repo
```

---

## 部署

### 多环境切换

推荐准备多个配置文件，部署时选择挂载：

```
config.json          # 默认（开发/本地）
config.prod.json     # 生产环境
```

在 `docker-compose.yml` 中通过变量控制挂载哪个文件：

```yaml
volumes:
  - ./${CONFIG_FILE:-config.json}:/data/config.json:rw
```

部署时指定：

```bash
# 生产环境
CONFIG_FILE=config.prod.json docker compose up -d

# 开发环境
docker compose up -d  # 默认使用 config.json
```

### 常用运维命令

```bash
# 构建并启动
docker compose up --build -d

# 查看日志
docker compose logs -f ds2api

# 重启服务
docker compose restart ds2api

# 热重载配置（无需重启容器，推荐）
curl -X POST http://localhost:6011/admin/reload-config \
  -H "Authorization: Bearer admin"

# 停止服务
docker compose down

# 停止并删除数据卷
docker compose down -v
```

---

## License

MIT
