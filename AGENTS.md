# Repository Guidelines

## Project Structure & Module Organization

```
ds2api-java/
├── src/main/java/com/ds2api/
│   ├── adapter/       # OpenAI ↔ DeepSeek protocol adaptation
│   ├── admin/         # Admin endpoints (dev capture, queue status)
│   ├── auth/          # Authentication filters & JWT utilities
│   ├── cache/         # Caffeine-based session & response caching
│   ├── client/        # DeepSeek HTTP clients (auth, session, file, PoW)
│   ├── compat/        # Prompt compatibility & file splitting
│   ├── config/        # Configuration loader, model aliases, Ds2Config DTO
│   ├── controller/    # REST controllers (OpenAI-compat, health, admin)
│   ├── filter/        # WebFlux filters (request ID)
│   ├── model/         # Internal DTOs (InternalRequest, ModelMeta, etc.)
│   ├── pool/          # Account pool & token refresh management
│   ├── pow/           # DeepSeek proof-of-work solver
│   ├── registry/      # Model registry service
│   ├── runtime/       # Core chat streaming orchestration
│   ├── tool/          # DSML tool call formatting & stream parsing
│   └── usage/         # Token usage calculation
├── src/main/resources/
│   └── application.yml
├── config.json         # Runtime configuration (accounts, keys, aliases)
├── pom.xml             # Maven build (Spring Boot 3.3.5, Java 17)
├── Dockerfile          # Multi-stage Docker build
└── docker-compose.yml  # Docker Compose with healthcheck
```

## Build, Test, and Development Commands

| Command | Purpose |
|---|---|
| `mvn compile` | Compile all sources |
| `mvn test` | Run unit tests (JUnit 5 + reactor-test) |
| `mvn clean package -DskipTests` | Build executable JAR without tests |
| `mvn spring-boot:run` | Run locally (requires `config.json` in working directory) |
| `docker compose up --build` | Build and run with Docker (port 6011 by default) |

Use `-Dmaven.repo.local=/tmp/m2repo` for offline or isolated dependency resolution.

## Coding Style & Naming Conventions

- **Java 17**, **Spring Boot 3.3.5** with **WebFlux** (Project Reactor)
- Indentation: 4 spaces (standard IntelliJ IDEA defaults in `.idea/`)
- Use **Lombok** for boilerplate reduction (`@Slf4j`, `@Data`, `@Builder`)
- Package naming: `com.ds2api.<domain>` — one package per architectural concern
- Reactive patterns: return `Mono<T>` / `Flux<T>` from service and adapter methods
- Configuration DTOs (`Ds2Config`, `Ds2ApiProperties`) follow `@ConfigurationProperties` style

## Testing Guidelines

- Framework: **JUnit 5** + **Spring Boot Test** + **reactor-test**
- Place tests under `src/test/java/com/ds2api/` mirroring the main source structure
- Run with: `mvn test`
- Use `@WebFluxTest` for controller tests and `@SpringBootTest` for integration tests

## Commit & Pull Request Guidelines

- Follow **Conventional Commits**: `feat:`, `fix:`, `refactor:`, `docs:`, `chore:`
- Chinese descriptions preferred (e.g., `feat(chat): 添加会话ID处理和响应消息ID缓存功能`)
- Scope the commit to the affected module: `chat`, `runtime`, `config`, `adapter`, etc.
- PRs should include:
  - A clear description of the change and motivation
  - At least one linked issue (if applicable)
  - Verification that `mvn test` passes and the Docker build succeeds

## Security & Configuration

- Sensitive values live in `config.json` or environment variables — **never commit real credentials**
- Environment variable overrides: `DS2API_UPSTREAM_TOKEN`, `DS2API_ADMIN_KEY`, `DS2API_LOG_LEVEL`, etc.
- Admin endpoints are protected by `AdminAuthFilter`; API endpoints use `ApiAuthFilter`
- Docker health check endpoint: `GET /healthz` (no auth required)
