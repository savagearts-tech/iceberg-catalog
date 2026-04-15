# GEMINI.md — Antigravity 专属配置

## 交互偏好
- 使用**中文**回复所有问题和解释。
- 代码注释和 Javadoc 描述使用**英文**（便于国际化），但 Javadoc 中的业务说明可保留中文。
- 生成代码时自动添加 `@author` 和 `@since` 标签。

## 代码生成风格
- **依赖注入**：优先使用 Lombok `@RequiredArgsConstructor`。
- **HTTP 客户端**：使用 Spring Boot 3.4 推荐的 `RestClient` 或 `WebClient`，**禁止**使用已弃用的 `RestTemplate`。
- **DTO 命名**：请求体用 `XxxRequest`，响应体用 `XxxResponse`。
- **测试类命名**：`XxxTest` 或 `XxxIntegrationTest`。

## 日志规范
- 统一使用 Lombok `@Slf4j` 注解。
- 业务入口记录 `INFO` 日志，调试细节用 `DEBUG`。
- 异常捕获必须打印堆栈：`log.error("Failed to create namespace: {}", name, e);`

## 与 OpenSpec 协作提示
- 当你需要开发重大功能时，我会先通过 `/openspec:propose` 创建规格。
- 任务完成后，请提醒我执行 `/openspec:archive` 归档变更。