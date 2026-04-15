# 测试细则

- **单元测试**：不依赖外部环境，执行时间 < 100ms，类名加 `@UnitTest` 注解（如果有自定义注解）。
- **集成测试**：使用 Testcontainers，执行时间 < 30s，类名加 `@IntegrationTest` 注解。
- **Mock 原则**：只 Mock 外部依赖（HTTP Client、文件系统），不 Mock 同一模块内的简单 POJO 或 Service。