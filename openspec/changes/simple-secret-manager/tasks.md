## 1. 数据库 Schema 与 Flyway 迁移

- [ ] 1.1 在平台 PostgreSQL 中创建 `secret_manager` Schema（Flyway 迁移脚本 `V1__create_secret_manager_schema.sql`）
- [ ] 1.2 创建 `secrets` 表（id, secret_path, ciphertext, iv, created_at, updated_at, revoked_at）
- [ ] 1.3 创建 `one_time_tokens` 表（token, secret_path, expires_at, used_at）
- [ ] 1.4 创建 `secret_path` 唯一索引和 `expires_at` 索引（用于过期清理查询性能）
- [ ] 1.5 在开发环境验证 Flyway 迁移脚本执行成功，表结构与 design.md 一致

## 2. SDK 接口模块（platform-secret-manager-api）

- [ ] 2.1 新建 Maven 模块 `platform-secret-manager-api`，不引入 PostgreSQL / JPA 依赖
- [ ] 2.2 定义 `SecretManagerClient` Java 接口（write / read / revoke / issueOneTimeToken / consumeToken）
- [ ] 2.3 定义异常类：`SecretManagerConfigException`、`SecretNotFoundException`、`SecretRevokedException`、`SecretIntegrityException`、`InvalidTokenException`
- [ ] 2.4 验证 `platform-secret-manager-api` 的 Maven 依赖树中无 `postgresql` / `spring-data-jpa` compile/runtime scope

## 3. SimpleSecretManagerClient 实现

- [ ] 3.1 新建 Maven 模块 `platform-secret-manager-simple`，依赖 `platform-secret-manager-api` + Spring Data JPA + PostgreSQL JDBC
- [ ] 3.2 实现 `SecretEncryptor`：AES-256-GCM 加密/解密，Master Key 从 `PLATFORM_SECRET_MASTER_KEY` 环境变量读取，缺失时抛 `SecretManagerConfigException`
- [ ] 3.3 实现 `write(secretPath, values)`：序列化 Map → JSON → AES-GCM 加密 → 持久化，支持覆盖更新
- [ ] 3.4 实现 `read(secretPath)`：查询激活（`revoked_at IS NULL`）记录 → AES-GCM 解密 → 返回 Map
- [ ] 3.5 实现 `revoke(secretPath)`：更新 `revoked_at = now()`，幂等处理
- [ ] 3.6 实现 `issueOneTimeToken(secretPath, ttl)`：CSPRNG 32 字节 → SHA-256 hex → 插入 `one_time_tokens`
- [ ] 3.7 实现 `consumeToken(token)`：`SELECT FOR UPDATE` 事务验证（非空、未使用、未过期）→ 设置 `used_at` → 调用 `read` 返回值
- [ ] 3.8 实现 `@Scheduled` 过期 Token 清理任务（可配置周期，默认 1 小时）
- [ ] 3.9 实现 Spring `@ConditionalOnProperty(name="platform.secret-manager.type", havingValue="simple", matchIfMissing=true)` 自动配置

## 4. 单元测试与集成测试

- [ ] 4.1 编写 `SecretEncryptorTest`：加密后解密还原原始值；篡改密文触发 `SecretIntegrityException`；缺失 Master Key 触发 `SecretManagerConfigException`
- [ ] 4.2 编写 `SimpleSecretManagerClientTest`（使用 Testcontainers PostgreSQL）：
  - write → read 正常场景
  - read 不存在路径 → `SecretNotFoundException`
  - read 已吊销路径 → `SecretRevokedException`
  - revoke 幂等性
- [ ] 4.3 编写 `CredentialDistributionTest`：OTT 发放 → 消费成功；OTT 二次消费 → `TOKEN_ALREADY_USED`；OTT 过期 → `TOKEN_EXPIRED`；并发消费 → 只有一个成功
- [ ] 4.4 验证 `@ConditionalOnProperty` 装配：`type=simple`（默认）使用 Simple 实现；注入 Mock 替代实现时 Simple 不激活

## 5. 上游集成 — unified-multi-tenant-catalog

- [ ] 5.1 在 `catalog-service` 的 `pom.xml` 中引入 `platform-secret-manager-api` 和 `platform-secret-manager-simple` 依赖
- [ ] 5.2 修改 `TenantProvisioningService.createTenant()`：MinIO AccessKey 生成后调用 `SecretManagerClient.write`，Secret Manager 写入失败时回滚租户创建
- [ ] 5.3 修改 Tenant Provisioning API 响应体：移除 `accessKey`/`secretKey` 字段，添加 `credentialRef`（OTT token）
- [ ] 5.4 实现 `GET /api/tenants/{tenantId}/credentials?token=<ott>` 端点：调用 `consumeToken` 返回凭证
- [ ] 5.5 编写集成测试：`POST /api/tenants` 响应体不含明文凭证，`GET .../credentials?token=<ott>` 返回正确 AccessKey

## 6. 上游集成 — managed-app-runtime Init Container

- [ ] 6.1 在 Init Container 项目中引入 `platform-secret-manager-api` 依赖
- [ ] 6.2 实现 `InitContainerCredentialFetcher`：读取 `PLATFORM_CREDENTIAL_TOKEN` 环境变量 → 调用 `consumeToken` → 将凭证写入 `/platform/secrets/` 共享 Volume
- [ ] 6.3 实现容错逻辑：`consumeToken` 失败时以非零退出码退出，日志输出错误类型（`TOKEN_EXPIRED` / `TOKEN_ALREADY_USED` / `SECRET_MANAGER_UNAVAILABLE`）
- [ ] 6.4 编写 Init Container 集成测试：有效 Token → 凭证文件正确写入；过期 Token → 非零退出；二次消费 → 非零退出

## 7. 验证与文档

- [ ] 7.1 端到端验证：租户创建 → OTT 领取 → Init Container 启动 → 应用通过注入凭证访问 MinIO 成功
- [ ] 7.2 生成 Master Key 的运维脚本（`scripts/generate-master-key.sh`）并写入 `docs/secret-manager-ops.md`
- [ ] 7.3 在 `unified-multi-tenant-catalog` design.md 的 OQ2 中标注为"已决策：采用 Simple Secret Manager，见变更 `simple-secret-manager`"
