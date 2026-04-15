## Context

平台需要一个 Secret Manager 来满足两个上游依赖：

1. **`unified-multi-tenant-catalog` D6**：Tenant Provisioning API 完成租户创建后，需将 MinIO AccessKey 写入 Secret Manager；租户通过一次性 Token 从 Secret Manager 领取凭证
2. **`managed-app-runtime` 上下文注入**：Init Container 在 Pod 启动时从 Secret Manager 按 `secretPath` 读取租户凭证，注入为环境变量

OQ2 的两个候选方案均有明显门槛：
- **HashiCorp Vault**：需单独部署集群，支持 HA，有学习曲线，对早期阶段而言运维成本过高
- **云托管 Secrets Manager**：绑定云厂商，不适合私有化部署场景

**简单方案**：复用平台已有的 PostgreSQL，在独立 Schema `secret_manager` 中以 AES-256-GCM 加密存储凭证，通过 Java 服务层提供与 Vault 相似的 API 语义（写入 / 读取 / 吊销），并预留接口抽象以便未来平滑迁移。

### 数据模型

```sql
-- Schema: secret_manager

CREATE TABLE secrets (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  secret_path TEXT NOT NULL UNIQUE,   -- e.g., "tenants/acme/minio"
  ciphertext  BYTEA NOT NULL,         -- AES-256-GCM encrypted value
  iv          BYTEA NOT NULL,         -- 随机 IV（96-bit）
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  revoked_at  TIMESTAMPTZ            -- NULL = active
);

CREATE TABLE one_time_tokens (
  token       TEXT PRIMARY KEY,       -- SHA-256(random 32 bytes), hex
  secret_path TEXT NOT NULL,
  expires_at  TIMESTAMPTZ NOT NULL,
  used_at     TIMESTAMPTZ             -- NULL = not yet consumed
);
```

### 加密方案

```
Master Key (256-bit) ← 来自环境变量 PLATFORM_SECRET_MASTER_KEY
     ↓
AES-256-GCM(plaintext, IV=random 96-bit)
     ↓
ciphertext + IV → 存入 PostgreSQL
```

## Goals / Non-Goals

**Goals:**
- 实现凭证加密存储（AES-256-GCM），依托平台已有 PostgreSQL，无新增基础设施
- 实现一次性 Token 凭证分发（OTT，15 分钟有效期，领取后立即失效）
- 提供 Java SDK（`SecretManagerClient` 接口 + `SimpleSecretManagerClient` 实现）
- 支持凭证吊销（租户下线时撤销 AccessKey 的存储引用）
- 预留接口抽象，未来可通过配置切换至 Vault 或云 KMS，业务代码不需改动

**Non-Goals:**
- 不实现 Vault 的动态凭证生成（Dynamic Secrets）
- 不实现细粒度访问策略（Policy）
- 不实现密钥层级（KV v2 / namespace hierarchy）
- 不实现 KMS 根密钥托管（Master Key 由运维注入，不纳入本变更范围）
- 不实现审计日志（本期 Secret Manager 操作不产生独立审计记录，依赖 PostgreSQL WAL）

## Decisions

### D1: 存储后端 — 平台已有 PostgreSQL（独立 Schema）

**决策**: 在平台 PostgreSQL 中新建 `secret_manager` Schema，而非部署独立数据库或外部服务

**理由**: 平台已有可靠的 PostgreSQL HA 部署（主从或 CloudSQLProxy），零新增基础设施，且早期租户数量（< 100）下性能完全满足需求（凭证读取 P99 < 5ms）

**替代方案考虑**:
- 独立 SQLite 文件：无 HA，不适合生产；且不支持 K8s 多副本并发访问
- Redis：缺乏持久化保障，重启丢数据风险不可接受

### D2: 加密方案 — AES-256-GCM + 随机 IV

**决策**: 每条凭证使用独立随机 96-bit IV，以 AES-256-GCM 加密，IV 与密文一同存入数据库

**理由**:
- GCM 模式提供认证加密（AEAD），防止密文篡改
- 随机 IV 保证相同明文每次密文不同，防止频率分析
- 标准 Java `javax.crypto` 原生支持，无需额外依赖

**Master Key 管理**:
- 开发环境：环境变量 `PLATFORM_SECRET_MASTER_KEY`（Base64 编码的 32 字节随机值）
- 生产环境：K8s Secret 挂载（由 Platform Operator 管理），不硬编码
- 密钥轮换：支持"双密钥并存"轮换流程（旧密钥读，新密钥写），轮换完成后单密钥读写

### D3: 一次性 Token — SHA-256(CSPRNG) + 数据库状态机

**决策**: Token 由服务端生成（`SecureRandom` 32 字节，SHA-256 取值），存入 `one_time_tokens` 表，领取时以事务方式标记 `used_at`，防止并发重用

**理由**: 无需 JWT 签名复杂性；数据库事务保证 Token 只能被消费一次（唯一性由 `SELECT FOR UPDATE` + `used_at IS NULL` 保证）

**Token 传递方式**: `credentialRef` 字段返回给调用者（格式 `ssm:tenants/<tenantId>/minio`），Token 通过单独接口返回，两者分离

### D4: SDK 接口设计 — `SecretManagerClient` 抽象 + Spring Bean 条件装配

**决策**: 定义 `SecretManagerClient` Java 接口，通过 Spring `@ConditionalOnProperty` 按配置装配 `SimpleSecretManagerClient`（本变更）或 `VaultSecretManagerClient`（未来）

```java
public interface SecretManagerClient {
    void write(String secretPath, Map<String, String> values);
    Map<String, String> read(String secretPath);
    String issueOneTimeToken(String secretPath, Duration ttl);
    Map<String, String> consumeToken(String token);
    void revoke(String secretPath);
}
```

**理由**: 接口稳定后，Vault 迁移只需新增 `VaultSecretManagerClient` Bean 实现，业务代码（Tenant Provisioning API、Init Container）零改动

## Risks / Trade-offs

| 风险 | 严重度 | 缓解措施 |
|------|--------|----------|
| PostgreSQL 宕机导致凭证不可读 | 🔴 高 | 复用平台已有 PG HA（主从 / CloudSQL HA）；Init Container 重试 3 次后失败快报错，不静默忽略 |
| Master Key 泄漏导致所有凭证暴露 | 🔴 高 | Master Key 仅通过 K8s Secret 挂载，不落日志；支持密钥轮换；定期审查 K8s Secret 访问权限 |
| 并发 Token 消费竞争 | 🟠 中 | 使用 `SELECT FOR UPDATE SKIP LOCKED` 事务保证 Token 原子消费，并发请求只有一个成功 |
| AES-256-GCM IV 碰撞（理论） | 🟡 低 | 96-bit 随机 IV 在 2^32 次加密前碰撞概率可忽略（约 1/10^9），现有租户规模内安全 |
| 未来 Vault 迁移的数据迁移成本 | 🟡 低 | 迁移脚本：解密所有现有凭证 → 重新写入 Vault；支持双写过渡期；接口抽象确保业务侧零改动 |

## Migration Plan

1. **Step 1**: 在平台 PostgreSQL 中创建 `secret_manager` Schema 和两张表（Flyway 迁移脚本）
2. **Step 2**: 实现 `SimpleSecretManagerClient` Java 实现类（含加解密、Token 生命周期）
3. **Step 3**: 将 SDK 打包为内部 Maven 模块，供 `catalog-service`（Tenant Provisioning）和 `app-runtime-init-container` 引用
4. **Step 4**: 在 `unified-multi-tenant-catalog` Tenant Provisioning API 中替换"响应体返回明文凭证"为"写入 Simple Secret Manager + 返回 credentialRef"
5. **Step 5**: 更新 `managed-app-runtime` Init Container 使用 SDK 从 Secret Manager 读取凭证

**回滚策略**: Simple Secret Manager 仅新增数据库表和 SDK 模块，不修改现有逻辑；若出现问题，可临时切回 `failOpen` 模式（跳过 Secret Manager 写入，凭证通过临时明文接口传递，仅限开发环境）。

## Open Questions

| # | 问题 | 决策前提 | 优先级 |
|---|------|----------|--------|
| OQ1 | **Master Key 初始化流程**：谁负责生成 Master Key 并注入 K8s Secret？是 Platform Operator 脚本还是手动操作？ | Step 1 前 | 🔴 紧急 |
| OQ2 | **密钥轮换触发机制**：密钥轮换是手动触发还是定时轮换？轮换期间业务中断窗口可接受范围是多少？ | 上线前 | 🟠 高 |
| OQ3 | **Token TTL 配置化**：默认 15 分钟是否满足所有场景（CI/CD 自动化可能需要更短的 TTL）？ | Step 2 前 | 🟡 中 |
