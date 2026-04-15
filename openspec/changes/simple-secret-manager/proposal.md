## Why

`unified-multi-tenant-catalog` 的 D6 决策（凭证分发模型）和 `managed-app-runtime` 的上下文注入机制均依赖一个 Secret Manager 作为凭证存储后端，但 OQ2 将 Secret Manager 选型标记为 🔴 紧急。完整部署 HashiCorp Vault 或云托管 Secrets Manager 引入新的基础设施依赖和运维复杂度，不适合作为平台早期阶段的起步选择。本变更决定使用**平台已有的 PostgreSQL + AES-256-GCM 加密**实现一个轻量 Secret Manager，消除外部依赖，解锁 Phase 0 的进展。

## What Changes

- 新增 Simple Secret Manager 服务（PostgreSQL-backed），提供凭证的加密存储、读取和吊销能力
- 新增一次性令牌（One-Time Token）凭证领取机制，实现 D6 中定义的安全分发流程
- 新增 Secret Manager SDK（Java 库），供 `unified-multi-tenant-catalog` 的 Tenant Provisioning API 和 `managed-app-runtime` 的 Init Container 统一调用
- 明确 Simple Secret Manager 为**阶段性方案**，预留标准 Secret Manager 接口抽象，便于未来迁移至 Vault

## Capabilities

### New Capabilities

- `secret-store`: 凭证的加密存储与生命周期管理，基于 PostgreSQL 存储加密后的凭证，支持按 `secretPath` 读写和吊销
- `credential-distribution`: 一次性令牌（OTT）发放与凭证领取流程，令牌有效期可配置（默认 15 分钟），领取后令牌自动失效
- `secret-manager-sdk`: 平台内部 Java SDK，封装 Secret Manager 客户端接口（`SecretManagerClient`），实现可替换的接口抽象，当前默认使用 Simple 实现

### Modified Capabilities

_(无现有 spec 需变更)_

## Impact

- **依赖**: 复用平台已有 PostgreSQL 实例（新增独立 Schema `secret_manager`），无新增基础设施
- **安全**: 加密密钥（Master Key）以环境变量方式注入，不存入数据库；采用 AES-256-GCM + 随机 IV，支持密钥轮换
- **上游消费方**:
  - `unified-multi-tenant-catalog` Tenant Provisioning API：写入 MinIO AccessKey 至 Secret Manager
  - `managed-app-runtime` Init Container：从 Secret Manager 按 secretPath 读取租户凭证
- **限制**: 不支持动态凭证生成（Vault dynamic secrets 模式）；不支持细粒度审计策略（Vault policy）；预期在租户数 < 1000 时的规模内可用
- **未来迁移路径**: `SecretManagerClient` 接口定义不变，未来可通过配置切换至 Vault 或云 KMS 实现
