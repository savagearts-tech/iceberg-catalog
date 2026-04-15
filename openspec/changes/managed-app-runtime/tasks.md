## 1. Platform Prerequisites

- [ ] 1.1 确认 Kubernetes 集群已安装 NetworkPolicy 插件（Calico 或 Cilium），验证 NetworkPolicy 生效
- [ ] 1.2 在 K8s 集群中部署 Spark Operator（锁定与 Spark 3.4 兼容的版本）
- [ ] 1.3 确认 `unified-multi-tenant-catalog` 的 Phase 1 已完成（Gravitino 可用、Vault 可用、IdP 可用）
- [ ] 1.4 解答 OQ1（Webhook vs Init Container）和 OQ4（应用类型范围），更新 design.md

## 2. App Registry Service

- [ ] 2.1 创建 App Registry Service 模块，定义应用注册数据模型（AppRegistration、AppVersion 实体）
- [ ] 2.2 实现 `POST /api/apps` 应用注册接口，含 JAR 路径跨租户校验逻辑
- [ ] 2.3 实现 `GET /api/apps/{appId}/versions` 版本列表接口
- [ ] 2.4 实现 `DELETE /api/apps/{appId}/versions/{version}` 接口，含活跃实例检查
- [ ] 2.5 编写应用注册集成测试（正常注册、跨租户 JAR 路径拒绝、重复注册创建新版本）

## 3. K8s Namespace 与隔离基础设施

- [ ] 3.1 实现 `TenantNamespaceProvisioner`：为每个租户创建 K8s Namespace `tenant-<tenantId>`
- [ ] 3.2 为每个租户 Namespace 创建专属 ServiceAccount `platform-app-sa` 和最小权限 RoleBinding
- [ ] 3.3 为每个租户 Namespace 应用 ResourceQuota（初始默认值：8 CPU、16Gi Memory、10 Pods）
- [ ] 3.4 为每个租户 Namespace 应用 NetworkPolicy：默认 deny-all + Catalog 服务白名单
- [ ] 3.5 验证隔离：租户 A 的 Pod 无法访问租户 B 的 Namespace Pod（NetworkPolicy 测试）
- [ ] 3.6 验证隔离：租户 A 的 ServiceAccount 无法 list 租户 B 的 K8s 资源（RBAC 测试）
- [ ] 3.7 实现平台 API `PUT /api/tenants/{tenantId}/quota` 动态调整 ResourceQuota

## 4. 租户上下文注入

- [ ] 4.1 实现平台 Init Container 镜像：从 Vault 获取 MinIO AccessKey + JWT，写入共享 emptyDir Volume
- [ ] 4.2 实现 Mutating Admission Webhook（或 Init Container 注入逻辑）：自动向应用 Pod 注入环境变量
- [ ] 4.3 实现 Spark 类型应用的 `spark-defaults.conf` 模板生成与 Volume 挂载
- [ ] 4.4 验证注入透明性：原始应用镜像无需修改，通过 `env | grep PLATFORM_` 验证注入结果
- [ ] 4.5 验证凭证安全性：`kubectl get pod -o yaml` 不含明文 AccessKey（使用 emptyDir + init 注入方案）
- [ ] 4.6 验证 Init Container 失败阻断启动：模拟 Vault 不可达，确认应用容器不启动并返回 `CONTEXT_INJECTION_FAILED`

## 5. App Runtime Scheduler

- [ ] 5.1 实现运行提交 API `POST /api/runs`，创建 Run 记录（初始 `QUEUED` 状态）
- [ ] 5.2 实现 ResourceQuota 余量校验：提交时预检配额，不足时返回 `QUOTA_EXCEEDED`
- [ ] 5.3 实现 FIFO 调度器：监听 `QUEUED` 队列，按提交时间顺序调度可运行的 Run
- [ ] 5.4 实现 Docker 类型 Run 的 K8s Pod 创建（含 Init Container 注入、NetworkPolicy 关联）
- [ ] 5.5 实现 Spark 类型 Run 的 SparkApplication CRD 创建（Spark Operator 集成）
- [ ] 5.6 实现运行取消 API `DELETE /api/runs/{runId}`（QUEUED 直接取消 / RUNNING 发送 SIGTERM）
- [ ] 5.7 验证多租户并发提交：10 个租户同时提交，各自 FIFO 顺序正确，配额互不干扰

## 6. App Observability

- [ ] 6.1 实现 `GET /api/runs/{runId}` 状态查询，含 K8s Pod 状态映射逻辑
- [ ] 6.2 实现 `GET /api/runs/{runId}/logs?follow=true` 实时日志流（SSE 或 chunked transfer）
- [ ] 6.3 实现日志持久化：Run 完成后日志归档，保留 7 天
- [ ] 6.4 实现 `GET /api/runs/{runId}/metrics` 指标快照（从 K8s metrics-server 获取）
- [ ] 6.5 实现 `GET /api/runs` 带过滤的分页列表（status、appId、时间范围）
- [ ] 6.6 验证跨租户日志访问拒绝（403 Forbidden 测试）

## 7. Audit Logging

- [ ] 7.1 设计并创建 PostgreSQL `app_run_audit_log` 表 schema（迁移脚本）
- [ ] 7.2 实现审计事件发布：在每个 Run 生命周期转换点写入 PostgreSQL
- [ ] 7.3 实现租户 Iceberg 审计表自动初始化（`lakehouse.<tenantId>.platform_app_audit`）
- [ ] 7.4 实现审计日志异步双写至 Iceberg（独立写入线程，失败重试 3 次 + 死信队列）
- [ ] 7.5 实现平台管理员审计查询 API `GET /api/audit-logs`（支持 tenantId、日期范围过滤）
- [ ] 7.6 验证审计不可变性：确认无 API 支持修改或删除单条审计记录
- [ ] 7.7 验证 Iceberg 写入失败不影响应用主流程（模拟 MinIO 不可达，确认应用正常完成）

## 8. 端到端验证与文档

- [ ] 8.1 编写端到端集成测试：注册应用 → 提交运行 → 验证 Catalog 上下文注入 → 应用读写 Iceberg 成功 → 审计日志可查
- [ ] 8.2 验证网络隔离：运行中应用只能访问 Catalog 白名单，外部 IP 访问被拒
- [ ] 8.3 编写 `docs/app-onboarding.md`：应用开发者如何注册和提交应用
- [ ] 8.4 编写 `docs/platform-context-contract.md`：平台注入的环境变量规范（应用集成契约）
- [ ] 8.5 回答 OQ2（JWT 刷新策略）并在 Init Container 中实现对应方案
