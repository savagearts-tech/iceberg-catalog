## Why

Lakehouse 平台已提供统一 Catalog 和多租户数据隔离能力，但数据应用（ETL 作业、分析脚本、ML 训练任务）仍以临时、手工的方式部署，缺乏统一的托管运行入口。应用开发者需要自行处理 Catalog 连接注入、租户凭证配置、运行资源申请和日志采集，重复成本高且难以保证租户环境的安全边界。平台需要一个**托管应用运行时（Managed App Runtime）**层，让应用以声明式方式提交，平台自动完成租户上下文注入并在隔离的沙箱环境中执行。

## What Changes

- 新增应用注册管理 API：支持开发者将应用（Docker 镜像 / Spark 作业 / Python 脚本）注册到平台
- 新增应用运行时调度器：接收应用运行请求，按租户上下文分配运行槽位（Kubernetes Pod / Spark Session）
- 新增租户上下文注入机制：运行时自动向应用注入 Gravitino Iceberg REST Catalog 地址、JWT Bearer Token、MinIO AccessKey，应用无需感知连接配置细节
- 新增应用运行隔离沙箱：每个应用运行实例共享底层 Kubernetes 集群，但通过 Namespace / RBAC / NetworkPolicy 实现租户级进程和网络隔离
- 新增运行状态观测 API：提供应用运行实例的状态查询、日志流式获取和指标暴露接口
- 新增应用运行历史和审计日志：记录每次应用运行的租户 ID、运行时间、资源用量和退出状态，供合规审计使用

## Capabilities

### New Capabilities

- `app-registry`: 应用注册与版本管理，支持 Docker 镜像、Spark jar、Python wheel 等多种应用类型的声明式注册与元数据管理
- `app-runtime-scheduler`: 应用运行请求调度，根据租户配额和集群资源动态分配运行槽位，支持队列和优先级策略
- `tenant-context-injection`: 运行时租户上下文自动注入，包括 Catalog 地址、JWT Token、MinIO AccessKey，应用通过环境变量或挂载文件消费，无需硬编码
- `runtime-isolation-sandbox`: 应用运行沙箱，基于 Kubernetes Namespace + RBAC + NetworkPolicy 实现租户间进程和网络隔离，防止跨租户资源访问
- `app-observability`: 应用运行实例的状态查询、实时日志流、资源指标（CPU/内存/IO）暴露，供租户和平台管理员监控
- `app-audit-log`: 每次应用运行的完整审计记录，包含租户 ID、应用 ID、触发来源、资源消耗和退出状态，满足合规要求

### Modified Capabilities

_(此变更为新增能力，不影响 `unified-multi-tenant-catalog` 已有 spec 的需求；Gravitino Catalog 连接参数由新的 `tenant-context-injection` 能力消费，属于实现层扩展，不修改现有 catalog spec 的行为要求)_

## Impact

- **新增服务**: App Runtime Scheduler（Kubernetes Operator 或独立控制面）、App Registry Service
- **平台依赖**: 依赖 `unified-multi-tenant-catalog` 的 D5（JWT 身份传播）和 D6（凭证分发）作为上下文注入的数据来源
- **基础设施**: 需要 Kubernetes 集群支持，每租户独立 Namespace，NetworkPolicy 插件（Calico / Cilium）
- **安全**: 应用运行 Pod 使用专属 ServiceAccount，绑定租户专属 RBAC 角色；禁止挂载其他租户 Secret
- **计量与配额**: 租户运行配额（并发实例数、CPU 上限、内存上限）需在 App Runtime Scheduler 中管理
- **API 变更**: 新增 App Management API（注册、提交运行、查询状态、获取日志）和 Audit Log Query API
