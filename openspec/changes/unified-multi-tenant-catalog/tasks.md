## 1. Infrastructure Setup

- [ ] 1.1 编写 Docker Compose 文件，包含 Gravitino Server、Gravitino Iceberg REST Server、MinIO、PostgreSQL（Gravitino 元数据后端）
- [ ] 1.2 配置 Gravitino Server 使用 PostgreSQL 作为元数据存储后端（`gravitino.entity.store=postgresql`）
- [ ] 1.3 验证 Gravitino Server 启动成功：`GET /api/version` 返回 HTTP 200
- [ ] 1.4 验证 Gravitino Iceberg REST Server 启动成功：`GET /iceberg/v1/config` 返回 HTTP 200
- [ ] 1.5 配置 MinIO 默认 Bucket `lakehouse` 并验证 S3 API 可访问

## 2. Catalog Core — Metalake 与 Catalog 注册

- [ ] 2.1 通过 Gravitino REST API 创建 platform metalake（`POST /api/metalakes`）
- [ ] 2.2 注册 MinIO-backed Iceberg Lakehouse Catalog（`provider=lakehouse-iceberg`，`warehouse=s3a://lakehouse/`）
- [ ] 2.3 注册 ClickHouse JDBC Catalog（`provider=jdbc-clickhouse`，只读模式）
- [ ] 2.4 注册 PostgreSQL JDBC Catalog（`provider=jdbc-postgresql`，读写模式）
- [ ] 2.5 编写集成测试验证 `GET /api/metalakes/{metalake}/catalogs` 返回全部 3 个注册 Catalog

## 3. Iceberg Catalog — 表管理能力验证

- [ ] 3.1 通过 Iceberg REST API 在 `lakehouse` catalog 下创建测试 namespace
- [ ] 3.2 创建 Iceberg 测试表并验证元数据和 MinIO 数据目录 `s3a://lakehouse/<ns>/` 已生成
- [ ] 3.3 验证 schema 演化：为测试表添加列（`add-column` 操作）
- [ ] 3.4 验证 snapshot 时间旅行：写入两个 snapshot 后通过 `VERSION AS OF` 查询历史快照
- [ ] 3.5 验证 snapshot 过期清理任务（手动触发）能删除过期快照和孤儿文件

## 4. MinIO Integration — 租户存储隔离

- [ ] 4.1 实现 MinIO Admin API 调用：为指定 tenantId 创建 Service Account（AccessKey + SecretKey）
- [ ] 4.2 实现 MinIO Bucket Policy：将 Service Account 权限限制在 `lakehouse/<tenantId>/` 前缀
- [ ] 4.3 验证跨租户路径访问被拒绝（tenant-A 的 AccessKey 无法访问 `lakehouse/tenant-B/`）
- [ ] 4.4 实现 MinIO AccessKey 吊销接口（用于租户下线流程）

## 5. Multi-Tenant Access Control — 多租户隔离

- [ ] 5.1 设计并实现 Tenant Management API：`POST /api/tenants`（原子化创建 namespace + AccessKey + ACL）
- [ ] 5.2 实现 Gravitino ACL 策略应用：租户默认获得自身 namespace 的 READ/WRITE 权限
- [ ] 5.3 实现角色管理：租户内 ADMIN 和 VIEWER 角色分配
- [ ] 5.4 编写测试验证 VIEWER 角色无法执行 DDL（DROP TABLE 返回 403）
- [ ] 5.5 实现 `DELETE /api/tenants/{tenantId}` 接口，支持 `archiveData` 参数
- [ ] 5.6 验证跨租户 namespace 查询通过 Gravitino API 返回 HTTP 403

## 6. Spark Integration — ETL 引擎接入

- [ ] 6.1 配置 Spark `spark-defaults.conf` 使用 Gravitino Iceberg REST Catalog（`spark.sql.catalog.lakehouse`）
- [ ] 6.2 编写 Spark 集成测试：通过 SparkSession 创建 Iceberg 表并写入数据，验证文件出现在 MinIO
- [ ] 6.3 编写 Spark 集成测试：读场景验证 Spark SQL `SELECT` 能从 MinIO 正确读取 Iceberg 数据
- [ ] 6.4 验证 Spark 跨 namespace 访问被拦截（使用 tenant-A token 访问 tenant-B namespace 报错）
- [ ] 6.5 提供 Spark 连接配置模板文档（`docs/spark-catalog-config.md`）

## 7. Airflow Integration — Pipeline 元数据接入

- [ ] 7.1 在 Airflow 中配置 HTTP Connection `gravitino_api`，指向 Gravitino Server REST API
- [ ] 7.2 实现 `GravitinoMetadataHook`（继承 `HttpHook`），实现 `list_catalogs`、`list_namespaces`、`list_tables` 方法
- [ ] 7.3 实现 `GravitinoIcebergSensor`：轮询 Iceberg 快照 API，检测新快照触发下游任务
- [ ] 7.4 编写示例 DAG 演示：读取 Gravitino 元数据 → 动态生成 Spark 处理任务
- [ ] 7.5 验证 Hook Bearer token 传递正确（Gravitino 返回 403 时 DAG 正确处理异常）

## 8. Tenant Lakehouse View — 租户视图能力

- [ ] 8.1 实现 Tenant Lakehouse Summary API：聚合返回租户的表数量、文件总量、存储大小、最新 snapshot 时间
- [ ] 8.2 实现 Tenant Catalog Connection Info API：返回租户专属 Iceberg REST URL + Spark 配置片段 + 访问 Token
- [ ] 8.3 验证新租户首次访问视图返回空 namespace 且无报错
- [ ] 8.4 验证 namespace summary 在新 snapshot 提交后 60 秒内更新

## 9. Verification & Documentation

- [ ] 9.1 编写端到端集成测试：完整租户生命周期（创建 → 写数据 → 查询 → 下线）
- [ ] 9.2 编写 `docs/architecture.md`：平台整体架构图（Gravitino 与各系统关系）
- [ ] 9.3 编写 `docs/tenant-onboarding.md`：租户接入操作手册
- [ ] 9.4 编写 `docs/open-questions.md`：记录 Gravitino 版本选型、Vault 集成、血缘追踪等待决策项
- [ ] 9.5 在 CI/CD 管道中增加 Gravitino Server 健康检查 smoke test
