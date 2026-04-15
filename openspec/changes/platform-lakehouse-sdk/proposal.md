## Why

`managed-app-runtime` 的租户上下文注入以环境变量为契约（`PLATFORM_CATALOG_URL`、`PLATFORM_CATALOG_TOKEN`、`PLATFORM_MINIO_ACCESS_KEY` 等），应用通过读取环境变量自行构建 Iceberg REST Catalog 客户端和 S3 客户端。这对任何语言兼容，但存在以下实际痛点：

1. **重复模板代码**：每个 Java/Python 应用都在写相同的 Catalog 客户端初始化、S3 客户端构建、Token 解析逻辑
2. **缺乏 Token 自动刷新**：应用需自行处理 JWT 过期和续期，大部分开发者会忽略这一点
3. **无命名空间强制限定**：应用可以构造请求访问其他租户的 namespace，仅依赖 Gravitino ACL 为最后防线
4. **平台 API 演进成本高**：Gravitino REST API 或 MinIO 端点变更时，需要通知所有应用侧适配

平台需要提供一套 **Lakehouse SDK**（Java + Python），屏蔽 Catalog 接口细节，让应用以一行 `PlatformLakehouse.connect()` 进入租户隔离的 Lakehouse 环境。

## What Changes

- 新增 Java SDK 模块 `platform-lakehouse-sdk-java`，提供 `PlatformLakehouse` 入口类和 `LakehouseTable`、`LakehouseS3` 便捷封装
- 新增 Python SDK 包 `platform-lakehouse-sdk`（PyPI），提供 `lakehouse.connect()` 入口和 `pyspark` 集成层
- SDK 内部从环境变量读取平台上下文（保持环境变量作为底层契约不变），自动构建 Catalog/S3 客户端
- SDK 内置 JWT Token 自动刷新（Sidecar 或 SDK 内线程定期检查 Token 有效期）
- SDK 强制将所有 Iceberg 操作限定在租户自身 namespace，禁止跨 namespace 操作
- 提供 `TestLakehouse` 测试辅助类，支持应用在本地开发时 mock 平台上下文

## Capabilities

### New Capabilities

- `lakehouse-sdk-core`: SDK 核心抽象层（语言无关），定义 `Lakehouse` 入口、`Table`、`S3Client` 等接口契约，以及环境变量读取、Token 刷新、namespace 限定等公共逻辑
- `lakehouse-sdk-java`: Java 实现，基于 Iceberg Java REST Catalog Client + AWS S3 SDK，提供 `PlatformLakehouse.connect()` 一键初始化和 `SparkSession` 预配置工厂方法
- `lakehouse-sdk-python`: Python 实现，基于 `pyiceberg` + `boto3`，提供 `lakehouse.connect()` 和 PySpark Session 预配置，发布为 PyPI 包

### Modified Capabilities

_(不修改现有 capability 需求。环境变量契约 `tenant-context-injection` 保持不变，SDK 是其上层消费者。)_

## Impact

- **新增模块**: `platform-lakehouse-sdk-java`（Maven 模块）、`platform-lakehouse-sdk`（Python 包）
- **依赖**: Java SDK 依赖 `iceberg-rest-client`、`aws-s3-sdk`；Python SDK 依赖 `pyiceberg`、`boto3`
- **兼容性**: SDK 是可选的；不使用 SDK 的应用仍然可以直读环境变量（Go、Rust、Node.js 应用不受影响）
- **版本管理**: SDK 版本须与平台版本对齐（通过 BOM 或版本矩阵管理）
- **文档**: 需要提供 SDK 使用指南和 API 参考文档
