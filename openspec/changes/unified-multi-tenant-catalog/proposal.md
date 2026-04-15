## Why

当前平台缺乏统一的数据资产管控入口，MinIO、Iceberg、ClickHouse、PostgreSQL、Spark 和 Airflow 各自孤立，数据团队无法以统一视图发现、治理和访问跨系统的数据资产。此外，数据团队无法追踪跨系统的数据流向（MinIO → Spark ETL → ClickHouse 报表），影响数据问题排查和合规审计。随着多租户 Lakehouse 需求的增长，亟需一个具备权限隔离、数据发现、多引擎互通和数据血缘追踪能力的统一 Catalog 层。

## What Changes

- 引入 Apache Gravitino 作为统一 Catalog 服务，提供跨异构数据源的元数据统一视图
- 接入 MinIO（S3 兼容对象存储）作为 Lakehouse 底层存储，通过 Iceberg REST Catalog 管理表元数据
- 接入 ClickHouse 作为高性能 OLAP 查询引擎，注册为 Gravitino 的 JDBC Catalog
- 接入 PostgreSQL 作为关系型数据源，注册为 Gravitino 的 JDBC Catalog
- 接入 Spark 通过 Gravitino Iceberg REST Catalog 进行批处理和 ETL 作业
- 接入 Airflow 通过 Gravitino 元数据 API 动态发现数据资产，实现元数据驱动的 DAG 参数化（数据血缘回写为 Phase 2 范围）
- 实现多租户隔离：租户只能看到和访问自身被授权的 Catalog、Schema 和 Table
- 提供租户级别的 Lakehouse 视图：每租户独立的 Iceberg 命名空间、存储路径前缀和访问凭证

## Capabilities

### New Capabilities

- `catalog-core`: Gravitino 统一 Catalog 服务的安装、配置和核心 API 能力，支持多 Catalog 注册和元数据聚合
- `minio-integration`: MinIO 作为 S3 兼容存储的接入，提供对象存储后端和 Iceberg 数据文件存储
- `iceberg-catalog`: Apache Iceberg REST Catalog 注册至 Gravitino，支持表创建、schema 演化和快照管理
- `clickhouse-catalog`: ClickHouse 通过 JDBC 注册为 Gravitino Catalog，**仅限元数据发现**（schema、table、column），不支持跨引擎联邦查询
- `postgresql-catalog`: PostgreSQL 通过 JDBC 注册为 Gravitino Catalog，支持关系型数据资产统一管理
- `spark-integration`: Spark 通过 Gravitino Iceberg REST Catalog 连接，支持批量 ETL 和数据湖读写
- `airflow-integration`: Airflow 通过 Gravitino REST API 动态发现数据资产（catalog、namespace、table、snapshot），驱动元数据参数化的 DAG 执行；数据血缘回写不在本期范围内
- `multi-tenant-access-control`: 基于 Ranger 或 Gravitino 内置权限模型的多租户访问控制，租户只见自己授权资产
- `tenant-lakehouse-view`: 每租户独立的 Lakehouse 视图，包含专属命名空间、MinIO 路径前缀和 Iceberg catalog 入口

### Modified Capabilities

_(无现有 spec 需变更)_

## Impact

- **新增服务**: Apache Gravitino Server（元数据服务）、Gravitino Iceberg REST Server
- **存储**: MinIO Bucket 按租户前缀分区（`s3a://lakehouse/<tenant-id>/`）
- **依赖**: Gravitino Java Client、Iceberg REST Catalog、Spark Gravitino Connector、JDBC Drivers（ClickHouse / PostgreSQL）
- **安全**: 租户级 AccessKey/SecretKey 隔离，Ranger 策略或 Gravitino 原生 ACL
- **运维**: Airflow DAG 需要更新连接配置以使用 Gravitino 元数据端点
- **API 变更**: 新增 Catalog Management REST API 供平台管理员注册和管理数据源
