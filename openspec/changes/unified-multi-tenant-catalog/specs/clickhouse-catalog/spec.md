## ADDED Requirements

> [!NOTE]
> ClickHouse JDBC Catalog **仅用于元数据发现**（schema、table、column 定义），不代理 ClickHouse 查询执行。跨引擎联邦查询（如通过 Spark/Trino 查询 ClickHouse 数据）不在此 Catalog 能力范围内，需通过 ClickHouse 原生 JDBC 连接实现。

### Requirement: ClickHouse JDBC Catalog registration
The system SHALL register ClickHouse as a read-only JDBC Catalog in Gravitino using `provider=jdbc-clickhouse`, allowing metadata discovery of ClickHouse databases, tables, and columns through the unified Catalog API.

#### Scenario: ClickHouse catalog registration
- **WHEN** a platform administrator sends `POST /api/metalakes/{metalake}/catalogs` with `provider=jdbc-clickhouse` and valid JDBC connection URI
- **THEN** Gravitino SHALL register the catalog and `GET /api/metalakes/{metalake}/catalogs/{catalog}/schemas` SHALL return ClickHouse database names

#### Scenario: Table metadata discovery from ClickHouse
- **WHEN** a client calls `GET /api/metalakes/{metalake}/catalogs/{catalog}/schemas/{db}/tables/{table}`
- **THEN** Gravitino SHALL return the ClickHouse table's column names, types, and nullable flags

### Requirement: ClickHouse catalog is read-only
The ClickHouse JDBC Catalog SHALL be registered as read-only; DDL operations (CREATE TABLE, DROP TABLE) through Gravitino SHALL be rejected with a descriptive error.

#### Scenario: Write operation rejected
- **WHEN** a client attempts to create a table via `POST /api/metalakes/.../catalogs/clickhouse/schemas/{db}/tables`
- **THEN** Gravitino SHALL return HTTP 405 Method Not Allowed with reason `CATALOG_READONLY`

### Requirement: ClickHouse Catalog pre-registration PoC validation
Before registering ClickHouse as a Gravitino Catalog in production, the system SHALL perform a PoC validation covering schema enumeration and column type mapping to confirm the selected Gravitino version supports the required metadata operations.

#### Scenario: PoC validates schema enumeration
- **WHEN** the PoC connects Gravitino to a ClickHouse instance with mixed database types (MergeTree, ReplicatedMergeTree)
- **THEN** `GET /api/metalakes/.../catalogs/clickhouse/schemas` SHALL return all non-system user databases

#### Scenario: PoC validates column type mapping
- **WHEN** a ClickHouse table contains columns of types `UInt64`, `DateTime64`, `Nullable(String)`, `Array(Float32)`
- **THEN** Gravitino SHALL map these to recognizable types without throwing a deserialization error

#### Scenario: Connection pool limits enforced
- **WHEN** more than `maxPoolSize` concurrent metadata requests arrive
- **THEN** requests exceeding the pool size SHALL queue and complete within the configured timeout, or fail-fast with HTTP 503
