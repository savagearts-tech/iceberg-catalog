# Lakehouse Catalog

基于 **Apache Iceberg** 的 Java 与 Spark 工具库：统一配置多种 Catalog（Hadoop / REST / JDBC），JDBC 路径带**可插拔连接池**，并提供 **Parquet 写入、表迁移、合并追加写入（带 JSONL 预写日志）** 等能力。

| 模块 | 说明 |
|------|------|
| [`catalog-client`](catalog-client) | Iceberg 客户端、`CatalogFactory`、`IcebergCatalogClient`、JDBC 池化扩展、`CoalescingAppendWriter`、迁移服务与示例 |
| [`spark-client`](spark-client) | 依赖 `catalog-client`（精简传递依赖），提供 Spark Session 工厂与 Iceberg 集成测试 |

**运行时版本（见根 [`pom.xml`](pom.xml)）：** Java 17、Iceberg 1.7.1、Hadoop 3.3.6、AWS SDK 2.x、Spark 3.5.4（`spark-client` 为 `provided`）。

---

## 构建

```bash
# 全模块编译与测试
./mvnw test

# 仅 catalog-client
./mvnw -pl catalog-client test
```

Windows 可使用仓库根目录的 `mvnw.cmd`，或本机已安装的 `mvn` 命令，参数相同。

---

## 核心能力（`catalog-client`）

### Catalog 工厂与配置

- **`CatalogFactory`**：按 `CatalogConfig.CatalogType` 构建并初始化 `HadoopCatalog`、`RESTCatalog` 或 **`PooledJdbcCatalog`**（JDBC 专用子类，见下）。
- **`CatalogConfig`**：Catalog 名称、默认 namespace、warehouse（`file://`、`s3://`、`s3a://`）、REST URI、JDBC URL/用户/密码、**MinIO/S3 端点与凭证**、连接池与可选 `jdbcProperties` / `catalogProperties`。

### JDBC 与连接池

- **`PooledJdbcCatalog`**：继承 `JdbcCatalog`，通过 SPI 创建 `JdbcClientPool`（默认 **HikariCP**），并实现 `Closeable`，便于与 `IcebergCatalogClient` 一起释放连接池。
- **`JdbcConnectionPoolProvider`** / **`JdbcConnectionPoolProviderFactory`**：可扩展其他池实现；池相关键值由 **`JdbcPoolProperties`** 写入 Iceberg catalog properties。

Spark 侧使用同一实现类名：

`spark.sql.catalog.<name>.catalog-impl = com.lakehouse.catalog.jdbc.pool.PooledJdbcCatalog`

### 高层客户端

- **`IcebergCatalogClient`**：`createTable` / `loadTable` / `writeRecords`（Parquet）/ `readRecords` / `ensureNamespace` / `dropTable` 等，支持从 **`CatalogConfig`** 构造（内部走 `CatalogFactory`）或单元测试里注入 `Catalog`。

### 合并追加与预写日志

- **`CoalescingAppendWriter`**：将多个已落地的 **`DataFile`** 合并为单次 `AppendFiles.commit()`，并把待提交元数据写入 **`JsonLinePendingJournal`**（JSON Lines），进程崩溃后可在下次 `open` 时重放未提交条目。
- **`CoalescingAppendWriterConfig`**：批量阈值（文件数、字节数、时间间隔）、提交重试、`flushOnClose`、journal `fsync`、**`maxJournalLinesPerFile`**（大于 0 时轮转 active journal 文件段）。

详细语义与限制见 [`catalog-client/.../writer/package-info.java`](catalog-client/src/main/java/com/lakehouse/catalog/client/writer/package-info.java) 与各类 Javadoc。

### 迁移

- **`CatalogMigrationService`**：在两个 Iceberg `Catalog` 实例之间按 namespace 迁移表注册（元数据位置等），供 **`HadoopToJdbcMigrationExample`** 与测试使用。

---

## Spark 模块（`spark-client`）

- **`SparkCatalogSessionFactory`**：REST Catalog + MinIO/S3 风格配置。
- **`SparkJdbcCatalogSessionFactory`**：JDBC + 池化 `PooledJdbcCatalog` + warehouse（常为 `s3a://`）的 Spark 配置。

Spark 与 Iceberg 运行时由部署环境提供，见该模块 `pom.xml` 的 `provided` 依赖。

---

## 示例程序

迁移示例（默认 main 在 `catalog-client` 的 exec 插件中已配置）：

```bash
./mvnw -pl catalog-client exec:java -Dexec.mainClass=com.lakehouse.catalog.HadoopToJdbcMigrationExample
```

带参数示例：

```bash
./mvnw -pl catalog-client exec:java -Dexec.mainClass=com.lakehouse.catalog.HadoopToJdbcMigrationExample -Dexec.args="--source-catalog-name source --source-warehouse s3a://warehouse/ --target-catalog-name target --target-warehouse s3a://warehouse/ --target-jdbc-url jdbc:postgresql://localhost:5432/iceberg_catalog --target-jdbc-username iceberg --target-jdbc-password iceberg"
```

其他入口类如 **`WriteParquetExample`** 可通过 `-Dexec.mainClass=...` 切换。

---

## 测试说明

### 默认单元测试

```bash
./mvnw -pl catalog-client test
```

`catalog-client` 默认 **排除** JUnit 5 `@Tag("slow")` 用例；包含全部 slow 测试时：

```bash
./mvnw -pl catalog-client test -Dtests.excludeGroups=
```

### 与 JDBC / 工厂相关的快速子集

```bash
./mvnw -pl catalog-client -Dtest=CatalogFactoryTest,CatalogConfigTest,IcebergCatalogClientJdbcCatalogTest,CatalogMigrationServiceTest test
```

### Spark 模块

```bash
./mvnw -pl spark-client -Dtest=SparkJdbcCatalogSessionFactoryTest,SparkIcebergJDBCIntegrationTest test
```

### 需要本机 MinIO 的集成测试

以下测试在连接 **`http://127.0.0.1:9000`** 且使用默认凭证（如 `minioadmin` / `minioadmin`）和 **`warehouse` bucket** 成功时执行；否则 **`assumeTrue` 跳过**（Maven 仍为成功）：

| 测试类 | 用途 |
|--------|------|
| `PooledJdbcCatalogMinioIntegrationTest` | 断言 `CatalogFactory` 产出 `PooledJdbcCatalog`，S3 warehouse 上建表读写 |
| `CoalescingAppendWriterMinioIntegrationTest` | `CoalescingAppendWriter` 在 JDBC + MinIO 上的批量提交与 journal 恢复 |
| `CatalogMigrationServiceMinioIntegrationTest` | Hadoop → JDBC 迁移链路 |

示例：

```bash
./mvnw -pl catalog-client -Dtest=PooledJdbcCatalogMinioIntegrationTest test
./mvnw -pl catalog-client -Dtest=CoalescingAppendWriterMinioIntegrationTest test
```

使用 **Testcontainers** 的测试（如 `IcebergCatalogClientIntegrationTest`）会自行拉起容器，不依赖本机 MinIO。

### Windows

部分测试会在本机临时目录生成占位 **`winutils.exe`** 并设置 `hadoop.home.dir`，以满足 Hadoop 在 Windows 上的路径检查。

---

## 配置片段速查

### Java：`CatalogConfig` + JDBC + S3 warehouse（MinIO）

```java
CatalogConfig config = CatalogConfig.builder()
        .catalogType(CatalogConfig.CatalogType.JDBC)
        .catalogName("jdbc-test-catalog")
        .defaultNamespace("my_tenant")
        .warehousePath("s3a://warehouse/my-prefix/")
        .jdbcUrl("jdbc:h2:file:/tmp/catalog-db")
        .jdbcUsername("sa")
        .jdbcPassword("")
        .jdbcPoolProvider("hikari")
        .jdbcPoolMaxSize(8)
        .jdbcPoolMinIdle(2)
        .minioEndpoint("http://localhost:9000")
        .minioAccessKey("minioadmin")
        .minioSecretKey("minioadmin")
        .minioRegion("us-east-1")
        .build();

try (IcebergCatalogClient client = new IcebergCatalogClient(config)) {
    client.ensureNamespace();
    // ...
}
```

### Spark：JDBC Catalog + 池实现类

```java
SparkSession spark = SparkJdbcCatalogSessionFactory.builder()
        .catalogName("jdbc_lakehouse")
        .jdbcUrl("jdbc:postgresql://localhost:5432/iceberg_catalog")
        .jdbcUsername("iceberg")
        .jdbcPassword("iceberg")
        .warehousePath("s3a://lakehouse/")
        .jdbcPoolProvider("hikari")
        .jdbcPoolMaxSize(10)
        .jdbcPoolMinIdle(2)
        .build()
        .createSession();

spark.sql("CREATE NAMESPACE IF NOT EXISTS jdbc_lakehouse.db");
spark.sql("SELECT * FROM jdbc_lakehouse.db.orders").show(false);
```

### 合并追加写入（节选）

```java
Path journalDir = Paths.get("/var/lib/myapp/iceberg-journal");
CoalescingAppendWriterConfig cfg = CoalescingAppendWriterConfig.builder()
        .maxPendingFiles(50)
        .fsyncJournal(true)
        .maxJournalLinesPerFile(100_000)  // 0 表示不轮转
        .build();

try (CoalescingAppendWriter writer = CoalescingAppendWriter.open(client, "my_table", journalDir, cfg)) {
    writer.stage(dataFile);
}
```

---

## 设计说明

- **REST 与 JDBC** 使用两套独立的 Spark Session 工厂，配置边界清晰，避免混用一套 giant config。
- **JDBC 路径**下 `CatalogFactory` 固定使用 **`PooledJdbcCatalog`**，以便统一关闭连接池与行为一致的池参数。

---

## 仓库布局（摘要）

```
catalog-client/src/main/java/com/lakehouse/catalog/
  client/          # IcebergCatalogClient, writer (CoalescingAppendWriter, JsonLinePendingJournal, …)
  config/          # CatalogConfig
  factory/         # CatalogFactory
  jdbc/pool/       # PooledJdbcCatalog, Hikari provider, pool SPI
  migration/       # CatalogMigrationService
  *.java           # WriteParquetExample, HadoopToJdbcMigrationExample, …

spark-client/src/main/java/com/lakehouse/spark/
  SparkCatalogSessionFactory.java
  SparkJdbcCatalogSessionFactory.java
  …
```

更多细节以源码与测试为准；欢迎针对具体类补充文档或示例。
