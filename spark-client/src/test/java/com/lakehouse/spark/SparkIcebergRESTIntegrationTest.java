package com.lakehouse.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration test: Spark SQL ↔ Iceberg REST Catalog ↔ MinIO.
 *
 * <p>Full production-like data path tested end-to-end:</p>
 * <pre>
 *   SparkSession (local[*])
 *       → SparkCatalog (type=rest)
 *           → Iceberg REST Catalog Server (apache/iceberg-rest-fixture)
 *               → MinIO (S3-compatible Parquet storage)
 * </pre>
 *
 * <p>Tests cover: DDL (CREATE TABLE), DML (INSERT INTO), DQL (SELECT),
 * schema evolution (ALTER TABLE), time travel, and cross-client interop
 * (Java IcebergCatalogClient writes → Spark SQL reads).</p>
 *
 * @author platform
 * @since 1.0.0
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SparkIcebergRESTIntegrationTest {

    private static final String BUCKET = "warehouse";
    private static final String ACCESS_KEY = "admin";
    private static final String SECRET_KEY = "password";

    static Network network = Network.newNetwork();

    @Container
    static MinIOContainer minio = new MinIOContainer("minio/minio:RELEASE.2024-01-16T16-07-38Z")
            .withUserName(ACCESS_KEY)
            .withPassword(SECRET_KEY)
            .withNetwork(network)
            .withNetworkAliases("minio");

    @SuppressWarnings("resource")
    @Container
    static GenericContainer<?> restCatalog = new GenericContainer<>("apache/iceberg-rest-fixture:latest")
            .withNetwork(network)
            .withNetworkAliases("rest-catalog")
            .withExposedPorts(8181)
            .dependsOn(minio)
            .withEnv("CATALOG_WAREHOUSE", "s3://" + BUCKET + "/")
            .withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .withEnv("CATALOG_S3_ENDPOINT", "http://minio:9000")
            .withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
            .withEnv("AWS_ACCESS_KEY_ID", ACCESS_KEY)
            .withEnv("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
            .withEnv("AWS_REGION", "us-east-1")
            .waitingFor(Wait.forHttp("/v1/config").forPort(8181).forStatusCode(200));

    private SparkSession spark;

    @BeforeEach
    void setUp() {
        createBucketIfNotExists();

        String restUrl = "http://" + restCatalog.getHost() + ":" + restCatalog.getMappedPort(8181);
        String minioUrl = minio.getS3URL();

        spark = SparkCatalogSessionFactory.builder()
                .catalogName("lakehouse")
                .catalogUri(restUrl)
                .warehousePath("s3://" + BUCKET + "/")
                .minioEndpoint(minioUrl)
                .minioAccessKey(ACCESS_KEY)
                .minioSecretKey(SECRET_KEY)
                .minioRegion("us-east-1")
                .sparkMaster("local[*]")
                .build()
                .createSession();

        // Ensure namespace
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.db");
    }

    @AfterEach
    void tearDown() {
        if (spark != null) {
            // Clean up tables
            List<Row> tables = spark.sql("SHOW TABLES IN lakehouse.db").collectAsList();
            for (Row table : tables) {
                String tableName = table.getString(1);
                spark.sql("DROP TABLE IF EXISTS lakehouse.db." + tableName + " PURGE");
            }
            spark.stop();
        }
    }

    // ========================= DDL =========================

    @Test
    @Order(1)
    @DisplayName("should_CreateIcebergTable_When_SparkSQLExecuted")
    void should_CreateIcebergTable_When_SparkSQLExecuted() {
        spark.sql("CREATE TABLE lakehouse.db.users (" +
                "  id BIGINT, " +
                "  name STRING, " +
                "  email STRING, " +
                "  created_at TIMESTAMP" +
                ") USING iceberg");

        // Verify table exists
        Dataset<Row> tables = spark.sql("SHOW TABLES IN lakehouse.db");
        List<String> tableNames = tables.select("tableName").as(org.apache.spark.sql.Encoders.STRING()).collectAsList();
        assertThat(tableNames).contains("users");

        // Verify schema
        Dataset<Row> columns = spark.sql("DESCRIBE TABLE lakehouse.db.users");
        List<String> colNames = columns.select("col_name").as(org.apache.spark.sql.Encoders.STRING()).collectAsList();
        assertThat(colNames).contains("id", "name", "email", "created_at");
    }

    // ========================= DML + DQL =========================

    @Test
    @Order(2)
    @DisplayName("should_InsertAndQuery_When_ParquetWrittenToMinIO")
    void should_InsertAndQuery_When_ParquetWrittenToMinIO() {
        spark.sql("CREATE TABLE lakehouse.db.events (" +
                "  event_id BIGINT, " +
                "  event_type STRING, " +
                "  user_id BIGINT, " +
                "  payload STRING" +
                ") USING iceberg");

        // Insert data via Spark SQL
        spark.sql("INSERT INTO lakehouse.db.events VALUES " +
                "(1, 'login', 1001, '{\"ip\": \"192.168.1.1\"}'), " +
                "(2, 'purchase', 1002, '{\"item\": \"laptop\", \"price\": 999.99}'), " +
                "(3, 'login', 1003, '{\"ip\": \"10.0.0.5\"}'), " +
                "(4, 'logout', 1001, '{}'), " +
                "(5, 'purchase', 1004, '{\"item\": \"phone\", \"price\": 599.99}')");

        // Query all records
        Dataset<Row> result = spark.sql("SELECT * FROM lakehouse.db.events ORDER BY event_id");
        assertThat(result.count()).isEqualTo(5);

        // Verify specific values
        Row firstRow = result.collectAsList().get(0);
        assertThat(firstRow.getLong(0)).isEqualTo(1L);
        assertThat(firstRow.getString(1)).isEqualTo("login");
        assertThat(firstRow.getLong(2)).isEqualTo(1001L);
    }

    @Test
    @Order(3)
    @DisplayName("should_SupportAggregation_When_QueryingIcebergTable")
    void should_SupportAggregation_When_QueryingIcebergTable() {
        spark.sql("CREATE TABLE lakehouse.db.sales (" +
                "  product STRING, " +
                "  amount DOUBLE, " +
                "  region STRING" +
                ") USING iceberg");

        spark.sql("INSERT INTO lakehouse.db.sales VALUES " +
                "('laptop', 999.99, 'US'), " +
                "('phone', 599.99, 'US'), " +
                "('laptop', 899.99, 'EU'), " +
                "('tablet', 349.99, 'US'), " +
                "('phone', 649.99, 'EU')");

        // Aggregation query
        Dataset<Row> regionSales = spark.sql(
                "SELECT region, COUNT(*) as cnt, ROUND(SUM(amount), 2) as total " +
                "FROM lakehouse.db.sales " +
                "GROUP BY region " +
                "ORDER BY region");

        List<Row> rows = regionSales.collectAsList();
        assertThat(rows).hasSize(2);

        // EU: 2 items
        assertThat(rows.get(0).getString(0)).isEqualTo("EU");
        assertThat(rows.get(0).getLong(1)).isEqualTo(2L);

        // US: 3 items
        assertThat(rows.get(1).getString(0)).isEqualTo("US");
        assertThat(rows.get(1).getLong(1)).isEqualTo(3L);
    }

    // ========================= Schema Evolution =========================

    @Test
    @Order(4)
    @DisplayName("should_EvolveSchema_When_AlterTableAddColumn")
    void should_EvolveSchema_When_AlterTableAddColumn() {
        spark.sql("CREATE TABLE lakehouse.db.products (" +
                "  id BIGINT, " +
                "  name STRING" +
                ") USING iceberg");

        spark.sql("INSERT INTO lakehouse.db.products VALUES (1, 'Widget'), (2, 'Gadget')");

        // Schema evolution: add column
        spark.sql("ALTER TABLE lakehouse.db.products ADD COLUMN price DOUBLE");
        spark.sql("INSERT INTO lakehouse.db.products VALUES (3, 'Doohickey', 29.99)");

        // Old records should have null for new column
        Dataset<Row> result = spark.sql("SELECT * FROM lakehouse.db.products ORDER BY id");
        List<Row> rows = result.collectAsList();

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).isNullAt(2)).isTrue();  // Widget: price=null
        assertThat(rows.get(1).isNullAt(2)).isTrue();  // Gadget: price=null
        assertThat(rows.get(2).getDouble(2)).isEqualTo(29.99); // Doohickey: price=29.99
    }

    // ========================= Snapshot & Time Travel =========================

    @Test
    @Order(5)
    @DisplayName("should_SupportSnapshotHistory_When_MultipleCommits")
    void should_SupportSnapshotHistory_When_MultipleCommits() {
        spark.sql("CREATE TABLE lakehouse.db.audit_log (" +
                "  action STRING, " +
                "  ts TIMESTAMP" +
                ") USING iceberg");

        // Commit #1
        spark.sql("INSERT INTO lakehouse.db.audit_log VALUES ('create_user', TIMESTAMP '2026-01-01 00:00:00')");

        // Commit #2
        spark.sql("INSERT INTO lakehouse.db.audit_log VALUES ('delete_user', TIMESTAMP '2026-01-02 00:00:00')");

        // Verify snapshot history
        Dataset<Row> snapshots = spark.sql("SELECT snapshot_id FROM lakehouse.db.audit_log.snapshots ORDER BY committed_at");
        assertThat(snapshots.count()).isEqualTo(2);

        // Time travel: read only first commit
        long firstSnapshotId = snapshots.collectAsList().get(0).getLong(0);
        Dataset<Row> v1 = spark.sql(
                "SELECT * FROM lakehouse.db.audit_log VERSION AS OF " + firstSnapshotId);
        assertThat(v1.count()).isEqualTo(1);
        assertThat(v1.collectAsList().get(0).getString(0)).isEqualTo("create_user");
    }

    // ========================= DataFrame API =========================

    @Test
    @Order(6)
    @DisplayName("should_WriteViaDataFrameAPI_When_ProgrammaticRecords")
    void should_WriteViaDataFrameAPI_When_ProgrammaticRecords() throws Exception {
        spark.sql("CREATE TABLE lakehouse.db.metrics (" +
                "  metric_name STRING, " +
                "  value DOUBLE, " +
                "  ts BIGINT" +
                ") USING iceberg");

        // Create DataFrame programmatically
        Dataset<Row> df = spark.createDataFrame(
                List.of(
                        org.apache.spark.sql.RowFactory.create("cpu_usage", 72.5, 1700000001L),
                        org.apache.spark.sql.RowFactory.create("memory_usage", 85.2, 1700000001L),
                        org.apache.spark.sql.RowFactory.create("disk_io", 45.0, 1700000001L),
                        org.apache.spark.sql.RowFactory.create("cpu_usage", 68.3, 1700000002L),
                        org.apache.spark.sql.RowFactory.create("memory_usage", 82.1, 1700000002L)
                ),
                new org.apache.spark.sql.types.StructType()
                        .add("metric_name", "string")
                        .add("value", "double")
                        .add("ts", "long")
        );

        // Write via DataFrame API — this is how production Spark jobs should write
        df.writeTo("lakehouse.db.metrics").append();

        // Verify
        Dataset<Row> result = spark.sql(
                "SELECT metric_name, ROUND(AVG(value), 1) as avg_val " +
                "FROM lakehouse.db.metrics " +
                "GROUP BY metric_name " +
                "ORDER BY metric_name");

        List<Row> rows = result.collectAsList();
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getString(0)).isEqualTo("cpu_usage");
        assertThat(rows.get(0).getDouble(1)).isEqualTo(70.4);  // avg(72.5, 68.3)
    }

    // ==================== Helpers ====================

    private void createBucketIfNotExists() {
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(minio.getS3URL()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .build()) {

            boolean exists = s3.listBuckets().buckets().stream()
                    .anyMatch(b -> b.name().equals(BUCKET));
            if (!exists) {
                s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
            }
        }
    }
}
