package com.lakehouse.catalog;

import com.lakehouse.catalog.client.IcebergCatalogClient;
import com.lakehouse.catalog.config.CatalogConfig;
import com.lakehouse.catalog.config.CatalogConfig.CatalogType;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Example: write Iceberg Parquet through REST Catalog and S3-compatible storage.
 *
 * <p><strong>Production-style configuration</strong>: all connectivity is driven by environment
 * variables (with local-dev defaults so {@code mvn exec:java} still works out of the box).
 * Deployments should set secrets and endpoints explicitly; defaults are not suitable for
 * real production secrets.</p>
 *
 * <h2>Environment variables</h2>
 * <ul>
 *   <li>{@code ICEBERG_CATALOG_TYPE} — {@code REST}, {@code JDBC}, or {@code HADOOP} (default {@code REST})</li>
 *   <li>{@code ICEBERG_REST_URI} — REST catalog URL (default local Gravitino-style URL)</li>
 *   <li>{@code ICEBERG_CATALOG_NAME}, {@code ICEBERG_WAREHOUSE}, {@code ICEBERG_DEFAULT_NAMESPACE}</li>
 *   <li>{@code S3_ENDPOINT}, {@code S3_ACCESS_KEY}, {@code S3_SECRET_KEY}, {@code S3_REGION}</li>
 *   <li>{@code ICEBERG_CATALOG_CLIENT_POOL_SIZE} — {@link CatalogProperties#CLIENT_POOL_SIZE} (default {@code 8})</li>
 *   <li>{@code ICEBERG_EXAMPLE_TABLE} — sample table name (default {@code users})</li>
 *   <li>JDBC mode additionally: {@code ICEBERG_JDBC_URL}, {@code ICEBERG_JDBC_USER}, {@code ICEBERG_JDBC_PASSWORD},
 *       pool tuning {@code ICEBERG_JDBC_POOL_MAX_SIZE}, etc.</li>
 * </ul>
 *
 * <p>Data path: Java app → Iceberg REST Catalog (metadata) → S3-compatible object store (Parquet).</p>
 *
 * @author platform
 * @since 1.0.0
 */
@Slf4j
public class WriteParquetExample {

    /**
     * Environment variable names (single place to document and reuse).
     */
    private static final class IcebergEnv {
        static final String CATALOG_TYPE = "ICEBERG_CATALOG_TYPE";
        static final String REST_URI = "ICEBERG_REST_URI";
        static final String CATALOG_NAME = "ICEBERG_CATALOG_NAME";
        static final String WAREHOUSE = "ICEBERG_WAREHOUSE";
        static final String S3_ENDPOINT = "S3_ENDPOINT";
        static final String S3_ACCESS_KEY = "S3_ACCESS_KEY";
        static final String S3_SECRET_KEY = "S3_SECRET_KEY";
        static final String S3_REGION = "S3_REGION";
        static final String DEFAULT_NAMESPACE = "ICEBERG_DEFAULT_NAMESPACE";
        static final String CATALOG_CLIENT_POOL_SIZE = "ICEBERG_CATALOG_CLIENT_POOL_SIZE";
        static final String EXAMPLE_TABLE = "ICEBERG_EXAMPLE_TABLE";

        private IcebergEnv() {
        }
    }

    public static void main(String[] args) throws Exception {
        CatalogConfig config = buildCatalogConfigFromEnvironment();
        logStartupSummary(config);

        String tableName = env(IcebergEnv.EXAMPLE_TABLE, "users");

        try (IcebergCatalogClient client = new IcebergCatalogClient(config)) {
            client.ensureNamespace();

            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "name", Types.StringType.get()),
                    Types.NestedField.optional(3, "email", Types.StringType.get()),
                    Types.NestedField.optional(4, "created_at", Types.TimestampType.withZone())
            );

            PartitionSpec spec = PartitionSpec.builderFor(schema).day("created_at").build();
            Map<String, String> tableProperties = Map.of(
                    "write.format.default", "parquet",
                    "write.parquet.compression-codec", "zstd",
                    "comment", "WriteParquetExample — env-driven catalog + day(created_at)"
            );

            client.createTable(tableName, schema, spec, tableProperties);

            List<GenericRecord> records = new ArrayList<>();
            for (int i = 1; i <= 100; i++) {
                GenericRecord record = GenericRecord.create(schema);
                record.setField("id", (long) i);
                record.setField("name", "user_" + i);
                record.setField("email", "user_" + i + "@example.com");
                record.setField("created_at", java.time.OffsetDateTime.now());
                records.add(record);
            }

            client.writeRecords(tableName, records);

            List<Record> readBack = client.readRecords(tableName);
            log.info("Verification: wrote {} records, read back {} records", records.size(), readBack.size());

            log.info("Tables in namespace '{}': {}", config.getDefaultNamespace(), client.listTables());
        }
    }

    /**
     * Builds {@link CatalogConfig} from process environment. Missing keys use local-dev defaults;
     * {@link #logStartupSummary(CatalogConfig)} warns when insecure defaults are in effect.
     */
    static CatalogConfig buildCatalogConfigFromEnvironment() {
        CatalogType type = parseCatalogType(env(IcebergEnv.CATALOG_TYPE, "REST"));
        String clientPool = env(IcebergEnv.CATALOG_CLIENT_POOL_SIZE, "8");
        Map<String, String> catalogProperties = Map.of(CatalogProperties.CLIENT_POOL_SIZE, clientPool);

        CatalogConfig.CatalogConfigBuilder builder = CatalogConfig.builder()
                .catalogType(type)
                .catalogName(env(IcebergEnv.CATALOG_NAME, "lakehouse"))
                .warehousePath(env(IcebergEnv.WAREHOUSE, "s3a://lakehouse/"))
                .minioEndpoint(env(IcebergEnv.S3_ENDPOINT, "http://localhost:9000"))
                .minioAccessKey(env(IcebergEnv.S3_ACCESS_KEY, "minioadmin"))
                .minioSecretKey(env(IcebergEnv.S3_SECRET_KEY, "minioadmin"))
                .minioRegion(env(IcebergEnv.S3_REGION, "us-east-1"))
                .defaultNamespace(env(IcebergEnv.DEFAULT_NAMESPACE, "default"))
                .catalogProperties(catalogProperties);

        if (type == CatalogType.REST) {
            builder.catalogUri(env(IcebergEnv.REST_URI, "http://localhost:9001/iceberg/v1"));
        } else if (type == CatalogType.JDBC) {
            builder.jdbcUrl(env("ICEBERG_JDBC_URL", "jdbc:h2:mem:iceberg_catalog;DB_CLOSE_DELAY=-1"));
            builder.jdbcUsername(env("ICEBERG_JDBC_USER", "sa"));
            builder.jdbcPassword(env("ICEBERG_JDBC_PASSWORD", ""));
            builder.jdbcPoolProvider(env("ICEBERG_JDBC_POOL_PROVIDER", "hikari"));
            builder.jdbcPoolMaxSize(parseIntEnv("ICEBERG_JDBC_POOL_MAX_SIZE", 10));
            builder.jdbcPoolMinIdle(parseIntEnv("ICEBERG_JDBC_POOL_MIN_IDLE", 1));
            builder.jdbcPoolConnectionTimeoutMs(parseLongEnv("ICEBERG_JDBC_POOL_CONNECTION_TIMEOUT_MS", 30_000L));
            builder.jdbcPoolIdleTimeoutMs(parseLongEnv("ICEBERG_JDBC_POOL_IDLE_TIMEOUT_MS", 600_000L));
            builder.jdbcPoolMaxLifetimeMs(parseLongEnv("ICEBERG_JDBC_POOL_MAX_LIFETIME_MS", 1_800_000L));
            builder.jdbcPoolValidationTimeoutMs(parseLongEnv("ICEBERG_JDBC_POOL_VALIDATION_TIMEOUT_MS", 5_000L));
            builder.jdbcPoolLeakDetectionThresholdMs(parseLongEnv("ICEBERG_JDBC_POOL_LEAK_DETECTION_THRESHOLD_MS", 0L));
        }

        return builder.build();
    }

    private static void logStartupSummary(CatalogConfig config) {
        log.info(
                "Iceberg example: catalogType={}, catalogName={}, warehouse={}, namespace={}",
                config.getCatalogType(),
                config.getCatalogName(),
                config.getWarehousePath(),
                config.getDefaultNamespace());
        if (config.getCatalogType() == CatalogType.REST) {
            log.info("REST catalog URI: {}", config.getCatalogUri());
        } else if (config.getCatalogType() == CatalogType.JDBC) {
            log.info("JDBC URL: {}", config.getJdbcUrl());
        }
        if ("minioadmin".equals(config.getMinioAccessKey()) || "minioadmin".equals(config.getMinioSecretKey())) {
            log.warn(
                    "S3 credentials match well-known MinIO defaults. "
                            + "Set {} and {} for production.",
                    IcebergEnv.S3_ACCESS_KEY,
                    IcebergEnv.S3_SECRET_KEY);
        }
    }

    private static CatalogType parseCatalogType(String raw) {
        if (raw == null || raw.isBlank()) {
            return CatalogType.REST;
        }
        try {
            return CatalogType.valueOf(raw.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid " + IcebergEnv.CATALOG_TYPE + ": '" + raw + "'. Use REST, JDBC, or HADOOP.", e);
        }
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim();
    }

    private static int parseIntEnv(String key, int defaultValue) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(raw.trim());
    }

    private static long parseLongEnv(String key, long defaultValue) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return Long.parseLong(raw.trim());
    }
}
