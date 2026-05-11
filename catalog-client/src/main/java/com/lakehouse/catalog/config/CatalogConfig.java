package com.lakehouse.catalog.config;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.Map;

/**
 * Configuration for connecting to the Gravitino Iceberg REST Catalog and MinIO.
 *
 * <p>All connection parameters needed for Iceberg table operations via REST Catalog
 * backed by MinIO (S3-compatible) storage.</p>
 *
 * @author platform
 * @since 1.0.0
 */
@Getter
@Builder
@ToString(exclude = {"jdbcPassword", "minioSecretKey"})
public class CatalogConfig {

public enum CatalogType {
        HADOOP, JDBC, REST
    }

    /**
     * Type of Iceberg Catalog to use.
     */
    @Builder.Default
    private final CatalogType catalogType = CatalogType.REST;

    /**
     * JDBC URL for JDBC Catalog.
     */
    @Builder.Default
    private final String jdbcUrl = "jdbc:h2:mem:iceberg_catalog;DB_CLOSE_DELAY=-1";

    /**
     * Username for JDBC Catalog.
     */
    @Builder.Default
    private final String jdbcUsername = "sa";

    /**
     * Password for JDBC Catalog.
     */
    @Builder.Default
    private final String jdbcPassword = "";

    /**
     * Connection pool provider for JDBC Catalog.
     */
    @Builder.Default
    private final String jdbcPoolProvider = "hikari";

    /**
     * Maximum number of pooled JDBC connections.
     */
    @Builder.Default
    private final int jdbcPoolMaxSize = 10;

    /**
     * Minimum number of idle pooled JDBC connections.
     */
    @Builder.Default
    private final int jdbcPoolMinIdle = 1;

    /**
     * How long to wait for a pooled connection before timing out.
     */
    @Builder.Default
    private final long jdbcPoolConnectionTimeoutMs = 30_000L;

    /**
     * Maximum idle time before a pooled connection can be retired.
     */
    @Builder.Default
    private final long jdbcPoolIdleTimeoutMs = 600_000L;

    /**
     * Maximum lifetime of a pooled connection.
     */
    @Builder.Default
    private final long jdbcPoolMaxLifetimeMs = 1_800_000L;

    /**
     * Validation timeout used by the underlying connection pool.
     */
    @Builder.Default
    private final long jdbcPoolValidationTimeoutMs = 5_000L;

    /**
     * Leak detection threshold for the underlying connection pool. 0 disables it.
     */
    @Builder.Default
    private final long jdbcPoolLeakDetectionThresholdMs = 0L;

    /**
     * JDBC driver-specific properties, stored without the {@code jdbc.} prefix.
     */
    @Builder.Default
    private final Map<String, String> jdbcProperties = Map.of();

    /**
     * Extra Iceberg catalog properties passed through during initialization.
     */
    @Builder.Default
    private final Map<String, String> catalogProperties = Map.of();

    /**
     * Gravitino Iceberg REST Catalog URI.
     * Example: http://gravitino:9001/iceberg/v1
     */
    @Builder.Default
    private final String catalogUri = "http://localhost:9001/iceberg/v1";

    /**
     * Catalog name registered in Gravitino.
     */
    @Builder.Default
    private final String catalogName = "lakehouse";

    /**
     * Iceberg warehouse path (S3 prefix).
     * Example: s3a://lakehouse/
     */
    @Builder.Default
    private final String warehousePath = "s3a://lakehouse/";

    /**
     * MinIO / S3 endpoint URL.
     */
    @Builder.Default
    private final String minioEndpoint = "http://localhost:9000";

    /**
     * MinIO access key.
     */
    @Builder.Default
    private final String minioAccessKey = "minioadmin";

    /**
     * MinIO secret key.
     */
    @Builder.Default
    private final String minioSecretKey = "minioadmin";

    /**
     * MinIO region (required by AWS SDK, use any value for MinIO).
     */
    @Builder.Default
    private final String minioRegion = "us-east-1";

    /**
     * Default namespace for table operations.
     * Corresponds to the tenant namespace in multi-tenant mode.
     */
    @Builder.Default
    private final String defaultNamespace = "default";
}
