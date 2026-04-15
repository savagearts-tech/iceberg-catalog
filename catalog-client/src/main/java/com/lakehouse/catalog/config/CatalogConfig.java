package com.lakehouse.catalog.config;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

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
@ToString(exclude = {"minioSecretKey"})
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
