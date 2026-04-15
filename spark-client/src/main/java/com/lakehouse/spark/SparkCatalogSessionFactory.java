package com.lakehouse.spark;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.spark.sql.SparkSession;

/**
 * Factory for creating SparkSession pre-configured with Gravitino Iceberg REST Catalog.
 *
 * <p>Configures Spark to use Iceberg REST Catalog backed by MinIO (S3-compatible storage).
 * After obtaining a SparkSession, users can directly execute Spark SQL queries on Iceberg tables.</p>
 *
 * <p>Usage:</p>
 * <pre>{@code
 * SparkSession spark = SparkCatalogSessionFactory.builder()
 *     .catalogUri("http://gravitino:9001/iceberg/v1")
 *     .minioEndpoint("http://minio:9000")
 *     .minioAccessKey("minioadmin")
 *     .minioSecretKey("minioadmin")
 *     .build()
 *     .createSession();
 *
 * spark.sql("SELECT * FROM lakehouse.default.users").show();
 * }</pre>
 *
 * @author platform
 * @since 1.0.0
 */
@Getter
@Builder
@ToString(exclude = {"minioSecretKey"})
public class SparkCatalogSessionFactory {

    @Builder.Default
    private final String appName = "Lakehouse Spark Client";

    @Builder.Default
    private final String catalogName = "lakehouse";

    @Builder.Default
    private final String catalogUri = "http://localhost:9001/iceberg/v1";

    @Builder.Default
    private final String warehousePath = "s3a://lakehouse/";

    @Builder.Default
    private final String minioEndpoint = "http://localhost:9000";

    @Builder.Default
    private final String minioAccessKey = "minioadmin";

    @Builder.Default
    private final String minioSecretKey = "minioadmin";

    @Builder.Default
    private final String minioRegion = "us-east-1";

    @Builder.Default
    private final String sparkMaster = "local[*]";

    /**
     * Create a SparkSession with Iceberg REST Catalog fully configured.
     *
     * @return configured SparkSession
     */
    public SparkSession createSession() {
        return SparkSession.builder()
                .appName(appName)
                .master(sparkMaster)

                // Register Iceberg REST Catalog
                .config("spark.sql.catalog." + catalogName, "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog." + catalogName + ".type", "rest")
                .config("spark.sql.catalog." + catalogName + ".uri", catalogUri)
                .config("spark.sql.catalog." + catalogName + ".warehouse", warehousePath)
                .config("spark.sql.catalog." + catalogName + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

                // S3 / MinIO configuration for Iceberg FileIO
                .config("spark.sql.catalog." + catalogName + ".s3.endpoint", minioEndpoint)
                .config("spark.sql.catalog." + catalogName + ".s3.access-key-id", minioAccessKey)
                .config("spark.sql.catalog." + catalogName + ".s3.secret-access-key", minioSecretKey)
                .config("spark.sql.catalog." + catalogName + ".s3.region", minioRegion)
                .config("spark.sql.catalog." + catalogName + ".s3.path-style-access", "true")

                // Hadoop S3A configuration (for Spark reading Parquet data files)
                .config("spark.hadoop.fs.s3a.endpoint", minioEndpoint)
                .config("spark.hadoop.fs.s3a.access.key", minioAccessKey)
                .config("spark.hadoop.fs.s3a.secret.key", minioSecretKey)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.region", minioRegion)

                // Iceberg Spark extensions (merge-on-read, time travel, etc.)
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

                // Default catalog
                .config("spark.sql.defaultCatalog", catalogName)

                .getOrCreate();
    }
}
