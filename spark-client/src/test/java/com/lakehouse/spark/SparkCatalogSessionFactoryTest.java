package com.lakehouse.spark;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

/**
 * TDD tests for {@link SparkCatalogSessionFactory}.
 *
 * <p>Validates that the factory correctly configures all Spark SQL Catalog properties
 * for Gravitino Iceberg REST + MinIO. Uses local Spark (no external dependencies).</p>
 *
 * @author platform
 * @since 1.0.0
 */
class SparkCatalogSessionFactoryTest {

    @Test
    @DisplayName("should_ConfigureIcebergRESTCatalog_When_DefaultSettings")
    void should_ConfigureIcebergRESTCatalog_When_DefaultSettings() {
        SparkCatalogSessionFactory factory = SparkCatalogSessionFactory.builder().build();
        SparkSession spark = factory.createSession();

        try {
            // Verify Iceberg REST Catalog is configured
            assertThat(spark.conf().get("spark.sql.catalog.lakehouse"))
                    .isEqualTo("org.apache.iceberg.spark.SparkCatalog");
            assertThat(spark.conf().get("spark.sql.catalog.lakehouse.type"))
                    .isEqualTo("rest");
            assertThat(spark.conf().get("spark.sql.catalog.lakehouse.uri"))
                    .isEqualTo("http://localhost:9001/iceberg/v1");
            assertThat(spark.conf().get("spark.sql.catalog.lakehouse.warehouse"))
                    .isEqualTo("s3a://lakehouse/");

            // Verify S3 FileIO
            assertThat(spark.conf().get("spark.sql.catalog.lakehouse.io-impl"))
                    .isEqualTo("org.apache.iceberg.aws.s3.S3FileIO");

            // Verify MinIO S3 config
            assertThat(spark.conf().get("spark.sql.catalog.lakehouse.s3.endpoint"))
                    .isEqualTo("http://localhost:9000");
            assertThat(spark.conf().get("spark.sql.catalog.lakehouse.s3.path-style-access"))
                    .isEqualTo("true");

            // Verify Hadoop S3A config
            assertThat(spark.conf().get("spark.hadoop.fs.s3a.endpoint"))
                    .isEqualTo("http://localhost:9000");
            assertThat(spark.conf().get("spark.hadoop.fs.s3a.path.style.access"))
                    .isEqualTo("true");

            // Verify Iceberg extensions
            assertThat(spark.conf().get("spark.sql.extensions"))
                    .contains("IcebergSparkSessionExtensions");

            // Verify default catalog
            assertThat(spark.conf().get("spark.sql.defaultCatalog"))
                    .isEqualTo("lakehouse");
        } finally {
            spark.stop();
        }
    }

    @Test
    @DisplayName("should_UseCustomCatalogName_When_Configured")
    void should_UseCustomCatalogName_When_Configured() {
        SparkCatalogSessionFactory factory = SparkCatalogSessionFactory.builder()
                .catalogName("production")
                .catalogUri("http://gravitino:9001/iceberg/v1")
                .build();
        SparkSession spark = factory.createSession();

        try {
            assertThat(spark.conf().get("spark.sql.catalog.production"))
                    .isEqualTo("org.apache.iceberg.spark.SparkCatalog");
            assertThat(spark.conf().get("spark.sql.catalog.production.uri"))
                    .isEqualTo("http://gravitino:9001/iceberg/v1");
            assertThat(spark.conf().get("spark.sql.defaultCatalog"))
                    .isEqualTo("production");
        } finally {
            spark.stop();
        }
    }

    @Test
    @DisplayName("should_ConfigureMinIOEndpoint_When_CustomEndpoint")
    void should_ConfigureMinIOEndpoint_When_CustomEndpoint() {
        SparkCatalogSessionFactory factory = SparkCatalogSessionFactory.builder()
                .minioEndpoint("http://minio-cluster:9000")
                .minioAccessKey("AKID_CUSTOM")
                .minioSecretKey("SK_CUSTOM")
                .minioRegion("ap-east-1")
                .build();
        SparkSession spark = factory.createSession();

        try {
            assertThat(spark.conf().get("spark.sql.catalog.lakehouse.s3.endpoint"))
                    .isEqualTo("http://minio-cluster:9000");
            assertThat(spark.conf().get("spark.sql.catalog.lakehouse.s3.access-key-id"))
                    .isEqualTo("AKID_CUSTOM");
            assertThat(spark.conf().get("spark.sql.catalog.lakehouse.s3.region"))
                    .isEqualTo("ap-east-1");

            assertThat(spark.conf().get("spark.hadoop.fs.s3a.endpoint"))
                    .isEqualTo("http://minio-cluster:9000");
            assertThat(spark.conf().get("spark.hadoop.fs.s3a.access.key"))
                    .isEqualTo("AKID_CUSTOM");
            assertThat(spark.conf().get("spark.hadoop.fs.s3a.region"))
                    .isEqualTo("ap-east-1");
        } finally {
            spark.stop();
        }
    }

    @Test
    @DisplayName("should_MaskSecretKey_When_ToStringCalled")
    void should_MaskSecretKey_When_ToStringCalled() {
        SparkCatalogSessionFactory factory = SparkCatalogSessionFactory.builder()
                .minioSecretKey("do-not-leak-this")
                .build();

        assertThat(factory.toString()).doesNotContain("do-not-leak-this");
    }
}
