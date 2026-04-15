package com.lakehouse.catalog.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * TDD tests for {@link CatalogConfig}.
 *
 * @author platform
 * @since 1.0.0
 */
class CatalogConfigTest {

    @Test
    @DisplayName("should_UseDefaults_When_NoCustomization")
    void should_UseDefaults_When_NoCustomization() {
        CatalogConfig config = CatalogConfig.builder().build();

        assertThat(config.getCatalogUri()).isEqualTo("http://localhost:9001/iceberg/v1");
        assertThat(config.getCatalogName()).isEqualTo("lakehouse");
        assertThat(config.getWarehousePath()).isEqualTo("s3a://lakehouse/");
        assertThat(config.getMinioEndpoint()).isEqualTo("http://localhost:9000");
        assertThat(config.getMinioAccessKey()).isEqualTo("minioadmin");
        assertThat(config.getMinioSecretKey()).isEqualTo("minioadmin");
        assertThat(config.getMinioRegion()).isEqualTo("us-east-1");
        assertThat(config.getDefaultNamespace()).isEqualTo("default");
    }

    @Test
    @DisplayName("should_OverrideDefaults_When_CustomValuesProvided")
    void should_OverrideDefaults_When_CustomValuesProvided() {
        CatalogConfig config = CatalogConfig.builder()
                .catalogUri("http://gravitino:9001/iceberg/v1")
                .catalogName("production")
                .warehousePath("s3a://prod-bucket/warehouse/")
                .minioEndpoint("http://minio-cluster:9000")
                .minioAccessKey("AKID123")
                .minioSecretKey("SECRET456")
                .minioRegion("ap-east-1")
                .defaultNamespace("tenant-acme")
                .build();

        assertThat(config.getCatalogUri()).isEqualTo("http://gravitino:9001/iceberg/v1");
        assertThat(config.getCatalogName()).isEqualTo("production");
        assertThat(config.getWarehousePath()).isEqualTo("s3a://prod-bucket/warehouse/");
        assertThat(config.getMinioEndpoint()).isEqualTo("http://minio-cluster:9000");
        assertThat(config.getMinioAccessKey()).isEqualTo("AKID123");
        assertThat(config.getMinioSecretKey()).isEqualTo("SECRET456");
        assertThat(config.getMinioRegion()).isEqualTo("ap-east-1");
        assertThat(config.getDefaultNamespace()).isEqualTo("tenant-acme");
    }

    @Test
    @DisplayName("should_MaskSecretKey_When_ToStringCalled")
    void should_MaskSecretKey_When_ToStringCalled() {
        CatalogConfig config = CatalogConfig.builder()
                .minioSecretKey("super-secret-value")
                .build();

        String str = config.toString();
        assertThat(str).doesNotContain("super-secret-value");
        assertThat(str).contains("catalogUri");
    }
}
