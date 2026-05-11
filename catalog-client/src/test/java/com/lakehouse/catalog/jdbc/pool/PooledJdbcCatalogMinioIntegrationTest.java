package com.lakehouse.catalog.jdbc.pool;

import com.lakehouse.catalog.client.IcebergCatalogClient;
import com.lakehouse.catalog.config.CatalogConfig;
import com.lakehouse.catalog.factory.CatalogFactory;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end validation of {@link PooledJdbcCatalog} (via {@link CatalogFactory}) against a
 * real S3-compatible store. Metadata lives in embedded H2; warehouse is MinIO (S3A).
 *
 * <p>Requires MinIO listening at {@link #MINIO_ENDPOINT} with credentials {@link #ACCESS_KEY} /
 * {@link #SECRET_KEY} and bucket {@link #BUCKET}. If unreachable, the test is skipped.</p>
 */
class PooledJdbcCatalogMinioIntegrationTest {

    private static final String MINIO_ENDPOINT = "http://127.0.0.1:9000";
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final String BUCKET = "warehouse";
    private static final String REGION = "us-east-1";
    private static final String CATALOG_NAME = "pooled-jdbc-e2e";
    private static final Path FAKE_HADOOP_HOME = prepareFakeHadoopHome();

    private static final Schema PROBE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "label", Types.StringType.get())
    );

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("should_UsePooledJdbcCatalogWithS3Warehouse_When_MinioAvailable")
    void should_UsePooledJdbcCatalogWithS3Warehouse_When_MinioAvailable() throws IOException {
        assumeTrue(isMinioReachable(), "Requires local MinIO at " + MINIO_ENDPOINT);
        assertThat(FAKE_HADOOP_HOME).exists();
        createBucketIfNotExists();

        String namespace = "pooled_it_" + System.currentTimeMillis();
        String warehousePrefix = "s3a://" + BUCKET + "/pooled-jdbc-e2e-" + System.currentTimeMillis() + "/";

        CatalogConfig config = CatalogConfig.builder()
                .catalogType(CatalogConfig.CatalogType.JDBC)
                .catalogName(CATALOG_NAME)
                .defaultNamespace(namespace)
                .warehousePath(warehousePrefix)
                .jdbcUrl("jdbc:h2:file:" + normalizePath(tempDir.resolve("pooled-jdbc-catalog-db")))
                .jdbcUsername("sa")
                .jdbcPassword("")
                .jdbcPoolProvider("hikari")
                .jdbcPoolMaxSize(4)
                .jdbcPoolMinIdle(1)
                .minioEndpoint(MINIO_ENDPOINT)
                .minioAccessKey(ACCESS_KEY)
                .minioSecretKey(SECRET_KEY)
                .minioRegion(REGION)
                .build();

        Catalog catalog = CatalogFactory.build(config);
        assertThat(catalog)
                .isInstanceOf(PooledJdbcCatalog.class)
                .isInstanceOf(JdbcCatalog.class);

        try (IcebergCatalogClient client = new IcebergCatalogClient(catalog, namespace)) {
            client.ensureNamespace();
            client.createTable("probe", PROBE_SCHEMA);

            GenericRecord row = GenericRecord.create(PROBE_SCHEMA);
            row.setField("id", 42L);
            row.setField("label", "pooled-jdbc");
            client.writeRecords("probe", List.of(row));

            assertThat(client.loadTable("probe").currentSnapshot()).isNotNull();

            List<Record> read = client.readRecords("probe");
            assertThat(read).hasSize(1);
            assertThat(read.get(0).getField("id")).isEqualTo(42L);
            assertThat(read.get(0).getField("label")).isEqualTo("pooled-jdbc");

            client.dropTable("probe", true);
        }
    }

    private boolean isMinioReachable() {
        try (S3Client s3 = s3Client()) {
            s3.listBuckets();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void createBucketIfNotExists() {
        try (S3Client s3 = s3Client()) {
            boolean exists = s3.listBuckets().buckets().stream()
                    .anyMatch(b -> b.name().equals(BUCKET));
            if (!exists) {
                s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
            }
        }
    }

    private static S3Client s3Client() {
        return S3Client.builder()
                .endpointOverride(URI.create(MINIO_ENDPOINT))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(Region.of(REGION))
                .forcePathStyle(true)
                .build();
    }

    private static String normalizePath(Path path) {
        return path.toAbsolutePath().toString().replace("\\", "/");
    }

    private static Path prepareFakeHadoopHome() {
        try {
            Path hadoopHome = Files.createTempDirectory("fake-hadoop-pooled");
            Path binDir = Files.createDirectories(hadoopHome.resolve("bin"));
            Path winutilsExe = binDir.resolve("winutils.exe");
            String powershellScript = """
                    $source = @'
                    using System;
                    public static class Program
                    {
                        public static int Main(string[] args)
                        {
                            return 0;
                        }
                    }
                    '@;
                    Add-Type -TypeDefinition $source -OutputAssembly '%s' -OutputType ConsoleApplication -Language CSharp
                    """.formatted(winutilsExe);

            Process process = new ProcessBuilder(
                    "powershell.exe",
                    "-NoProfile",
                    "-Command",
                    powershellScript
            ).start();

            String errorOutput = new String(process.getErrorStream().readAllBytes());
            int exitCode = process.waitFor();
            if (exitCode != 0 || !Files.exists(winutilsExe)) {
                throw new IllegalStateException("failed to create fake winutils.exe: " + errorOutput);
            }

            System.setProperty("hadoop.home.dir", hadoopHome.toString());
            return hadoopHome;
        } catch (IOException | InterruptedException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
