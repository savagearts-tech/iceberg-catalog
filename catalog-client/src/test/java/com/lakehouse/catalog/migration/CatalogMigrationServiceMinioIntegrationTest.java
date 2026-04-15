package com.lakehouse.catalog.migration;

import com.lakehouse.catalog.client.IcebergCatalogClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class CatalogMigrationServiceMinioIntegrationTest {

    private static final String MINIO_ENDPOINT = "http://127.0.0.1:9000";
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final String BUCKET = "warehouse";
    private static final String REGION = "us-east-1";
    private static final String SOURCE_CATALOG_NAME = "migration-source-it";
    private static final String TARGET_CATALOG_NAME = "migration-target-it";
    private static final String TABLE_NAME = "users";
    private static final Path FAKE_HADOOP_HOME = prepareFakeHadoopHome();
    private static final Schema USER_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get())
    );

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("should_MigrateTableFromHadoopCatalogToJdbcCatalog_When_UsingLocalMinio")
    void should_MigrateTableFromHadoopCatalogToJdbcCatalog_When_UsingLocalMinio() throws IOException {
        assumeTrue(isMinioReachable(), "Requires local MinIO at " + MINIO_ENDPOINT);
        createBucketIfNotExists();

        String namespaceName = "migration_it_" + System.currentTimeMillis();
        Namespace namespace = Namespace.of(namespaceName);
        TableIdentifier tableIdentifier = TableIdentifier.of(namespace, TABLE_NAME);
        assertThat(FAKE_HADOOP_HOME).exists();

        HadoopCatalog sourceCatalog = buildSourceCatalog();
        JdbcCatalog targetCatalog = buildTargetCatalog();
        try (IcebergCatalogClient sourceClient = new IcebergCatalogClient(sourceCatalog, namespaceName)) {
            sourceClient.ensureNamespace();
            sourceClient.createTable(TABLE_NAME, USER_SCHEMA);
            sourceClient.writeRecords(TABLE_NAME, sampleRecords());

            try {
                BaseTable sourceTable = (BaseTable) sourceCatalog.loadTable(tableIdentifier);
                String sourceMetadataLocation = sourceTable.operations().current().metadataFileLocation();
                long sourceSnapshotId = sourceTable.currentSnapshot().snapshotId();
                @SuppressWarnings("resource")
                IcebergCatalogClient targetClient = new IcebergCatalogClient(targetCatalog, namespaceName);
                CatalogMigrationService migrationService = new CatalogMigrationService(sourceCatalog, targetCatalog);
                MigrationResult result = migrationService.migrateNamespace(namespaceName);

                assertThat(result.getNamespacesScanned()).isEqualTo(1);
                assertThat(result.getTablesScanned()).isEqualTo(1);
                assertThat(result.getTablesMigrated()).isEqualTo(1);
                assertThat(result.getTablesSkipped()).isZero();
                assertThat(result.getTablesFailed()).isZero();

                assertThat(targetCatalog.tableExists(tableIdentifier)).isTrue();

                BaseTable targetTable = (BaseTable) targetCatalog.loadTable(tableIdentifier);
                assertThat(targetTable.currentSnapshot()).isNotNull();
                assertThat(targetTable.currentSnapshot().snapshotId()).isEqualTo(sourceSnapshotId);
                assertThat(targetTable.operations().current().metadataFileLocation()).isEqualTo(sourceMetadataLocation);

                List<Record> migratedRecords = targetClient.readRecords(TABLE_NAME);
                assertThat(migratedRecords)
                        .extracting(
                                record -> record.getField("id"),
                                record -> record.getField("name"),
                                record -> record.getField("email")
                        )
                        .containsExactlyInAnyOrder(
                                tuple(1L, "alice", "alice@test.com"),
                                tuple(2L, "bob", "bob@test.com")
                        );

                MigrationResult secondRun = migrationService.migrateNamespace(namespaceName);
                assertThat(secondRun.getTablesMigrated()).isZero();
                assertThat(secondRun.getTablesSkipped()).isEqualTo(1);
            } finally {
                cleanupTable(sourceCatalog, targetCatalog, tableIdentifier);
                closeQuietly(sourceCatalog);
                closeQuietly(targetCatalog);
            }
        }
    }

    private List<GenericRecord> sampleRecords() {
        GenericRecord alice = GenericRecord.create(USER_SCHEMA);
        alice.setField("id", 1L);
        alice.setField("name", "alice");
        alice.setField("email", "alice@test.com");

        GenericRecord bob = GenericRecord.create(USER_SCHEMA);
        bob.setField("id", 2L);
        bob.setField("name", "bob");
        bob.setField("email", "bob@test.com");

        return List.of(alice, bob);
    }

    private boolean isMinioReachable() {
        try (S3Client s3 = s3Client()) {
            s3.listBuckets();
            return true;
        } catch (Exception exception) {
            return false;
        }
    }

    private void createBucketIfNotExists() {
        try (S3Client s3 = s3Client()) {
            boolean exists = s3.listBuckets().buckets().stream()
                    .anyMatch(bucket -> bucket.name().equals(BUCKET));
            if (!exists) {
                s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
            }
        }
    }

    private S3Client s3Client() {
        return S3Client.builder()
                .endpointOverride(URI.create(MINIO_ENDPOINT))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(Region.of(REGION))
                .forcePathStyle(true)
                .build();
    }

    private HadoopCatalog buildSourceCatalog() {
        HadoopCatalog catalog = new HadoopCatalog();
        catalog.setConf(buildHadoopConf());
        catalog.initialize(SOURCE_CATALOG_NAME, Map.of(
                CatalogProperties.WAREHOUSE_LOCATION, "s3a://" + BUCKET + "/"
        ));
        return catalog;
    }

    private JdbcCatalog buildTargetCatalog() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3a://" + BUCKET + "/");
        properties.put(CatalogProperties.URI, "jdbc:h2:file:" + normalizePath(tempDir.resolve("migration-catalog-db")));
        properties.put("jdbc.user", "sa");
        properties.put("jdbc.password", "");
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3.endpoint", MINIO_ENDPOINT);
        properties.put("s3.access-key-id", ACCESS_KEY);
        properties.put("s3.secret-access-key", SECRET_KEY);
        properties.put("s3.region", REGION);
        properties.put("s3.path-style-access", "true");
        properties.put("client.region", REGION);

        JdbcCatalog catalog = new JdbcCatalog();
        catalog.setConf(buildHadoopConf());
        catalog.initialize(TARGET_CATALOG_NAME, properties);
        return catalog;
    }

    private Configuration buildHadoopConf() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.endpoint", MINIO_ENDPOINT);
        hadoopConf.set("fs.s3a.access.key", ACCESS_KEY);
        hadoopConf.set("fs.s3a.secret.key", SECRET_KEY);
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.region", REGION);
        hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");
        hadoopConf.set("fs.s3a.fast.upload", "true");
        hadoopConf.set("fs.s3a.fast.upload.buffer", "array");
        return hadoopConf;
    }

    private void cleanupTable(Catalog sourceCatalog, Catalog targetCatalog, TableIdentifier tableIdentifier) {
        if (targetCatalog.tableExists(tableIdentifier)) {
            targetCatalog.dropTable(tableIdentifier, false);
        }
        if (sourceCatalog.tableExists(tableIdentifier)) {
            sourceCatalog.dropTable(tableIdentifier, true);
        }
    }

    private void closeQuietly(Catalog catalog) {
        if (catalog instanceof Closeable closeable) {
            try {
                closeable.close();
            } catch (IOException ignored) {
                // Best-effort cleanup for test resources.
            }
        }
    }

    private String normalizePath(Path path) {
        return path.toAbsolutePath().toString().replace("\\", "/");
    }

    private static Path prepareFakeHadoopHome() {
        try {
            Path hadoopHome = Files.createTempDirectory("fake-hadoop");
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
        } catch (IOException | InterruptedException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }
}
