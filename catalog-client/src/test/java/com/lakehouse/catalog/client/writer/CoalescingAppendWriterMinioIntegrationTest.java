package com.lakehouse.catalog.client.writer;

import com.lakehouse.catalog.client.IcebergCatalogClient;
import com.lakehouse.catalog.config.CatalogConfig;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
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
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end {@link CoalescingAppendWriter} against {@link IcebergCatalogClient} backed by
 * {@link com.lakehouse.catalog.factory.CatalogFactory JDBC + MinIO} (Parquet on S3A, journal on local disk).
 *
 * <p>Requires MinIO at {@link #MINIO_ENDPOINT} and bucket {@link #BUCKET}. Skipped when unreachable.</p>
 */
class CoalescingAppendWriterMinioIntegrationTest {

    private static final String MINIO_ENDPOINT = "http://127.0.0.1:9000";
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final String BUCKET = "warehouse";
    private static final String REGION = "us-east-1";
    private static final String CATALOG_NAME = "coalescing-append-e2e";
    private static final Path FAKE_HADOOP_HOME = prepareFakeHadoopHome();

    private static final Schema ROW_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get())
    );

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("should_SingleAppendCommit_When_MaxPendingFilesReached_OnMinioWarehouse")
    void should_SingleAppendCommit_When_MaxPendingFilesReached_OnMinioWarehouse() throws IOException {
        assumeTrue(isMinioReachable(), "Requires local MinIO at " + MINIO_ENDPOINT);
        assertThat(FAKE_HADOOP_HOME).exists();
        createBucketIfNotExists();

        String runId = UUID.randomUUID().toString().replace("-", "");
        String namespace = "coal_e2e_ns_" + runId;
        CatalogConfig config = baseConfig(namespace, runId);

        try (IcebergCatalogClient client = new IcebergCatalogClient(config)) {
            client.ensureNamespace();
            client.createTable("coal_batch", ROW_SCHEMA);

            Table before = client.loadTable("coal_batch");
            int snapshotsBefore = snapshotCount(before);

            Path journalDir = tempDir.resolve("journal-batch-" + runId);
            CoalescingAppendWriterConfig writerConfig = CoalescingAppendWriterConfig.builder()
                    .maxPendingFiles(3)
                    .fsyncJournal(false)
                    .maxFlushInterval(Duration.ofHours(1))
                    .build();

            try (CoalescingAppendWriter writer = CoalescingAppendWriter.open(client, "coal_batch", journalDir, writerConfig)) {
                writer.stage(writeOneRow(client, "coal_batch", 1L));
                writer.stage(writeOneRow(client, "coal_batch", 2L));
                assertThat(snapshotCount(client.loadTable("coal_batch"))).isEqualTo(snapshotsBefore);
                writer.stage(writeOneRow(client, "coal_batch", 3L));
            }

            assertThat(snapshotCount(client.loadTable("coal_batch"))).isEqualTo(snapshotsBefore + 1);
            assertThat(client.readRecords("coal_batch"))
                    .extracting(r -> r.getField("id"), r -> r.getField("name"), r -> r.getField("email"))
                    .containsExactlyInAnyOrder(
                            tuple(1L, "n1", "1@x.test"),
                            tuple(2L, "n2", "2@x.test"),
                            tuple(3L, "n3", "3@x.test"));

            client.dropTable("coal_batch", true);
        }
    }

    @Test
    @DisplayName("should_RecoverPendingFromJournal_When_RestartBeforeFlush_OnMinioWarehouse")
    void should_RecoverPendingFromJournal_When_RestartBeforeFlush_OnMinioWarehouse() throws IOException {
        assumeTrue(isMinioReachable(), "Requires local MinIO at " + MINIO_ENDPOINT);
        assertThat(FAKE_HADOOP_HOME).exists();
        createBucketIfNotExists();

        String runId = UUID.randomUUID().toString().replace("-", "");
        String namespace = "coal_e2e_rec_" + runId;
        CatalogConfig config = baseConfig(namespace, runId);
        Path journalDir = tempDir.resolve("journal-recover-" + runId);

        try (IcebergCatalogClient client = new IcebergCatalogClient(config)) {
            client.ensureNamespace();
            client.createTable("coal_recover", ROW_SCHEMA);

            CoalescingAppendWriterConfig hold = CoalescingAppendWriterConfig.builder()
                    .maxPendingFiles(100)
                    .fsyncJournal(false)
                    .flushOnClose(false)
                    .maxFlushInterval(Duration.ofHours(1))
                    .build();

            try (CoalescingAppendWriter first = CoalescingAppendWriter.open(client, "coal_recover", journalDir, hold)) {
                first.stage(writeOneRow(client, "coal_recover", 10L));
                first.stage(writeOneRow(client, "coal_recover", 20L));
            }

            assertThat(client.readRecords("coal_recover")).isEmpty();

            CoalescingAppendWriterConfig normal = CoalescingAppendWriterConfig.builder()
                    .maxPendingFiles(100)
                    .fsyncJournal(false)
                    .maxFlushInterval(Duration.ofHours(1))
                    .build();

            try (CoalescingAppendWriter second = CoalescingAppendWriter.open(client, "coal_recover", journalDir, normal)) {
                // open() replays journal and commits
            }

            assertThat(client.readRecords("coal_recover"))
                    .extracting(r -> r.getField("id"), r -> r.getField("name"))
                    .containsExactlyInAnyOrder(tuple(10L, "n10"), tuple(20L, "n20"));

            client.dropTable("coal_recover", true);
        }
    }

    private CatalogConfig baseConfig(String namespace, String runId) {
        String warehousePrefix = "s3a://" + BUCKET + "/coalescing-writer-e2e-" + runId + "/";
        return CatalogConfig.builder()
                .catalogType(CatalogConfig.CatalogType.JDBC)
                .catalogName(CATALOG_NAME + "-" + runId)
                .defaultNamespace(namespace)
                .warehousePath(warehousePrefix)
                .jdbcUrl("jdbc:h2:file:" + normalizePath(tempDir.resolve("coal-catalog-" + runId)))
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
    }

    private static DataFile writeOneRow(IcebergCatalogClient client, String tableName, long id) throws IOException {
        GenericRecord row = GenericRecord.create(ROW_SCHEMA);
        row.setField("id", id);
        row.setField("name", "n" + id);
        row.setField("email", id + "@x.test");
        return writeParquetDataFile(client, tableName, List.of(row));
    }

    private static int snapshotCount(Table table) {
        return (int) StreamSupport.stream(table.snapshots().spliterator(), false).count();
    }

    private static DataFile writeParquetDataFile(
            IcebergCatalogClient client,
            String tableName,
            List<GenericRecord> records) throws IOException {
        Table table = client.loadTable(tableName);
        Schema schema = table.schema();
        String filename = String.format("%s/data/%s.parquet", table.location(), UUID.randomUUID());
        OutputFile outputFile = table.io().newOutputFile(filename);
        DataWriter<GenericRecord> dataWriter = Parquet.writeData(outputFile)
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .overwrite()
                .withSpec(table.spec())
                .build();
        try {
            for (GenericRecord record : records) {
                dataWriter.write(record);
            }
        } finally {
            dataWriter.close();
        }
        return dataWriter.toDataFile();
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
            Path hadoopHome = Files.createTempDirectory("fake-hadoop-coalesce");
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
