package com.lakehouse.catalog.client;

import com.lakehouse.catalog.config.CatalogConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration test for {@link IcebergCatalogClient} via real Iceberg REST Catalog.
 *
 * <p>Full production-like data path tested end-to-end:</p>
 * <pre>
 *   IcebergCatalogClient
 *       → RESTCatalog (HTTP)
 *           → Iceberg REST Catalog Server (apache/iceberg-rest-fixture)
 *               → MinIO (S3-compatible Parquet storage)
 * </pre>
 *
 * <p>No mocks, no InMemoryCatalog — this tests exactly the same path
 * that runs in production against Gravitino Iceberg REST Server.</p>
 *
 * @author platform
 * @since 1.0.0
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Disabled("依赖环境 Docker/Podman，暂时跳过以免影响本地编译")
class IcebergCatalogClientIntegrationTest {

    private static final String BUCKET = "warehouse";
    private static final String NAMESPACE = "integration_test";
    private static final String ACCESS_KEY = "admin";
    private static final String SECRET_KEY = "password";

    private static final Schema EVENT_SCHEMA = new Schema(
            Types.NestedField.required(1, "event_id", Types.LongType.get()),
            Types.NestedField.optional(2, "event_type", Types.StringType.get()),
            Types.NestedField.optional(3, "user_id", Types.LongType.get()),
            Types.NestedField.optional(4, "payload", Types.StringType.get())
    );

    // Shared network so REST Catalog can reach MinIO by hostname
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

    private IcebergCatalogClient client;

    @BeforeEach
    void setUp() {
        // Create the bucket in MinIO
        createBucketIfNotExists();

        // Build CatalogConfig pointing to the real Iceberg REST Catalog
        String restUrl = "http://" + restCatalog.getHost() + ":" + restCatalog.getMappedPort(8181);
        String minioUrl = minio.getS3URL();

        CatalogConfig config = CatalogConfig.builder()
                .catalogUri(restUrl)
                .catalogName("rest-integration")
                .warehousePath("s3://" + BUCKET + "/")
                .minioEndpoint(minioUrl)
                .minioAccessKey(ACCESS_KEY)
                .minioSecretKey(SECRET_KEY)
                .minioRegion("us-east-1")
                .defaultNamespace(NAMESPACE)
                .build();

        // Use the production constructor — RESTCatalog, no mocks
        client = new IcebergCatalogClient(config);
        client.ensureNamespace();
    }

    @AfterEach
    void tearDown() {
        // Clean up tables for test isolation
        client.listTables().forEach(t -> client.dropTable(t.name(), true));
        client.close();
    }

    @Test
    @Order(1)
    @DisplayName("should_CreateTableViaREST_When_CatalogConnected")
    void should_CreateTableViaREST_When_CatalogConnected() {
        Table table = client.createTable("events", EVENT_SCHEMA);

        assertThat(table).isNotNull();
        assertThat(table.location()).contains("s3://");
        assertThat(table.schema().findField("event_id")).isNotNull();
        assertThat(table.schema().findField("event_type")).isNotNull();
        assertThat(table.schema().findField("user_id")).isNotNull();
        assertThat(table.schema().findField("payload")).isNotNull();
    }

    @Test
    @Order(2)
    @DisplayName("should_WriteAndReadParquetViaREST_When_RecordsProvided")
    void should_WriteAndReadParquetViaREST_When_RecordsProvided() throws IOException {
        client.createTable("events", EVENT_SCHEMA);

        List<GenericRecord> records = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            GenericRecord record = GenericRecord.create(EVENT_SCHEMA);
            record.setField("event_id", (long) i);
            record.setField("event_type", i % 2 == 0 ? "purchase" : "login");
            record.setField("user_id", 1000L + i);
            record.setField("payload", "{\"key\": \"value_" + i + "\"}");
            records.add(record);
        }

        client.writeRecords("events", records);

        // Full round-trip: RESTCatalog → MinIO → read back
        List<Record> readBack = client.readRecords("events");
        assertThat(readBack).hasSize(10);

        // Verify content correctness
        Record firstLogin = readBack.stream()
                .filter(r -> "login".equals(r.getField("event_type")))
                .findFirst()
                .orElseThrow();
        assertThat(firstLogin.getField("user_id")).isNotNull();
    }

    @Test
    @Order(3)
    @DisplayName("should_SupportMultipleCommitsViaREST_When_AppendingData")
    void should_SupportMultipleCommitsViaREST_When_AppendingData() throws IOException {
        client.createTable("events", EVENT_SCHEMA);

        // Two separate commits through REST API
        client.writeRecords("events", createEventRecords(1, 5));
        client.writeRecords("events", createEventRecords(6, 10));

        List<Record> readBack = client.readRecords("events");
        assertThat(readBack).hasSize(10);
    }

    @Test
    @Order(4)
    @DisplayName("should_TrackSnapshotsViaREST_When_MultipleCommits")
    void should_TrackSnapshotsViaREST_When_MultipleCommits() throws IOException {
        client.createTable("events", EVENT_SCHEMA);

        client.writeRecords("events", createEventRecords(1, 3));
        client.writeRecords("events", createEventRecords(4, 6));

        Table table = client.loadTable("events");
        long snapshotCount = com.google.common.collect.Iterables.size(table.snapshots());
        assertThat(snapshotCount).isEqualTo(2);
    }

    // ==================== Helpers ====================

    private List<GenericRecord> createEventRecords(int from, int to) {
        List<GenericRecord> records = new ArrayList<>();
        for (int i = from; i <= to; i++) {
            GenericRecord record = GenericRecord.create(EVENT_SCHEMA);
            record.setField("event_id", (long) i);
            record.setField("event_type", "event_" + i);
            record.setField("user_id", 1000L + i);
            record.setField("payload", "{}");
            records.add(record);
        }
        return records;
    }

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
