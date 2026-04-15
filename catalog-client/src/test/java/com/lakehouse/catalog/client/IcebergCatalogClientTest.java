package com.lakehouse.catalog.client;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * TDD tests for {@link IcebergCatalogClient}.
 *
 * <p>Uses InMemoryCatalog (Iceberg built-in) to isolate from external dependencies.
 * These tests define the behavioral contract that the client must satisfy.</p>
 *
 * @author platform
 * @since 1.0.0
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IcebergCatalogClientTest {

    private static final String NAMESPACE = "test_tenant";

    private static final Schema USER_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get())
    );

    private IcebergCatalogClient client;

    @BeforeEach
    void setUp() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test-catalog", java.util.Map.of());

        // Ensure namespace exists
        if (!catalog.namespaceExists(Namespace.of(NAMESPACE))) {
            catalog.createNamespace(Namespace.of(NAMESPACE));
        }

        client = new IcebergCatalogClient(catalog, NAMESPACE);
    }

    @AfterEach
    void tearDown() {
        client.close();
    }

    // ==================== Table Creation ====================

    @Test
    @DisplayName("should_CreateTable_When_TableDoesNotExist")
    void should_CreateTable_When_TableDoesNotExist() {
        Table table = client.createTable("users", USER_SCHEMA);

        assertThat(table).isNotNull();
        assertThat(table.schema().columns()).hasSize(3);
        assertThat(table.schema().findField("id").type()).isEqualTo(Types.LongType.get());
        assertThat(table.schema().findField("name").type()).isEqualTo(Types.StringType.get());
        assertThat(table.schema().findField("email").type()).isEqualTo(Types.StringType.get());
    }

    @Test
    @DisplayName("should_ReturnExistingTable_When_TableAlreadyExists")
    void should_ReturnExistingTable_When_TableAlreadyExists() {
        client.createTable("users", USER_SCHEMA);
        Table table = client.createTable("users", USER_SCHEMA);

        assertThat(table).isNotNull();
        assertThat(table.schema().columns()).hasSize(3);
    }

    @Test
    @DisplayName("should_ReturnTrue_When_TableExists")
    void should_ReturnTrue_When_TableExists() {
        client.createTable("users", USER_SCHEMA);

        assertThat(client.tableExists("users")).isTrue();
    }

    @Test
    @DisplayName("should_ReturnFalse_When_TableDoesNotExist")
    void should_ReturnFalse_When_TableDoesNotExist() {
        assertThat(client.tableExists("non_existent")).isFalse();
    }

    // ==================== Parquet Write ====================

    @Test
    @DisplayName("should_WriteRecordsAsParquet_When_ValidRecordsProvided")
    void should_WriteRecordsAsParquet_When_ValidRecordsProvided() throws IOException {
        client.createTable("users", USER_SCHEMA);

        List<GenericRecord> records = createSampleRecords(5);
        client.writeRecords("users", records);

        // Verify by reading back
        List<Record> readBack = client.readRecords("users");
        assertThat(readBack).hasSize(5);
    }

    @Test
    @DisplayName("should_AppendRecords_When_WrittenMultipleTimes")
    void should_AppendRecords_When_WrittenMultipleTimes() throws IOException {
        client.createTable("users", USER_SCHEMA);

        client.writeRecords("users", createSampleRecords(3));
        client.writeRecords("users", createSampleRecords(2));

        List<Record> readBack = client.readRecords("users");
        assertThat(readBack).hasSize(5);
    }

    @Test
    @DisplayName("should_SkipWrite_When_EmptyRecordsList")
    void should_SkipWrite_When_EmptyRecordsList() throws IOException {
        client.createTable("users", USER_SCHEMA);

        // Should not throw, just skip
        client.writeRecords("users", List.of());

        List<Record> readBack = client.readRecords("users");
        assertThat(readBack).isEmpty();
    }

    @Test
    @DisplayName("should_SkipWrite_When_NullRecordsList")
    void should_SkipWrite_When_NullRecordsList() throws IOException {
        client.createTable("users", USER_SCHEMA);

        // Should not throw, just skip
        client.writeRecords("users", null);

        List<Record> readBack = client.readRecords("users");
        assertThat(readBack).isEmpty();
    }

    // ==================== Parquet Read ====================

    @Test
    @DisplayName("should_ReadRecordsWithCorrectValues_When_DataWritten")
    void should_ReadRecordsWithCorrectValues_When_DataWritten() throws IOException {
        client.createTable("users", USER_SCHEMA);

        List<GenericRecord> records = new ArrayList<>();
        GenericRecord record = GenericRecord.create(USER_SCHEMA);
        record.setField("id", 42L);
        record.setField("name", "Alice");
        record.setField("email", "alice@example.com");
        records.add(record);

        client.writeRecords("users", records);

        List<Record> readBack = client.readRecords("users");
        assertThat(readBack).hasSize(1);

        Record first = readBack.get(0);
        assertThat(first.getField("id")).isEqualTo(42L);
        assertThat(first.getField("name")).isEqualTo("Alice");
        assertThat(first.getField("email")).isEqualTo("alice@example.com");
    }

    @Test
    @DisplayName("should_ReturnEmptyList_When_TableIsEmpty")
    void should_ReturnEmptyList_When_TableIsEmpty() throws IOException {
        client.createTable("users", USER_SCHEMA);

        List<Record> readBack = client.readRecords("users");
        assertThat(readBack).isEmpty();
    }

    // ==================== Table Operations ====================

    @Test
    @DisplayName("should_ListTables_When_TablesExist")
    void should_ListTables_When_TablesExist() {
        client.createTable("users", USER_SCHEMA);
        client.createTable("events", USER_SCHEMA);

        List<TableIdentifier> tables = client.listTables();
        assertThat(tables)
                .extracting(TableIdentifier::name)
                .containsExactlyInAnyOrder("users", "events");
    }

    @Test
    @DisplayName("should_ReturnEmptyList_When_NoTables")
    void should_ReturnEmptyList_When_NoTables() {
        List<TableIdentifier> tables = client.listTables();
        assertThat(tables).isEmpty();
    }

    @Test
    @DisplayName("should_DropTable_When_TableExists")
    void should_DropTable_When_TableExists() {
        client.createTable("users", USER_SCHEMA);
        assertThat(client.tableExists("users")).isTrue();

        boolean dropped = client.dropTable("users", true);
        assertThat(dropped).isTrue();
        assertThat(client.tableExists("users")).isFalse();
    }

    @Test
    @DisplayName("should_LoadTable_When_TableExists")
    void should_LoadTable_When_TableExists() {
        client.createTable("users", USER_SCHEMA);

        Table table = client.loadTable("users");
        assertThat(table).isNotNull();
        assertThat(table.schema().findField("id")).isNotNull();
    }

    @Test
    @DisplayName("should_ThrowException_When_LoadingNonExistentTable")
    void should_ThrowException_When_LoadingNonExistentTable() {
        assertThatThrownBy(() -> client.loadTable("non_existent"))
                .isInstanceOf(org.apache.iceberg.exceptions.NoSuchTableException.class);
    }

    // ==================== Namespace ====================

    @Test
    @DisplayName("should_CreateNamespace_When_NotExists")
    void should_CreateNamespace_When_NotExists() {
        // Client was initialized with a fresh namespace
        // ensureNamespace should be idempotent
        client.ensureNamespace();
        // No exception means success
    }

    // ==================== Schema Evolution ====================

    @Test
    @DisplayName("should_WriteAndReadWithSchemaEvolution_When_ColumnAdded")
    void should_WriteAndReadWithSchemaEvolution_When_ColumnAdded() throws IOException {
        // Create table with initial schema
        client.createTable("users", USER_SCHEMA);

        // Write initial records
        client.writeRecords("users", createSampleRecords(3));

        // Evolve schema: add a new column
        Table table = client.loadTable("users");
        table.updateSchema()
                .addColumn("age", Types.IntegerType.get())
                .commit();

        // Read back — old records should have null for new column
        List<Record> readBack = client.readRecords("users");
        assertThat(readBack).hasSize(3);
        assertThat(readBack.get(0).getField("age")).isNull();
    }

    // ==================== Large Batch ====================

    @Test
    @DisplayName("should_HandleLargeBatch_When_WritingManyRecords")
    void should_HandleLargeBatch_When_WritingManyRecords() throws IOException {
        client.createTable("users", USER_SCHEMA);

        List<GenericRecord> records = createSampleRecords(1000);
        client.writeRecords("users", records);

        List<Record> readBack = client.readRecords("users");
        assertThat(readBack).hasSize(1000);
    }

    // ==================== Helpers ====================

    private List<GenericRecord> createSampleRecords(int count) {
        List<GenericRecord> records = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            GenericRecord record = GenericRecord.create(USER_SCHEMA);
            record.setField("id", (long) i);
            record.setField("name", "user_" + i);
            record.setField("email", "user_" + i + "@test.com");
            records.add(record);
        }
        return records;
    }
}
