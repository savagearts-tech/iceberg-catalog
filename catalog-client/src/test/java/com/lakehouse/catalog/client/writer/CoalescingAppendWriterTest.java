package com.lakehouse.catalog.client.writer;

import com.lakehouse.catalog.client.IcebergCatalogClient;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

class CoalescingAppendWriterTest {

    private static final String NAMESPACE = "test_tenant";

    private static final Schema USER_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get())
    );

    @TempDir
    Path tempDir;

    private IcebergCatalogClient client;

    @BeforeEach
    void setUp() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test-catalog", java.util.Map.of());
        if (!catalog.namespaceExists(Namespace.of(NAMESPACE))) {
            catalog.createNamespace(Namespace.of(NAMESPACE));
        }
        client = new IcebergCatalogClient(catalog, NAMESPACE);
    }

    @AfterEach
    void tearDown() {
        client.close();
    }

    @Test
    @DisplayName("should_SingleCommit_When_MaxPendingFilesReached")
    void should_SingleCommit_When_MaxPendingFilesReached() throws IOException {
        client.createTable("coal_three", USER_SCHEMA);
        Path journalDir = tempDir.resolve("journal_three");
        CoalescingAppendWriterConfig config = CoalescingAppendWriterConfig.builder()
                .maxPendingFiles(3)
                .fsyncJournal(false)
                .maxFlushInterval(Duration.ofHours(1))
                .build();

        Table tableBefore = client.loadTable("coal_three");
        int snapshotsBefore = snapshotCount(tableBefore);

        try (CoalescingAppendWriter writer = CoalescingAppendWriter.open(client, "coal_three", journalDir, config)) {
            writer.stage(writeOneRow(client, "coal_three", 1L));
            writer.stage(writeOneRow(client, "coal_three", 2L));
            assertThat(snapshotCount(client.loadTable("coal_three"))).isEqualTo(snapshotsBefore);

            writer.stage(writeOneRow(client, "coal_three", 3L));
        }

        Table tableAfter = client.loadTable("coal_three");
        assertThat(snapshotCount(tableAfter)).isEqualTo(snapshotsBefore + 1);
        assertThat(client.readRecords("coal_three")).hasSize(3);
    }

    @Test
    @DisplayName("should_RecoverAndCommit_When_RestartBeforeFlush")
    void should_RecoverAndCommit_When_RestartBeforeFlush() throws IOException {
        client.createTable("coal_recover", USER_SCHEMA);
        Path journalDir = tempDir.resolve("journal_recover");
        CoalescingAppendWriterConfig holdConfig = CoalescingAppendWriterConfig.builder()
                .maxPendingFiles(100)
                .fsyncJournal(false)
                .flushOnClose(false)
                .maxFlushInterval(Duration.ofHours(1))
                .build();

        try (CoalescingAppendWriter first = CoalescingAppendWriter.open(client, "coal_recover", journalDir, holdConfig)) {
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
            // constructor recovery commits pending journal rows
        }

        List<Record> rows = client.readRecords("coal_recover");
        assertThat(rows).hasSize(2);
    }

    @Test
    @DisplayName("should_RebuildDataFile_FromJournalRecord")
    void should_RebuildDataFile_FromJournalRecord() throws IOException {
        client.createTable("coal_rebuild", USER_SCHEMA);
        DataFile original = writeOneRow(client, "coal_rebuild", 99L);
        Table table = client.loadTable("coal_rebuild");
        PendingJournalRecord rec = new PendingJournalRecord(
                "test-entry",
                JournalState.PENDING,
                original.path().toString(),
                original.format().toString().toLowerCase(java.util.Locale.ROOT),
                original.fileSizeInBytes(),
                original.recordCount(),
                null);
        DataFile rebuilt = CoalescingAppendWriter.rebuildDataFile(table, rec);
        assertThat(rebuilt.path()).isEqualTo(original.path());
        assertThat(rebuilt.fileSizeInBytes()).isEqualTo(original.fileSizeInBytes());
        assertThat(rebuilt.recordCount()).isEqualTo(original.recordCount());
    }

    private static DataFile writeOneRow(IcebergCatalogClient client, String tableName, long id) throws IOException {
        GenericRecord row = GenericRecord.create(USER_SCHEMA);
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
}
