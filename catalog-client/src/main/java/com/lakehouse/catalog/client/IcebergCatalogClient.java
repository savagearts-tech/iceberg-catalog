package com.lakehouse.catalog.client;

import com.lakehouse.catalog.config.CatalogConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import com.lakehouse.catalog.factory.CatalogFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Iceberg Catalog client for writing and reading Parquet data.
 *
 * <p>Supports two construction modes:</p>
 * <ul>
 *   <li>Production: {@link #IcebergCatalogClient(CatalogConfig)} — builds RESTCatalog from config</li>
 *   <li>Testing: {@link #IcebergCatalogClient(Catalog, String)} — accepts injected Catalog (TDD-friendly)</li>
 * </ul>
 *
 * @author platform
 * @since 1.0.0
 */
@Slf4j
public class IcebergCatalogClient implements Closeable {

    private final Catalog catalog;
    private final String defaultNamespace;

    /**
     * Production constructor — builds Catalog from configuration using CatalogFactory.
     *
     * @param config catalog connection configuration
     */
    public IcebergCatalogClient(CatalogConfig config) {
        this.defaultNamespace = config.getDefaultNamespace();
        this.catalog = CatalogFactory.build(config);
        log.info("IcebergCatalogClient initialized: type={}, namespace={}", config.getCatalogType(), defaultNamespace);
    }

    /**
     * Test constructor — accepts any Catalog implementation (HadoopCatalog, InMemory, etc.)
     *
     * @param catalog          injected catalog instance
     * @param defaultNamespace default namespace for operations
     */
    public IcebergCatalogClient(Catalog catalog, String defaultNamespace) {
        this.catalog = catalog;
        this.defaultNamespace = defaultNamespace;
        log.info("IcebergCatalogClient initialized with injected catalog, namespace={}", defaultNamespace);
    }

    /**
     * Create an Iceberg table in the default namespace.
     *
     * @param tableName table name
     * @param schema    Iceberg schema definition
     * @return the created Table
     */
    public Table createTable(String tableName, Schema schema) {
        var tableId = tableIdentifier(tableName);

        if (catalog.tableExists(tableId)) {
            log.info("Table already exists, loading: {}", tableId);
            return catalog.loadTable(tableId);
        }

        log.info("Creating table: {}", tableId);
        Table table = catalog.createTable(tableId, schema);
        log.info("Table created: {}, location={}", tableId, table.location());
        return table;
    }

    /**
     * Load an existing Iceberg table from the default namespace.
     *
     * @param tableName table name
     * @return the loaded Table
     */
    public Table loadTable(String tableName) {
        var tableId = tableIdentifier(tableName);
        log.debug("Loading table: {}", tableId);
        return catalog.loadTable(tableId);
    }

    /**
     * Check if a table exists in the default namespace.
     *
     * @param tableName table name
     * @return true if exists
     */
    public boolean tableExists(String tableName) {
        return catalog.tableExists(tableIdentifier(tableName));
    }

    /**
     * Write a batch of records to an Iceberg table as Parquet data files.
     *
     * @param tableName table name in the default namespace
     * @param records   list of GenericRecord to write
     * @throws IOException if write fails
     */
    public void writeRecords(String tableName, List<GenericRecord> records) throws IOException {
        if (records == null || records.isEmpty()) {
            log.warn("No records to write to table {}", tableName);
            return;
        }

        Table table = loadTable(tableName);
        Schema schema = table.schema();

        String filename = String.format("%s/data/%s.parquet", table.location(), UUID.randomUUID());
        OutputFile outputFile = table.io().newOutputFile(filename);

        log.info("Writing {} records to table {} as Parquet: {}", records.size(), tableName, filename);

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

        // Commit the data file to the table
        table.newAppend()
                .appendFile(dataWriter.toDataFile())
                .commit();

        log.info("Committed {} records to table {}", records.size(), tableName);
    }

    /**
     * Read all records from an Iceberg table.
     *
     * @param tableName table name in the default namespace
     * @return list of records
     * @throws IOException if read fails
     */
    public List<Record> readRecords(String tableName) throws IOException {
        Table table = loadTable(tableName);
        log.info("Reading records from table: {}", tableName);

        try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
            List<Record> records = com.google.common.collect.Lists.newArrayList(iterable);
            log.info("Read {} records from table {}", records.size(), tableName);
            return records;
        }
    }

    /**
     * Create the default namespace if it does not exist.
     */
    public void ensureNamespace() {
        if (catalog instanceof SupportsNamespaces nsSupport) {
            Namespace ns = Namespace.of(defaultNamespace);
            if (!nsSupport.namespaceExists(ns)) {
                log.info("Creating namespace: {}", ns);
                nsSupport.createNamespace(ns);
            } else {
                log.debug("Namespace already exists: {}", ns);
            }
        }
    }

    /**
     * List all tables in the default namespace.
     *
     * @return list of table identifiers
     */
    public List<TableIdentifier> listTables() {
        return catalog.listTables(Namespace.of(defaultNamespace));
    }

    /**
     * Drop a table from the default namespace.
     *
     * @param tableName table name
     * @param purge     if true, also delete data files
     * @return true if the table was dropped
     */
    public boolean dropTable(String tableName, boolean purge) {
        var tableId = tableIdentifier(tableName);
        log.info("Dropping table: {}, purge={}", tableId, purge);
        return catalog.dropTable(tableId, purge);
    }

    @Override
    public void close() {
        if (catalog instanceof Closeable closeable) {
            try {
                closeable.close();
                log.info("IcebergCatalogClient closed");
            } catch (IOException e) {
                log.error("Failed to close catalog: {}", e.getMessage(), e);
            }
        }
    }

    private TableIdentifier tableIdentifier(String tableName) {
        return TableIdentifier.of(Namespace.of(defaultNamespace), tableName);
    }

}
