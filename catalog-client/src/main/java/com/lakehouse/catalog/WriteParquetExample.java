package com.lakehouse.catalog;

import com.lakehouse.catalog.client.IcebergCatalogClient;
import com.lakehouse.catalog.config.CatalogConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;

/**
 * Example: Java program writing Iceberg Parquet via Gravitino REST Catalog.
 *
 * <p>Demonstrates the core data path:
 * Java App → Gravitino Iceberg REST → PG-Iceberg (metadata) → MinIO (Parquet files)</p>
 *
 * @author platform
 * @since 1.0.0
 */
@Slf4j
public class WriteParquetExample {

    public static void main(String[] args) throws Exception {
        // 1. Configure connection to Gravitino Iceberg REST Catalog + MinIO
        CatalogConfig config = CatalogConfig.builder()
                .catalogUri("http://localhost:9001/iceberg/v1")
                .catalogName("lakehouse")
                .warehousePath("s3a://lakehouse/")
                .minioEndpoint("http://localhost:9000")
                .minioAccessKey("minioadmin")
                .minioSecretKey("minioadmin")
                .defaultNamespace("default")
                .build();

        try (IcebergCatalogClient client = new IcebergCatalogClient(config)) {
            // 2. Ensure the namespace exists
            client.ensureNamespace();

            // 3. Define the Iceberg table schema
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "name", Types.StringType.get()),
                    Types.NestedField.optional(3, "email", Types.StringType.get()),
                    Types.NestedField.optional(4, "created_at", Types.TimestampType.withZone())
            );

            // 4. Create the table (idempotent — loads if already exists)
            client.createTable("users", schema);

            // 5. Build sample records
            List<GenericRecord> records = new ArrayList<>();
            for (int i = 1; i <= 100; i++) {
                GenericRecord record = GenericRecord.create(schema);
                record.setField("id", (long) i);
                record.setField("name", "user_" + i);
                record.setField("email", "user_" + i + "@example.com");
                record.setField("created_at", java.time.OffsetDateTime.now());
                records.add(record);
            }

            // 6. Write records as Parquet to MinIO via Iceberg
            client.writeRecords("users", records);

            // 7. Read back and verify
            List<Record> readBack = client.readRecords("users");
            log.info("Verification: wrote {} records, read back {} records", records.size(), readBack.size());

            // 8. List all tables
            log.info("Tables in namespace '{}': {}", config.getDefaultNamespace(), client.listTables());
        }
    }
}
