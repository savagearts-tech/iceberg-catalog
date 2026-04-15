package com.lakehouse.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Example: Spark SQL reading and writing Iceberg tables via Gravitino REST Catalog.
 *
 * <p>Demonstrates the core Spark data path:
 * Spark SQL → Gravitino Iceberg REST → PG-Iceberg (metadata) → MinIO (Parquet files)</p>
 *
 * <p>Run with spark-submit:</p>
 * <pre>
 * spark-submit \
 *   --class com.lakehouse.spark.SparkIcebergQueryExample \
 *   --master local[*] \
 *   spark-client-1.0.0-SNAPSHOT.jar
 * </pre>
 *
 * @author platform
 * @since 1.0.0
 */
@Slf4j
public class SparkIcebergQueryExample {

    public static void main(String[] args) {
        // 1. Create SparkSession with Gravitino Iceberg REST Catalog
        SparkSession spark = SparkCatalogSessionFactory.builder()
                .catalogUri("http://localhost:9001/iceberg/v1")
                .catalogName("lakehouse")
                .warehousePath("s3a://lakehouse/")
                .minioEndpoint("http://localhost:9000")
                .minioAccessKey("minioadmin")
                .minioSecretKey("minioadmin")
                .sparkMaster("local[*]")
                .build()
                .createSession();

        try {
            // 2. Create namespace (if not exists)
            spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.`default`");

            // 3. Create an Iceberg table via Spark SQL
            spark.sql("""
                CREATE TABLE IF NOT EXISTS lakehouse.`default`.events (
                    event_id    BIGINT,
                    event_type  STRING,
                    user_id     BIGINT,
                    payload     STRING,
                    event_time  TIMESTAMP
                )
                USING iceberg
                """);
            log.info("Table lakehouse.default.events created/verified");

            // 4. Insert sample data
            spark.sql("""
                INSERT INTO lakehouse.`default`.events VALUES
                    (1, 'login',    1001, '{"ip": "192.168.1.1"}',            current_timestamp()),
                    (2, 'purchase', 1001, '{"item": "laptop", "price": 999}', current_timestamp()),
                    (3, 'login',    1002, '{"ip": "10.0.0.5"}',               current_timestamp()),
                    (4, 'logout',   1001, '{}',                               current_timestamp()),
                    (5, 'purchase', 1002, '{"item": "mouse", "price": 29}',   current_timestamp())
                """);
            log.info("Sample data inserted");

            // 5. Query: Read all events
            log.info("--- All Events ---");
            Dataset<Row> allEvents = spark.sql("SELECT * FROM lakehouse.`default`.events ORDER BY event_id");
            allEvents.show(false);

            // 6. Query: Aggregation — count events by type
            log.info("--- Events by Type ---");
            spark.sql("""
                SELECT event_type, COUNT(*) as cnt
                FROM lakehouse.`default`.events
                GROUP BY event_type
                ORDER BY cnt DESC
                """).show();

            // 7. Query: Filter — purchases only
            log.info("--- Purchases Only ---");
            spark.sql("""
                SELECT event_id, user_id, payload, event_time
                FROM lakehouse.`default`.events
                WHERE event_type = 'purchase'
                """).show(false);

            // 8. Read the table written by Java client (if exists)
            try {
                log.info("--- Users table (written by Java client) ---");
                spark.sql("SELECT * FROM lakehouse.`default`.users LIMIT 10").show(false);
            } catch (Exception e) {
                log.info("Users table not found (run WriteParquetExample first): {}", e.getMessage());
            }

            // 9. Show Iceberg metadata: snapshots
            log.info("--- Iceberg Snapshots ---");
            spark.sql("SELECT * FROM lakehouse.`default`.events.snapshots").show(false);

            // 10. Show Iceberg metadata: files
            log.info("--- Iceberg Data Files ---");
            spark.sql("SELECT file_path, file_format, record_count, file_size_in_bytes FROM lakehouse.`default`.events.files").show(false);

            // 11. List all tables in namespace
            log.info("--- All Tables ---");
            spark.sql("SHOW TABLES IN lakehouse.`default`").show();

        } finally {
            spark.stop();
        }
    }
}
