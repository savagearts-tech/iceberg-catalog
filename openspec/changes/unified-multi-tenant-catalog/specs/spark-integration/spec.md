## ADDED Requirements

### Requirement: Spark Gravitino Iceberg REST Catalog connector
The system SHALL configure Spark to use the Gravitino Iceberg REST Catalog as the default catalog for all Iceberg table operations, using the `org.apache.iceberg.spark.SparkCatalog` with REST catalog implementation.

#### Scenario: Spark reads Iceberg table via Gravitino REST
- **WHEN** a Spark job executes `spark.table("lakehouse.<namespace>.<table>")` with Gravitino Iceberg REST catalog configured
- **THEN** Spark SHALL resolve table metadata from Gravitino and read data files from MinIO without direct schema registry access

#### Scenario: Spark writes new Iceberg table partition
- **WHEN** a Spark structured streaming or batch job writes to an Iceberg table
- **THEN** new data files SHALL be committed to MinIO at the correct tenant path and a new Iceberg snapshot SHALL be created in Gravitino metadata

### Requirement: Spark catalog configuration via SparkConf
The system SHALL provide a reference Spark configuration (SparkConf or `spark-defaults.conf`) for connecting to the Gravitino Iceberg REST Catalog, including OAuth2 or bearer token for tenant identity propagation.

#### Scenario: Configuration documented and validated
- **WHEN** a data engineer applies the reference Spark configuration to their session
- **THEN** `spark.sql("SHOW NAMESPACES IN lakehouse")` SHALL list all namespaces the tenant is authorized to access

### Requirement: Spark SQL DDL for Iceberg tables
The system SHALL support Spark SQL DDL operations (`CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`) on Iceberg tables through Gravitino, subject to tenant namespace permissions.

#### Scenario: CREATE TABLE via Spark SQL
- **WHEN** a tenant executes `CREATE TABLE lakehouse.<tenant>.my_table (id BIGINT, name STRING) USING iceberg`
- **THEN** Gravitino SHALL create the table metadata and the data directory SHALL be initialized in `s3a://lakehouse/<tenant>/my_table/`

#### Scenario: DROP TABLE rejected for unauthorized namespace
- **WHEN** a tenant attempts to drop a table in another tenant's namespace
- **THEN** the operation SHALL fail with `AccessDeniedException` and the table SHALL remain intact
