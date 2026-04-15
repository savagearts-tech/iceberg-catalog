## ADDED Requirements

### Requirement: PlatformLakehouse Java entry point
The system SHALL provide `PlatformLakehouse.connect()` as the primary Java entry point, returning a `Lakehouse` object that exposes `catalog()` (Iceberg `RESTCatalog`), `s3Client()` (`S3Client`), and `sparkSession()` methods.

#### Scenario: Java connect and load table
- **WHEN** a Java application calls `PlatformLakehouse.connect().loadTable("events")`
- **THEN** the SDK SHALL return an Iceberg `Table` object loaded from the tenant's namespace without the developer specifying any connection parameters

### Requirement: SparkSession factory method
The SDK SHALL provide `lakehouse.sparkSession()` that returns a pre-configured `SparkSession` with `spark.sql.catalog.lakehouse` properties set to use the Gravitino Iceberg REST Catalog, including the Authorization header and S3A credentials.

#### Scenario: SparkSession pre-configured
- **WHEN** `lakehouse.sparkSession()` is called
- **THEN** the returned `SparkSession` SHALL have `spark.sql.catalog.lakehouse` configured with the correct URI, token header, and MinIO credentials, and `spark.sql("SELECT * FROM lakehouse.<tenant>.events")` SHALL work without additional configuration

#### Scenario: SparkSession customizable
- **WHEN** `lakehouse.sparkSession(builder -> builder.config("spark.executor.memory", "4g"))` is called with a custom configurator
- **THEN** the returned SparkSession SHALL include both the platform defaults and the user-provided configuration

### Requirement: TestLakehouse test helper
The system SHALL provide `TestLakehouse.builder()` for Java unit tests, allowing developers to inject mock catalog URL, namespace, and credentials without requiring a running platform environment.

#### Scenario: TestLakehouse in unit test
- **WHEN** a developer creates `TestLakehouse.builder().catalogUrl("http://localhost:9001/iceberg/v1").namespace("test-ns").build()`
- **THEN** `loadTable("events")` SHALL attempt to load from `test-ns.events` on the specified local catalog

### Requirement: Dependency isolation via Maven shade
The SDK SHALL shade or relocate its transitive dependencies (Iceberg REST client, AWS SDK classes) to avoid classpath conflicts with application dependencies.

#### Scenario: No classpath conflict with user Iceberg version
- **WHEN** an application depends on `iceberg-core:1.6.0` and uses `platform-lakehouse-sdk-java` which bundles `iceberg-core:1.4.3`
- **THEN** both versions SHALL coexist without `NoSuchMethodError` or `ClassNotFoundException`

### Requirement: Maven BOM for version alignment
The SDK SHALL publish a Maven BOM (`platform-lakehouse-bom`) that aligns the SDK version with compatible Iceberg, Spark, and AWS SDK versions.

#### Scenario: BOM imported in application POM
- **WHEN** a developer adds `<dependencyManagement><dependencies><dependency>platform-lakehouse-bom</dependency></dependencies></dependencyManagement>`
- **THEN** all SDK transitive dependency versions SHALL be managed by the BOM, preventing version conflicts
