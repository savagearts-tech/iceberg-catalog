## ADDED Requirements

### Requirement: Python lakehouse.connect() entry point
The system SHALL provide `lakehouse.connect()` as the primary Python entry point, returning a `Lakehouse` object with `catalog()` (pyiceberg `RestCatalog`), `s3_client()` (`boto3.client("s3")`), and `spark_session()` methods.

#### Scenario: Python connect and list tables
- **WHEN** a Python application calls `lh = lakehouse.connect()` followed by `lh.list_tables()`
- **THEN** the SDK SHALL return a list of Iceberg table names within the tenant's namespace without the developer configuring any connection parameters

### Requirement: PySpark session factory
The SDK SHALL provide `lakehouse.spark_session()` returning a pre-configured `SparkSession` with Gravitino Iceberg REST Catalog properties, including S3A credentials and the Authorization header.

#### Scenario: PySpark session pre-configured
- **WHEN** `lh.spark_session()` is called
- **THEN** the returned `SparkSession` SHALL support `spark.sql("SELECT * FROM lakehouse.<tenant>.events")` without additional Spark configuration

#### Scenario: PySpark session with custom config
- **WHEN** `lh.spark_session(extra_config={"spark.executor.memory": "4g"})` is called
- **THEN** the SparkSession SHALL include both platform defaults and the provided extra config

### Requirement: Python test_lakehouse pytest fixture
The SDK SHALL provide a `test_lakehouse` pytest fixture for unit tests, allowing injection of mock catalog URL, namespace, and credentials.

#### Scenario: pytest fixture usage
- **WHEN** a test function declares `def test_my_etl(test_lakehouse):`
- **THEN** `test_lakehouse.list_tables()` SHALL interact with a locally configured catalog without requiring a running platform

### Requirement: Python package published to internal PyPI
The SDK SHALL be published as `platform-lakehouse-sdk` to the internal PyPI repository, installable via `pip install platform-lakehouse-sdk`.

#### Scenario: pip install in Dockerfile
- **WHEN** an application's Dockerfile includes `RUN pip install platform-lakehouse-sdk==1.0.0`
- **THEN** the package and its dependencies (`pyiceberg`, `boto3`) SHALL be installed and importable as `from platform_lakehouse import lakehouse`

### Requirement: Python context manager support
The `Lakehouse` object SHALL implement the Python context manager protocol (`__enter__` / `__exit__`) for clean resource management.

#### Scenario: Context manager usage
- **WHEN** `with lakehouse.connect() as lh:` is used
- **THEN** upon exiting the `with` block, the token refresh thread SHALL be stopped and HTTP connections SHALL be closed
