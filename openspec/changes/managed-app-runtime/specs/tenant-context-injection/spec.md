## ADDED Requirements

### Requirement: Catalog context injected as environment variables
The system SHALL automatically inject the following environment variables into every application Pod before container start, sourced from the tenant's registered context in the unified Catalog:

| Variable | Value |
|----------|-------|
| `PLATFORM_CATALOG_URL` | Gravitino Iceberg REST Server URL |
| `PLATFORM_CATALOG_TOKEN` | Short-lived JWT Bearer Token for the tenant |
| `PLATFORM_TENANT_NAMESPACE` | Tenant's Iceberg namespace name |
| `PLATFORM_MINIO_ENDPOINT` | MinIO S3 endpoint URL |
| `PLATFORM_MINIO_ACCESS_KEY` | Tenant-scoped MinIO AccessKey |
| `PLATFORM_MINIO_SECRET_KEY` | Tenant-scoped MinIO SecretKey |

#### Scenario: Application reads Catalog URL from environment
- **WHEN** a Docker application Pod starts under tenant `acme`
- **THEN** `PLATFORM_CATALOG_URL` SHALL be set to `http://gravitino:9001/iceberg/v1` and `PLATFORM_TENANT_NAMESPACE` SHALL be set to `acme`

#### Scenario: Application reads MinIO credentials from environment
- **WHEN** a Spark application Pod starts
- **THEN** `PLATFORM_MINIO_ACCESS_KEY` and `PLATFORM_MINIO_SECRET_KEY` SHALL contain the tenant's scoped credentials fetched from Secret Manager at Pod creation time

### Requirement: Spark catalog configuration file injection
For Spark-type applications, the system SHALL additionally create a mounted file at `/platform/catalog/spark-defaults.conf` containing the pre-configured Spark properties for the Gravitino Iceberg REST Catalog.

#### Scenario: Spark-defaults.conf correctly populated
- **WHEN** a Spark application Pod is created for tenant `acme`
- **THEN** `/platform/catalog/spark-defaults.conf` SHALL contain `spark.sql.catalog.lakehouse.uri=http://gravitino:9001/iceberg/v1`, `spark.sql.catalog.lakehouse.header.Authorization=Bearer <token>`, and the MinIO S3A credentials properties

### Requirement: Credentials injected via init container, not Pod Spec
Tenant credentials (MinIO AccessKey, JWT Token) SHALL be fetched from Secret Manager by a platform init container at Pod startup time, written to a shared in-memory volume, and projected into the application container. Credentials SHALL NOT be stored in the Pod Spec or K8s Secret objects.

#### Scenario: Credentials not visible in Pod Spec
- **WHEN** a platform administrator inspects the Pod spec via `kubectl get pod <pod-name> -o yaml`
- **THEN** the `env` section SHALL NOT contain `PLATFORM_MINIO_ACCESS_KEY` or `PLATFORM_MINIO_SECRET_KEY` values in plaintext; they SHALL reference an ephemeral volume

#### Scenario: Init container failure blocks Pod startup
- **WHEN** the init container fails to fetch credentials from Secret Manager (e.g., Vault is unreachable)
- **THEN** the Pod SHALL remain in `Init:Error` state and SHALL NOT start the application container, and the run SHALL transition to `FAILED` with reason `CONTEXT_INJECTION_FAILED`

### Requirement: Context injection is transparent to the application
Applications SHALL require no code-level changes to consume platform context. The platform SHALL not mandate any specific SDK or library; environment variables SHALL be the sole integration contract.

#### Scenario: Unmodified application consumes catalog URL
- **WHEN** an existing application reads `os.environ["PLATFORM_CATALOG_URL"]` or equivalent
- **THEN** it SHALL receive the correct tenant-scoped Gravitino Iceberg REST URL without any application code changes
