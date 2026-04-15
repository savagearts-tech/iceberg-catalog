## ADDED Requirements

### Requirement: Platform context auto-discovery from environment
The SDK SHALL automatically read platform context from the following environment variables at `connect()` time: `PLATFORM_CATALOG_URL`, `PLATFORM_CATALOG_TOKEN`, `PLATFORM_TENANT_NAMESPACE`, `PLATFORM_MINIO_ENDPOINT`, `PLATFORM_MINIO_ACCESS_KEY`, `PLATFORM_MINIO_SECRET_KEY`. If any required variable is missing, the SDK SHALL throw a descriptive `PlatformContextMissingException`.

#### Scenario: Successful auto-discovery
- **WHEN** `PlatformLakehouse.connect()` is called with all required environment variables set
- **THEN** the SDK SHALL return a configured `Lakehouse` instance with a working Iceberg Catalog and S3 Client

#### Scenario: Missing environment variable
- **WHEN** `connect()` is called but `PLATFORM_CATALOG_URL` is not set
- **THEN** the SDK SHALL throw `PlatformContextMissingException` with message listing the missing variable name

### Requirement: Namespace forced scoping
All Iceberg operations (loadTable, listTables, createTable, dropTable) performed through the SDK SHALL be automatically scoped to `PLATFORM_TENANT_NAMESPACE`. Explicit cross-namespace access SHALL be rejected.

#### Scenario: Implicit namespace resolution
- **WHEN** `lakehouse.loadTable("events")` is called for a tenant with namespace `acme`
- **THEN** the SDK SHALL internally invoke `catalog.loadTable(Namespace.of("acme"), "events")`

#### Scenario: Cross-namespace access rejected
- **WHEN** `lakehouse.loadTable(Namespace.of("other-tenant"), "events")` is called
- **THEN** the SDK SHALL throw `CrossNamespaceException` without forwarding the request to Gravitino

#### Scenario: listTables scoped to tenant
- **WHEN** `lakehouse.listTables()` is called
- **THEN** the SDK SHALL return only tables within the tenant's own namespace

### Requirement: JWT Token auto-refresh
The SDK SHALL monitor the JWT Token expiry time and automatically refresh it when the remaining validity is less than 5 minutes. The refresh mechanism SHALL re-read the token from the environment or the shared volume file, not call the IdP directly.

#### Scenario: Token refreshed before expiry
- **WHEN** a `connect()` session has been active for longer than the token's TTL minus 5 minutes
- **THEN** the SDK SHALL re-read `PLATFORM_CATALOG_TOKEN` and update the Catalog client's Authorization header transparently

#### Scenario: Token refresh failure logged but non-fatal
- **WHEN** the token re-read returns the same expired token (Sidecar has not yet updated)
- **THEN** the SDK SHALL log a WARNING and retry after 30 seconds; existing in-flight operations SHALL continue with the current token

### Requirement: S3 client pre-configured for tenant
The SDK SHALL provide a pre-configured S3 client (`lakehouse.s3Client()`) pointing to `PLATFORM_MINIO_ENDPOINT` with the tenant's AccessKey/SecretKey, and with path-style access enabled.

#### Scenario: S3 client writes to tenant path
- **WHEN** `lakehouse.s3Client().putObject("my-data.parquet", data)` is called for tenant `acme`
- **THEN** the object SHALL be written to `s3a://lakehouse/acme/my-data.parquet` using the tenant's scoped credentials

### Requirement: Graceful shutdown
The SDK SHALL support `lakehouse.close()` (or implement `AutoCloseable` / context manager) that stops the token refresh daemon thread and releases HTTP connections cleanly.

#### Scenario: Close releases resources
- **WHEN** `lakehouse.close()` is called
- **THEN** the token refresh thread SHALL stop within 1 second and no background HTTP connections SHALL remain open
