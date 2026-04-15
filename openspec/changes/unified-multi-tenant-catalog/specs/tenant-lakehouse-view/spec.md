## ADDED Requirements

### Requirement: Tenant Lakehouse dashboard entry point
The system SHALL provide each tenant with a dedicated Lakehouse view entry point that shows their authorized catalogs, namespaces, tables, and storage usage in a unified interface.

#### Scenario: Tenant logs in and sees their Lakehouse
- **WHEN** a tenant user authenticates and accesses the Lakehouse view
- **THEN** the view SHALL display only the tenant's authorized namespaces, their Iceberg tables, latest snapshot timestamp, and MinIO storage usage

#### Scenario: Empty state for new tenant
- **WHEN** a newly provisioned tenant accesses their Lakehouse view
- **THEN** the view SHALL display an empty namespace with guidance to create their first table

### Requirement: Per-tenant Iceberg namespace summary
The system SHALL display per-tenant Iceberg namespace metadata including table count, total files, total size, and the most recent snapshot timestamp.

#### Scenario: Namespace summary displayed
- **WHEN** a tenant accesses their namespace overview
- **THEN** the system SHALL aggregate and display: total tables, total Iceberg snapshots, total data size in MinIO, and last write timestamp

#### Scenario: Namespace summary refreshed on new snapshot
- **WHEN** a new Iceberg snapshot is committed
- **THEN** the tenant namespace summary SHALL reflect updated counts within 60 seconds

### Requirement: Iceberg catalog URL per tenant
Each tenant SHALL be provided with a dedicated Iceberg REST Catalog URL pre-configured with their namespace for use in Spark or other compatible engines.

#### Scenario: Tenant retrieves their Iceberg catalog URL
- **WHEN** a tenant views their catalog connection settings
- **THEN** the system SHALL display the Iceberg REST Catalog URI, spark.sql.catalog configurations, and their scoped access token

#### Scenario: Catalog URL uses tenant-scoped bearer token
- **WHEN** the tenant's Iceberg REST catalog URL is used in a Spark session
- **THEN** all table operations SHALL be scoped to the tenant's namespace and cross-namespace access SHALL return 403
