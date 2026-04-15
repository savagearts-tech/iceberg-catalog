## ADDED Requirements

### Requirement: Iceberg REST Catalog registration in Gravitino
The system SHALL register Gravitino's built-in Iceberg REST Server as a Lakehouse Iceberg Catalog within the platform metalake, using `provider=lakehouse-iceberg` and MinIO as the warehouse backend.

#### Scenario: Iceberg catalog creation
- **WHEN** a platform administrator sends `POST /api/metalakes/{metalake}/catalogs` with `type=RELATIONAL` and `provider=lakehouse-iceberg`
- **THEN** Gravitino SHALL register the catalog and the Iceberg REST Server SHALL be accessible at `http://<gravitino-host>:9001/iceberg/`

#### Scenario: Table creation through Iceberg REST API
- **WHEN** a client sends a `POST /iceberg/v1/namespaces/{namespace}/tables` request with a valid Iceberg schema
- **THEN** Gravitino Iceberg REST Server SHALL create the table metadata and place data files in `s3a://lakehouse/<namespace>/`

### Requirement: Iceberg table schema evolution
The system SHALL support Iceberg's native schema evolution operations (add column, rename column, drop column, reorder columns) through the REST Catalog API.

#### Scenario: Add column to existing Iceberg table
- **WHEN** a client sends a table update request with an `add-column` action
- **THEN** Gravitino SHALL apply the schema change to the Iceberg table metadata and the new column SHALL be visible in subsequent queries

### Requirement: Iceberg snapshot management
The system SHALL retain Iceberg table snapshots according to configured retention policy and support time-travel queries using snapshot IDs or timestamps.

#### Scenario: Time-travel query to previous snapshot
- **WHEN** a Spark query specifies `VERSION AS OF <snapshot-id>`
- **THEN** the query SHALL read data from the specified snapshot without modification to the current table state

#### Scenario: Snapshot expiry
- **WHEN** the configured snapshot retention period (default 7 days) expires
- **THEN** the system SHALL remove expired snapshot metadata and orphaned data files from MinIO

### Requirement: Namespace isolation in Iceberg catalog
Each tenant's Iceberg tables SHALL be organized under a dedicated namespace matching the tenant ID, ensuring metadata isolation.

#### Scenario: Namespace creation for new tenant
- **WHEN** a new tenant is onboarded
- **THEN** a corresponding Iceberg namespace SHALL be created under the `lakehouse` catalog with the tenant's storage prefix configured

#### Scenario: Cross-namespace access denied
- **WHEN** tenant-A attempts to list tables in tenant-B's namespace
- **THEN** Gravitino SHALL return HTTP 403 Forbidden
