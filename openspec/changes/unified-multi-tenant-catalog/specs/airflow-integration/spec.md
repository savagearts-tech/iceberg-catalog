## ADDED Requirements

### Requirement: Airflow Gravitino HTTP connection
The system SHALL configure an Airflow HTTP Connection (`gravitino_api`) pointing to the Gravitino REST API base URL, enabling DAGs to query catalog metadata without hardcoding system-specific connection strings.

#### Scenario: Airflow connection health check
- **WHEN** the Airflow connection `gravitino_api` is configured
- **THEN** a connection test SHALL verify `GET /api/version` returns HTTP 200

#### Scenario: DAG queries table metadata via Gravitino
- **WHEN** an Airflow DAG uses the `GravitinoMetadataHook` to call `GET /api/metalakes/{metalake}/catalogs/{catalog}/schemas/{ns}/tables/{table}`
- **THEN** the DAG SHALL receive table column definitions without directly connecting to Iceberg or MinIO

### Requirement: Gravitino metadata hook for Airflow
The system SHALL provide a custom `GravitinoMetadataHook` (extending `HttpHook`) implementing methods to list catalogs, namespaces, and tables from Gravitino.

#### Scenario: List tenant namespaces in DAG
- **WHEN** a DAG calls `GravitinoMetadataHook.list_namespaces(metalake, catalog)`
- **THEN** the hook SHALL return a list of namespace names the configured service account is authorized to view

#### Scenario: Hook propagates tenant identity via Bearer token
- **WHEN** a DAG is executed in tenant context
- **THEN** the hook SHALL include the tenant's Bearer token in all Gravitino API requests for proper ACL enforcement

### Requirement: Airflow pipeline metadata-driven triggering
The system SHALL support Airflow DAGs that discover Iceberg table partitions from Gravitino metadata to trigger downstream processing without hardcoded partition lists.

#### Scenario: DAG discovers new Iceberg snapshot via metadata
- **WHEN** a new Iceberg snapshot is committed to `lakehouse.<tenant>.raw_events`
- **THEN** the metadata-aware Airflow sensor SHALL detect the new snapshot and trigger the downstream processing task

#### Scenario: Missing table returns empty list not error
- **WHEN** a DAG queries metadata for a table that does not yet exist in Gravitino
- **THEN** the hook SHALL return an empty list and the DAG SHALL continue without raising an exception
