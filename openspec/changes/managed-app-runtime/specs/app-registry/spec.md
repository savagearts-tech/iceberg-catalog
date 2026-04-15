## ADDED Requirements

### Requirement: Application registration API
The system SHALL expose `POST /api/apps` to register an application with its type, artifact reference (Docker image URI or Spark JAR path), and resource requirements. Each application SHALL belong to exactly one tenant and be assigned a unique `appId`.

#### Scenario: Register a Docker-based application
- **WHEN** a tenant developer calls `POST /api/apps` with `{"type": "docker", "image": "registry.io/myapp:v1.2", "cpu": "2", "memory": "4Gi"}`
- **THEN** the system SHALL store the application metadata and return `{ appId, version: "v1", status: "registered" }`

#### Scenario: Register a Spark application
- **WHEN** a tenant developer registers an app with `{"type": "spark", "mainClass": "com.example.EtlJob", "jarPath": "s3a://lakehouse/<tenant>/jars/etl.jar"}`
- **THEN** the system SHALL validate that the JAR path is within the tenant's MinIO namespace and store the registration

#### Scenario: Registration rejected for cross-tenant JAR path
- **WHEN** a tenant-A developer attempts to register an app referencing `s3a://lakehouse/tenant-B/jars/etl.jar`
- **THEN** the system SHALL return HTTP 403 Forbidden with reason `CROSS_TENANT_RESOURCE`

### Requirement: Application versioning
The system SHALL support multiple versions of the same application, identified by `appId` + `version`. Submitting a new registration for an existing `appId` SHALL create a new version rather than overwriting.

#### Scenario: New version created on duplicate appId
- **WHEN** a developer registers an app with the same `appId` but a new `image` tag
- **THEN** the system SHALL create `version: "v2"` and both versions SHALL remain accessible

#### Scenario: List application versions
- **WHEN** a developer calls `GET /api/apps/{appId}/versions`
- **THEN** the system SHALL return all versions with their artifact references and registration timestamps

### Requirement: Application deletion
The system SHALL support deleting an application version, provided no running instances reference it.

#### Scenario: Delete a version with no active runs
- **WHEN** a developer calls `DELETE /api/apps/{appId}/versions/{version}` and no running instances use that version
- **THEN** the version SHALL be removed from the registry

#### Scenario: Delete rejected for active version
- **WHEN** a developer attempts to delete a version that has active running instances
- **THEN** the system SHALL return HTTP 409 Conflict with reason `VERSION_IN_USE`
