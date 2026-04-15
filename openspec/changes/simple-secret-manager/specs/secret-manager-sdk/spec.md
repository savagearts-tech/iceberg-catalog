## ADDED Requirements

### Requirement: SecretManagerClient interface definition
The system SHALL define a `SecretManagerClient` Java interface in a shared platform library module (`platform-secret-manager-api`) exposing the following methods: `write`, `read`, `revoke`, `issueOneTimeToken`, `consumeToken`. All implementations SHALL conform to this interface.

#### Scenario: Interface is stable across implementations
- **WHEN** a new `VaultSecretManagerClient` implementation is created in the future
- **THEN** it SHALL implement `SecretManagerClient` without requiring any changes to callers (`TenantProvisioningService`, `InitContainerCredentialFetcher`)

### Requirement: SimpleSecretManagerClient implementation
The system SHALL provide `SimpleSecretManagerClient` as the default implementation of `SecretManagerClient`, backed by the platform PostgreSQL `secret_manager` schema. It SHALL be Spring-managed and conditionally activated by `platform.secret-manager.type=simple` (default value).

#### Scenario: SimpleSecretManagerClient activated by default
- **WHEN** no `platform.secret-manager.type` property is configured
- **THEN** Spring SHALL auto-configure `SimpleSecretManagerClient` as the active `SecretManagerClient` bean

#### Scenario: Alternative implementation substituted via configuration
- **WHEN** `platform.secret-manager.type=vault` is configured
- **THEN** `SimpleSecretManagerClient` SHALL NOT be instantiated; a `VaultSecretManagerClient` bean (if present on classpath) SHALL be used instead

### Requirement: SDK used by Tenant Provisioning API
The `TenantProvisioningService` in `unified-multi-tenant-catalog` SHALL use `SecretManagerClient.write` to store the tenant's MinIO AccessKey and SecretKey after a successful Tenant Onboarding operation, and `issueOneTimeToken` to produce the `credentialRef` returned to the caller.

#### Scenario: Tenant provisioning writes credentials to Secret Manager
- **WHEN** `POST /api/tenants` successfully creates a tenant namespace and MinIO service account
- **THEN** `SecretManagerClient.write("tenants/<tenantId>/minio", {"accessKey": "...", "secretKey": "..."})` SHALL be called, and the response SHALL contain `credentialRef` (the OTT token) instead of plaintext credentials

#### Scenario: Secret Manager write failure rolls back tenant creation
- **WHEN** `SecretManagerClient.write` throws an exception during tenant provisioning
- **THEN** the tenant namespace creation and MinIO service account creation SHALL be rolled back, and the API SHALL return HTTP 500 with reason `SECRET_MANAGER_UNAVAILABLE`

### Requirement: SDK used by App Runtime Init Container
The `managed-app-runtime` Init Container SHALL use `SecretManagerClient.consumeToken` to fetch tenant MinIO credentials at Pod startup time, using the OTT provided as a Pod environment variable `PLATFORM_CREDENTIAL_TOKEN`.

#### Scenario: Init Container fetches credentials via OTT
- **WHEN** the Init Container starts with `PLATFORM_CREDENTIAL_TOKEN=<valid-ott>`
- **THEN** `consumeToken` SHALL return the tenant's `accessKey` and `secretKey`, which SHALL be written to the shared volume for the application container to read

#### Scenario: Init Container fails on expired token
- **WHEN** the Init Container starts with an expired or used `PLATFORM_CREDENTIAL_TOKEN`
- **THEN** Init Container SHALL exit with a non-zero exit code and the error reason `TOKEN_EXPIRED` or `TOKEN_ALREADY_USED` SHALL appear in the Init Container logs

### Requirement: SDK dependency isolation
The `platform-secret-manager-api` module SHALL have no runtime dependency on `SimpleSecretManagerClient` or any specific database driver. Consumers of the interface SHALL depend only on `platform-secret-manager-api`, not on the implementation module.

#### Scenario: API module has no PostgreSQL dependency
- **WHEN** the Maven dependency tree of `platform-secret-manager-api` is inspected
- **THEN** it SHALL NOT include `postgresql` JDBC driver or `spring-data-jpa` as compile or runtime scope dependencies
