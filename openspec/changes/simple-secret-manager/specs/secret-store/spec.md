## ADDED Requirements

### Requirement: Secret write with AES-256-GCM encryption
The system SHALL encrypt secret values using AES-256-GCM with a randomly generated 96-bit IV before persisting them to PostgreSQL. The Master Key SHALL be sourced exclusively from the environment variable `PLATFORM_SECRET_MASTER_KEY` and SHALL NOT be stored in the database.

#### Scenario: Write a new secret
- **WHEN** a caller invokes `SecretManagerClient.write("tenants/acme/minio", {"accessKey": "AKID...", "secretKey": "sk..."})`
- **THEN** the system SHALL encrypt the values and persist `(secret_path, ciphertext, iv)` to `secret_manager.secrets`, returning without exposing plaintext

#### Scenario: Overwrite an existing secret path
- **WHEN** `write` is called with an existing `secretPath`
- **THEN** the existing row SHALL be updated with new ciphertext and a fresh random IV, and `updated_at` SHALL reflect the current timestamp

#### Scenario: Write rejected when Master Key is absent
- **WHEN** `write` is called but `PLATFORM_SECRET_MASTER_KEY` environment variable is not set
- **THEN** the system SHALL throw `SecretManagerConfigException` with message `MASTER_KEY_NOT_CONFIGURED` and SHALL NOT persist any data

### Requirement: Secret read with decryption
The system SHALL decrypt a stored secret by `secretPath` and return the plaintext key-value map. A read on a revoked secret SHALL fail.

#### Scenario: Read an active secret
- **WHEN** `SecretManagerClient.read("tenants/acme/minio")` is called for an existing, non-revoked path
- **THEN** the system SHALL return the decrypted `{"accessKey": "...", "secretKey": "..."}` map

#### Scenario: Read a revoked secret fails
- **WHEN** `read` is called for a path where `revoked_at IS NOT NULL`
- **THEN** the system SHALL throw `SecretRevokedException` with reason `SECRET_REVOKED`

#### Scenario: Read a non-existent path fails
- **WHEN** `read` is called for a `secretPath` that has never been written
- **THEN** the system SHALL throw `SecretNotFoundException` with reason `SECRET_NOT_FOUND`

### Requirement: Secret revocation
The system SHALL support marking a secret as revoked by `secretPath`. Revocation sets `revoked_at` to the current timestamp and does not physically delete the row (for audit trail purposes). Revoked secrets SHALL NOT be readable.

#### Scenario: Revoke an active secret
- **WHEN** `SecretManagerClient.revoke("tenants/acme/minio")` is called
- **THEN** `revoked_at` SHALL be set to the current timestamp, and subsequent `read` calls SHALL throw `SecretRevokedException`

#### Scenario: Revoke an already-revoked secret is idempotent
- **WHEN** `revoke` is called on a path that is already revoked
- **THEN** the call SHALL succeed without error and `revoked_at` SHALL remain unchanged

### Requirement: Encrypted value integrity verification
The system SHALL use AES-256-GCM authenticated encryption. Any tampered ciphertext SHALL cause decryption to fail with an integrity error rather than returning corrupted data.

#### Scenario: Tampered ciphertext detected
- **WHEN** the ciphertext stored in the database has been manually modified
- **THEN** `read` SHALL throw `SecretIntegrityException` with reason `DECRYPTION_FAILED`
