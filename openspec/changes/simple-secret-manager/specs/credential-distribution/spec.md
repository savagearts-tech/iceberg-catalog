## ADDED Requirements

### Requirement: One-time token issuance
The system SHALL generate a cryptographically secure one-time token (OTT) linked to a `secretPath`. The token SHALL be a SHA-256 hex digest of 32 CSPRNG bytes. The token SHALL have a configurable TTL (default 15 minutes) recorded as `expires_at`. A token SHALL be invalidated immediately upon first consumption.

#### Scenario: Issue a one-time token
- **WHEN** `SecretManagerClient.issueOneTimeToken("tenants/acme/minio", Duration.ofMinutes(15))` is called
- **THEN** the system SHALL store the token in `one_time_tokens` with `expires_at = now() + 15 minutes` and return the token string to the caller

#### Scenario: Token TTL is configurable per call
- **WHEN** `issueOneTimeToken` is called with `Duration.ofMinutes(5)`
- **THEN** the stored `expires_at` SHALL be `now() + 5 minutes`

### Requirement: One-time token consumption
The system SHALL atomically validate and consume a one-time token, returning the decrypted secret values. Atomicity SHALL be enforced using a database transaction with `SELECT FOR UPDATE`. Only one concurrent caller SHALL succeed in consuming a given token.

#### Scenario: Consume a valid token
- **WHEN** `SecretManagerClient.consumeToken("<valid-token>")` is called within the TTL window and the token has not been consumed
- **THEN** the system SHALL set `used_at = now()` on the token row and return the decrypted secret values for the associated `secretPath`

#### Scenario: Consume an already-used token fails
- **WHEN** `consumeToken` is called with a token where `used_at IS NOT NULL`
- **THEN** the system SHALL throw `InvalidTokenException` with reason `TOKEN_ALREADY_USED`

#### Scenario: Consume an expired token fails
- **WHEN** `consumeToken` is called after the token's `expires_at` has passed
- **THEN** the system SHALL throw `InvalidTokenException` with reason `TOKEN_EXPIRED`

#### Scenario: Concurrent consumption — only one succeeds
- **WHEN** two concurrent requests call `consumeToken` with the same token simultaneously
- **THEN** exactly one request SHALL succeed and receive the secret values; the other SHALL receive `TOKEN_ALREADY_USED`

### Requirement: Expired token cleanup
The system SHALL periodically purge expired and used tokens from `one_time_tokens` to prevent unbounded table growth. The cleanup job SHALL run at a configurable interval (default: every 1 hour).

#### Scenario: Expired tokens removed by cleanup job
- **WHEN** the cleanup job runs
- **THEN** all rows where `expires_at < now() - 1 hour` OR `used_at IS NOT NULL AND used_at < now() - 24 hours` SHALL be deleted

### Requirement: Token issuance limited to existing active secrets
The system SHALL reject token issuance for a `secretPath` that does not exist or has been revoked.

#### Scenario: Token issuance for revoked secret rejected
- **WHEN** `issueOneTimeToken` is called for a path where `revoked_at IS NOT NULL`
- **THEN** the system SHALL throw `SecretRevokedException` with reason `SECRET_REVOKED`
