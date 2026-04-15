## ADDED Requirements

### Requirement: Per-tenant Kubernetes Namespace
Each tenant SHALL have a dedicated Kubernetes Namespace named `tenant-<tenantId>`. All application Pods for a tenant SHALL run exclusively within their assigned Namespace.

#### Scenario: Tenant Pod runs in correct Namespace
- **WHEN** tenant `acme` submits a run
- **THEN** the resulting Pod SHALL be created in Namespace `tenant-acme` and not in any other tenant Namespace or the platform system Namespace

#### Scenario: Pod creation in wrong Namespace rejected
- **WHEN** a request attempts to create a Pod in `tenant-acme` Namespace using `tenant-b`'s ServiceAccount
- **THEN** the K8s API Server SHALL reject the request with `403 Forbidden`

### Requirement: NetworkPolicy default-deny with Catalog allowlist
Each tenant Namespace SHALL have a default-deny NetworkPolicy that blocks all ingress and egress traffic, with explicit allow rules only for the platform Catalog services (Gravitino, MinIO, ClickHouse endpoints) and DNS resolution.

#### Scenario: Cross-tenant network access blocked
- **WHEN** a Pod in `tenant-acme` Namespace attempts to connect to a Pod in `tenant-b` Namespace
- **THEN** the connection SHALL be dropped by the NetworkPolicy and no response SHALL be received

#### Scenario: Gravitino access allowed from tenant Pod
- **WHEN** a Pod in any tenant Namespace makes an HTTP request to the Gravitino Server ClusterIP on port 8090
- **THEN** the connection SHALL succeed (permitted by the Catalog allowlist rule)

#### Scenario: Arbitrary external internet access blocked
- **WHEN** a Pod in a tenant Namespace attempts to connect to an arbitrary external IP (not in the platform allowlist)
- **THEN** the egress NetworkPolicy SHALL drop the connection

### Requirement: Tenant-scoped ServiceAccount with minimal RBAC
Each tenant Namespace SHALL have a dedicated ServiceAccount (`platform-app-sa`) bound only to a RoleBinding (not ClusterRoleBinding) that permits: listing Pods in the tenant Namespace, writing logs, and accessing the tenant's own Secrets. No ClusterRole access SHALL be granted.

#### Scenario: ServiceAccount cannot list other Namespace resources
- **WHEN** a Pod running with `platform-app-sa` in `tenant-acme` attempts to list Pods in `tenant-b` Namespace
- **THEN** the K8s API Server SHALL return `403 Forbidden`

### Requirement: ResourceQuota per tenant Namespace
Each tenant Namespace SHALL be configured with a ResourceQuota defining limits on CPU, memory, and maximum Pod count. ResourceQuota values SHALL be configurable per tenant.

#### Scenario: Pod creation blocked when quota exceeded
- **WHEN** a tenant's Namespace is at CPU limit and a new Pod is scheduled
- **THEN** the Pod SHALL remain in `Pending` state and the scheduler SHALL report `Insufficient cpu` as the reason

#### Scenario: ResourceQuota updated by platform admin
- **WHEN** a platform administrator updates a tenant's CPU quota via the Platform API
- **THEN** the K8s ResourceQuota object SHALL be updated within 10 seconds
