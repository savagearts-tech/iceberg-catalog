## Context

`managed-app-runtime` 已定义了环境变量注入契约（`PLATFORM_CATALOG_URL`、`PLATFORM_CATALOG_TOKEN` 等），SDK 是这些环境变量的**上层消费层**，不替换底层契约。

```
┌────────────────────────────────────────────────┐
│ 应用代码                                        │
│                                                │
│ 选项 A (推荐): SDK                              │
│   lakehouse = PlatformLakehouse.connect()      │
│   table = lakehouse.loadTable("events")        │
│   s3 = lakehouse.s3Client()                    │
│                                                │
│ 选项 B (兼容): 直读环境变量                       │
│   url = env["PLATFORM_CATALOG_URL"]            │
│   token = env["PLATFORM_CATALOG_TOKEN"]        │
│   catalog = IcebergRESTCatalog(url, token)     │
├────────────────────────────────────────────────┤
│ SDK 内部实现         │ 直连                      │
│   ↓                 │   ↓                      │
│ 环境变量 (始终注入)                               │
│ PLATFORM_CATALOG_URL / TOKEN / MINIO_*         │
└────────────────────────────────────────────────┘
```

SDK 需同时提供 Java 和 Python 两种实现，因平台核心计算负载以 Spark（Java/Scala）和 PySpark（Python）为主。

## Goals / Non-Goals

**Goals:**
- Java SDK：一行 `PlatformLakehouse.connect()` 初始化，返回配置好的 Iceberg Catalog 和 S3 客户端
- Python SDK：一行 `lakehouse.connect()` 初始化，返回 `pyiceberg` Catalog 和 `boto3` S3 resource
- 自动从环境变量读取平台上下文，开发者无需关心变量名称和解析逻辑
- 内置 JWT Token 有效期检查和自动刷新（通过 SDK 内部守护线程/协程后台刷新）
- 强制 namespace 限定：所有 Iceberg 操作自动前置 `PLATFORM_TENANT_NAMESPACE`，禁止跨 namespace
- 提供 `SparkSession` / PySpark Session 预配置工厂方法，免去 `spark.sql.catalog.*` 手动配置
- 提供 `TestLakehouse` 测试模式：开发者本地测试时 mock 平台上下文

**Non-Goals:**
- 不实现 Go/Rust/Node.js SDK（这些语言使用环境变量直连方式）
- 不实现 SDK 级别的权限校验（依赖 Gravitino ACL 作为权限控制层）
- 不包含数据操作 API（SDK 不封装 Spark DataFrame 操作，只封装连接初始化）
- 不包含 CLI 工具（SDK 仅为库/包）

## Decisions

### D1: SDK 内部架构 — Thin Wrapper 模式

**决策**: SDK 是围绕 Iceberg REST Catalog Client 和 S3 Client 的**薄包装**，不引入新的 API 层或远程调用。核心逻辑仅为：读取环境变量 → 构建标准客户端 → 注入租户约束（namespace 限定、Token 刷新）。

**理由**: 避免 SDK 成为不可控的"平台肥客户端"。Iceberg / S3 已有成熟生态，SDK 只做胶水，不做新轮子。

**替代方案考虑**:
- 厚 SDK（封装全部数据操作，如 `lakehouse.readParquet(path)`）：维护成本高，难跟进 Iceberg/Spark 版本演进
- gRPC 远程调用模式（SDK 做 server 端代理）：引入网络开销和新的 SPOF

### D2: Namespace 强制限定 — 代理模式

**决策**: SDK 返回的 Iceberg Catalog 对象是一个代理（Proxy / Wrapper），会拦截所有 `loadTable`、`listTables`、`createTable` 调用，自动将未限定 namespace 的请求强制加上 `PLATFORM_TENANT_NAMESPACE` 前缀。如果用户显式传入其他 namespace，SDK 拒绝并抛出 `CrossNamespaceException`。

```java
// SDK 代理行为：
lakehouse.loadTable("events")
  → 内部调用 catalog.loadTable(PLATFORM_TENANT_NAMESPACE, "events")

lakehouse.loadTable(Namespace.of("other-tenant"), "events")
  → 抛出 CrossNamespaceException("Cannot access namespace: other-tenant")
```

**理由**: 双重防线——即使 Gravitino ACL 配置有误，SDK 层也能阻止跨 namespace 操作。深度防御原则。

### D3: Token 刷新策略 — SDK 内部守护线程

**决策**: SDK 在 `connect()` 时启动一个低优先级守护线程，定期检查 JWT Token 剩余有效期。当 Token 有效期 < 5 分钟时，从环境变量重新读取（由 Sidecar 刷新后写入共享 Volume），并更新 Catalog 客户端的 Authorization header。

**理由**: 应用无需关心 Token 续期。环境变量重新读取比 SDK 直接调用 IdP 更安全（不需要在 SDK 中嵌入 IdP 凭证）。

### D4: Python SDK 发布方式 — Internal PyPI + pip install

**决策**: Python SDK 以 `platform-lakehouse-sdk` 包名发布到内部 PyPI 仓库，应用通过 `pip install platform-lakehouse-sdk` 引入。不支持 conda。

**理由**: 与平台 Docker 镜像构建流程对齐（pip 为标准包管理工具）。

### D5: 测试辅助 — TestLakehouse

**决策**: SDK 提供 `TestLakehouse` 辅助类（Java）/ `test_lakehouse` fixture（Python pytest），允许开发者在本地测试时注入 mock 连接参数，无需真实的平台环境。

```java
// Java 测试模式
var lakehouse = TestLakehouse.builder()
    .catalogUrl("http://localhost:9001/iceberg/v1")
    .namespace("test-ns")
    .build();
```

```python
# Python pytest fixture
@pytest.fixture
def lakehouse():
    return test_lakehouse(catalog_url="http://localhost:9001/iceberg/v1", namespace="test-ns")
```

## Risks / Trade-offs

| 风险 | 严重度 | 缓解措施 |
|------|--------|----------|
| SDK 版本与平台版本不匹配导致运行时错误 | 🟠 中 | 通过 Maven BOM / pip 版本约束控制；SDK 启动时校验 `PLATFORM_SDK_VERSION_COMPAT` 环境变量 |
| Java SDK 依赖冲突（Iceberg / S3 SDK 与应用依赖版本） | 🟠 中 | SDK 使用 `shade` 或 `relocation` 隔离依赖 namespace；提供 dependency BOM |
| Python SDK 的 pyiceberg 版本兼容性 | 🟡 低 | 锁定 pyiceberg 最小版本 + 在 CI 中测试多版本矩阵 |
| 开发者绕过 SDK 直读环境变量（失去 namespace 限定保护） | 🟠 中 | 在应用提交 API 添加 SDK-REQUIRED 标记（可选强制策略）；文档明确推荐 SDK |
| Token 刷新守护线程在应用内部不可控 | 🟡 低 | 守护线程为 daemon 模式，不阻止 JVM/Python 退出；超时机制确保重读失败不卡住 |

## Migration Plan

1. **Phase 1 - Java SDK Core**: 实现 `PlatformLakehouse.connect()`、Namespace 代理、Token 刷新线程
2. **Phase 2 - SparkSession 工厂**: 实现 `lakehouse.sparkSession()` 预配置工厂
3. **Phase 3 - Python SDK**: 实现 `lakehouse.connect()` + PySpark 集成
4. **Phase 4 - 测试辅助与文档**: TestLakehouse / test_lakehouse + 完整 API 文档 + 使用指南

**兼容策略**: SDK 为纯新增模块，不修改任何现有契约。不使用 SDK 的应用不受影响。

## Open Questions

| # | 问题 | 决策前提 | 优先级 |
|---|------|----------|--------|
| OQ1 | **SDK 是否应标记为 REQUIRED**？即是否在 App Runtime 层面要求所有应用必须使用 SDK？还是保持可选？ | Phase 1 发布前 | 🟠 高 |
| OQ2 | **Scala API**：是否额外提供 Scala-idiomatic API（`implicit` 扩展）？还是 Java API 对 Scala 足够友好？ | Phase 2 前 | 🟡 中 |
