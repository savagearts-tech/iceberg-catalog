## 1. Java SDK 项目结构

- [ ] 1.1 创建 Maven 模块 `platform-lakehouse-sdk-java`，配置 parent POM 和 shade 插件
- [ ] 1.2 创建 Maven BOM 模块 `platform-lakehouse-bom`，管理 Iceberg / Spark / AWS SDK 版本对齐
- [ ] 1.3 配置 Maven shade 插件 relocation 规则，隔离 Iceberg REST Client 和 AWS SDK 依赖

## 2. Java SDK 核心实现

- [ ] 2.1 实现 `PlatformContextReader`：从环境变量读取平台上下文，缺失时抛 `PlatformContextMissingException`
- [ ] 2.2 实现 `PlatformLakehouse.connect()`：调用 `PlatformContextReader` → 构建 Iceberg REST Catalog → 构建 S3 Client → 启动 Token 刷新线程
- [ ] 2.3 实现 `NamespaceScopedCatalog`（Catalog 代理）：拦截所有 Table 操作，强制限定到租户 namespace；跨 namespace 操作抛 `CrossNamespaceException`
- [ ] 2.4 实现 `TokenRefreshDaemon`：守护线程，每 60 秒检查 JWT Token 剩余有效期，< 5 分钟时重读环境变量/共享文件刷新
- [ ] 2.5 实现 `Lakehouse.s3Client()`：预配置 MinIO endpoint + path-style access + 租户 AccessKey
- [ ] 2.6 实现 `Lakehouse.close()` / `AutoCloseable`：停止 TokenRefreshDaemon，释放 HTTP 连接
- [ ] 2.7 实现 `SparkSessionFactory`：`lakehouse.sparkSession()` 返回预配置的 SparkSession（Catalog URI、Authorization header、S3A credentials），支持自定义 configurator

## 3. Java SDK 测试

- [ ] 3.1 实现 `TestLakehouse.builder()`：接受 mock catalogUrl / namespace / credentials，不读取真实环境变量
- [ ] 3.2 编写 `PlatformContextReaderTest`：有变量/缺失变量的场景测试
- [ ] 3.3 编写 `NamespaceScopedCatalogTest`：隐式 namespace 解析、跨 namespace 拒绝、listTables 范围限定
- [ ] 3.4 编写 `TokenRefreshDaemonTest`：模拟 Token 过期 → 重读 → 刷新成功 / 刷新失败 WARNING
- [ ] 3.5 编写集成测试（Testcontainers）：`connect()` → `loadTable` → 读取 Iceberg 表 → 验证 S3 路径正确

## 4. Python SDK 项目结构

- [ ] 4.1 创建 Python 包 `platform-lakehouse-sdk`，配置 `pyproject.toml`（依赖 pyiceberg、boto3）
- [ ] 4.2 配置内部 PyPI 仓库发布（twine / CI pipeline）

## 5. Python SDK 核心实现

- [ ] 5.1 实现 `lakehouse.connect()`：读取环境变量 → 构建 pyiceberg RestCatalog + boto3 S3 client → 启动 Token 刷新线程
- [ ] 5.2 实现 Namespace 强制限定（代理 `load_table` / `list_tables` / `create_table`），跨 namespace 操作抛 `CrossNamespaceError`
- [ ] 5.3 实现 Token 刷新协程/线程（与 Java 实现逻辑一致）
- [ ] 5.4 实现 `Lakehouse.spark_session()`：预配置 PySpark Session，支持 `extra_config` 参数
- [ ] 5.5 实现 Python context manager `__enter__` / `__exit__`

## 6. Python SDK 测试

- [ ] 6.1 实现 `test_lakehouse` pytest fixture：注入 mock 连接参数
- [ ] 6.2 编写 `test_connect.py`：自动发现、缺失变量、namespace 限定
- [ ] 6.3 编写 `test_token_refresh.py`：模拟 Token 过期与刷新
- [ ] 6.4 编写集成测试（testcontainers-python）：`connect()` → Iceberg 表操作 → 验证

## 7. 文档与发布

- [ ] 7.1 编写 `docs/sdk-quickstart.md`：Java 和 Python 快速上手指南（含代码示例）
- [ ] 7.2 编写 `docs/sdk-api-reference.md`：完整 API 参考（Java + Python）
- [ ] 7.3 编写 `docs/sdk-testing-guide.md`：TestLakehouse / test_lakehouse 使用说明
- [ ] 7.4 在 `managed-app-runtime` 应用提交文档中推荐使用 SDK，标注环境变量直连为兼容模式
