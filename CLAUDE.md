# Project Context

此项目用于生产环境大规模 ES 迁移至 Doris/SelectDB。将 Elasticsearch index mapping 快速转换为 Apache Doris 建表 DDL。

**目标 Doris 版本：2.1+**

关键约束：
- 充分利用 Doris 2.1+ 的 `VARIANT` 类型特性（半结构化数据、动态列、sub-column 索引）
- `nested`、`flattened`、`object(enabled:false)` 等无法精确映射的类型优先考虑 `VARIANT` 而非强行展平

## 工作规范

- **编写代码前**：充分了解代码的调用上下文（调用方、被调用方、数据流），不在只读过局部代码的情况下动手修改
- **设计优先**：使用合适的设计模式，写出易于维护、易于扩展的代码，避免过度工程化
- **测试要求**：新增或修改的代码必须有对应测试；以生产标准要求工程质量，包括边界条件、错误路径、负面测试用例
- **日志规范**：使用标准 logging 模块，区分 DEBUG/INFO/WARNING/ERROR 级别，不使用 `print` 或裸 `sys.stderr.write` 输出；日志内容应可读、可定位问题
