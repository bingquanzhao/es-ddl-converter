# es-ddl-converter

将 Elasticsearch index mapping 转换为 Apache Doris `CREATE TABLE` DDL。

**适用场景：** ES → Doris 数据迁移时，自动完成建表语句生成，无需手写 SQL。
**目标版本：** Doris 2.1+，充分利用 VARIANT 类型和倒排索引特性。

---

## 目录

- [安装](#安装)
- [快速上手](#快速上手)
- [使用方式](#使用方式)
  - [单文件转换](#单文件转换)
  - [批量转换](#批量转换)
  - [直接执行到 Doris](#直接执行到-doris)
- [配置参考](#配置参考)
  - [任务配置文件 -f](#任务配置文件--f)
  - [表设置 --table-properties](#表设置---table-properties)
  - [优先级规则](#优先级规则)
- [转换示例](#转换示例)
- [参考手册](#参考手册)
  - [类型映射](#类型映射)
  - [自动索引策略](#自动索引策略)
  - [自动建表决策](#自动建表决策)
  - [CLI 参数](#cli-参数)
  - [退出码](#退出码)
  - [支持的 Mapping 格式](#支持的-mapping-格式)
- [开发](#开发)

---

## 安装

**PyPI 稳定版：**

```bash
pip install es-ddl-converter
```

**源码安装（获取最新功能）：**

```bash
pip install git+https://github.com/bingquanzhao/es-ddl-converter.git
```

**本地开发安装：**

```bash
git clone https://github.com/bingquanzhao/es-ddl-converter.git
cd es-ddl-converter
pip install -e .
```

**环境要求：** Python 3.8+

---

## 快速上手

```bash
# 从 ES 集群导出 mapping
curl -s http://localhost:9200/my_index/_mapping > mapping.json

# 转换并输出到终端
es-ddl-converter -i mapping.json

# 转换并写入文件
es-ddl-converter -i mapping.json -o create_table.sql
```

---

## 使用方式

### 单文件转换

适用于已有 mapping JSON 文件、快速验证效果的场景。

```bash
# 基本用法
es-ddl-converter -i mapping.json

# 指定输出文件和表名
es-ddl-converter -i mapping.json -o out.sql --table-name my_table

# UNIQUE KEY 模型，包含 _id 列
es-ddl-converter -i mapping.json --model unique --include-id

# 只看告警，不生成 DDL
es-ddl-converter -i mapping.json --warnings-only

# 指定 tags 字段为数组类型（ES mapping 无法区分单值和数组）
es-ddl-converter -i mapping.json --table-properties config.yaml

# 直接在命令行传入表参数（无需创建文件）
es-ddl-converter -i mapping.json --table-properties "replication_num: 1"

# 多个 --table-properties 合并生效，后面的覆盖前面的同名 key
es-ddl-converter -i mapping.json \
  --table-properties "replication_num: 1" \
  --table-properties "compression: LZ4"
```

`config.yaml` 示例：

```yaml
array_fields:
  - tags
  - categories
ip_type: ipv6
compression: ZSTD   # NO_COMPRESSION / LZ4 / LZ4F / ZLIB / ZSTD / SNAPPY
```

### 批量转换

适用于迁移整个 ES 集群或目录下多个 mapping 文件的场景。推荐用任务配置文件管理所有参数：

```yaml
# job.yaml
source:
  url: http://localhost:9200
  user: elastic
  password: secret
  index: "logs-*"

exclude: ".*\\.test$"     # 正则，跳过匹配的索引名

output:
  dir: ./ddl_output/
  table_prefix: ""

table:
  model: duplicate
  replication_num: 3
  ip_type: ipv6
  array_fields: []

fail_fast: false
```

```bash
es-ddl-converter -f job.yaml
```

输出目录结构：

```
ddl_output/
├── logs-2024.01.sql
├── logs-2024.02.sql
├── users.sql
└── _batch_report.txt
```

CLI 参数可临时覆盖配置文件中的值：

```bash
es-ddl-converter -f job.yaml --model unique --table-prefix doris_
```

从本地目录批量转换：

```yaml
source:
  dir: ./mappings/    # 目录下所有 *.json 文件
```

### 直接执行到 Doris

在 `job.yaml` 中加入 `doris` 配置段，并显式设置 `execute: true`：

```yaml
doris:
  execute: true         # 必须显式开启，防止误操作
  host: 127.0.0.1
  port: 9030
  user: root
  password: ""
  database: my_db       # 目标数据库，不存在会自动创建
```

```bash
es-ddl-converter -f job.yaml
```

工具会先生成 DDL 文件，再连接 Doris 执行建表。数据库名称只允许字母、数字、下划线和连字符，其他字符会拒绝执行。

**注意：** 执行前会自动查询 Doris 的存活 BE 节点数。若 `replication_num` 超过 BE 数量，工具会立即报错退出并给出修正命令，而不是等到建表时才失败。单节点 Doris 环境需设置 `replication_num: 1`。

批量执行时工具复用单一持久连接，连接断开后自动重连一次。

也可通过 CLI 参数在批量模式下触发执行：

```bash
es-ddl-converter -f job.yaml --execute \
  --doris-host 127.0.0.1 --doris-db target_db

# 单节点 Doris 环境
es-ddl-converter -f job.yaml --execute \
  --doris-host 127.0.0.1 --doris-db target_db \
  --table-properties "replication_num: 1"
```

---

## 配置参考

### 任务配置文件 `-f`

完整字段说明：

```yaml
# ── 数据源（三选一）──────────────────────────────────────────
source:
  # 连接 ES 集群
  url: http://localhost:9200
  user: elastic
  password: secret
  index: "logs-*"         # 索引名模式，支持通配符，默认 *
  verify_ssl: true        # 禁用时仅输出警告，不建议在生产环境关闭

  # 本地目录（与 url 二选一）
  # dir: ./mappings/

  # 单个文件（与 url 二选一）
  # file: mapping.json

# ── 过滤 ─────────────────────────────────────────────────────
exclude: ""               # 正则表达式，跳过匹配索引名的索引

# ── 输出 ─────────────────────────────────────────────────────
output:
  dir: ./ddl_output/      # 批量模式输出目录
  # file: out.sql         # 单文件模式输出文件
  table_prefix: ""        # 所有表名添加此前缀
  # table_name: my_table  # 覆盖表名（仅单文件模式）

# ── Doris 执行（可选）────────────────────────────────────────
doris:
  execute: false          # 必须显式设为 true 才会连接 Doris 执行
  host: 127.0.0.1
  port: 9030
  user: root
  password: ""
  database: my_db

# ── 表结构设置 ───────────────────────────────────────────────
table:
  model: duplicate          # duplicate（默认）或 unique
  include_id: false         # 是否添加 _id VARCHAR(128) 列
  replication_num: 3
  compression: ZSTD         # NO_COMPRESSION / LZ4 / LZ4F / ZLIB / ZSTD / SNAPPY
  ip_type: ipv6             # ip 字段映射为 IPv4 或 IPv6
  array_fields: []          # 需手动标注为数组的字段路径列表
  # key_columns: []         # 不指定则自动推断
  # partition_field: "@timestamp"
  # bucket_strategy: random # random 或 hash(field_name)

# ── 运行行为 ─────────────────────────────────────────────────
fail_fast: false            # 遇到第一个错误即停止整批任务
warnings_only: false        # 仅校验输出告警，不写文件也不执行
```

### 表设置 `--table-properties`

轻量配置，仅包含表结构相关设置，适合在多个任务之间复用。支持 YAML 文件路径和 inline YAML 字符串两种形式，可多次指定，后面的覆盖前面的同名 key：

```yaml
# props.yaml
array_fields:
  - tags
  - categories
model: duplicate
key_columns:
  - "@timestamp"
  - level
partition_field: "@timestamp"
bucket_strategy: random
replication_num: 3
compression: ZSTD
ip_type: ipv6
include_id: false
```

```bash
# 文件形式
es-ddl-converter batch ... --table-properties props.yaml

# inline 形式（无需创建文件）
es-ddl-converter batch ... --table-properties "replication_num: 1"

# 多次指定，合并生效
es-ddl-converter batch ... \
  --table-properties props.yaml \
  --table-properties "replication_num: 1"  # 覆盖文件中的 replication_num
```

### 优先级规则

多种配置同时存在时，优先级从高到低：

```
CLI 参数  >  --table-properties（多个时后者覆盖前者）  >  -f job.yaml  >  默认值
```

`-f` 和 `--table-properties` 同时使用时，`--table-properties` 的内容覆盖 `-f` 中 `table:` 部分的设置。

---

## 转换示例

**输入 mapping（`mapping.json`）：**

```json
{
  "my_logs": {
    "mappings": {
      "dynamic": "true",
      "properties": {
        "@timestamp": { "type": "date", "format": "epoch_millis" },
        "level":      { "type": "keyword" },
        "service":    { "type": "keyword", "ignore_above": 128 },
        "trace_id":   { "type": "keyword" },
        "message": {
          "type": "text",
          "analyzer": "standard",
          "fields": { "keyword": { "type": "keyword", "ignore_above": 2048 } }
        },
        "host_ip":       { "type": "ip" },
        "duration":      { "type": "float" },
        "response_code": { "type": "short" },
        "tags":          { "type": "keyword" },
        "user": {
          "type": "object",
          "properties": {
            "id":   { "type": "long" },
            "name": { "type": "keyword" }
          }
        },
        "location": { "type": "geo_point" },
        "metadata": { "type": "object", "enabled": false },
        "time_range": { "type": "date_range" }
      }
    }
  }
}
```

**配置（`config.yaml`）：**

```yaml
array_fields:
  - tags
```

**运行：**

```bash
es-ddl-converter -i mapping.json --table-properties config.yaml
```

**输出（stdout）：**

```sql
CREATE TABLE IF NOT EXISTS `my_logs` (
    `@timestamp`     DATETIME(3)         NOT NULL    COMMENT 'date, format=epoch_millis',
    `level`          VARCHAR(256)        NOT NULL    COMMENT 'keyword',
    `service`        VARCHAR(128)        NOT NULL    COMMENT 'keyword, ignore_above=128',

    `trace_id`       VARCHAR(256)        NULL        COMMENT 'keyword',
    `message`        TEXT                NULL        COMMENT 'text, analyzer=standard',
    `host_ip`        IPv6                NULL        COMMENT 'ip',
    `duration`       FLOAT               NULL        COMMENT 'float',
    `response_code`  SMALLINT            NULL        COMMENT 'short',
    `tags`           ARRAY<VARCHAR(256)> NULL        COMMENT 'keyword, multi-value',
    `user`           VARIANT             NULL        COMMENT 'object',
    `location`       VARIANT             NULL        COMMENT 'geo_point',
    `metadata`       VARIANT             NULL        COMMENT 'object, enabled=false',
    `time_range_gte` DATETIME(3)         NULL        COMMENT 'date_range lower bound',
    `time_range_lte` DATETIME(3)         NULL        COMMENT 'date_range upper bound',

    INDEX idx_level(`level`) USING INVERTED COMMENT 'keyword exact match',
    INDEX idx_service(`service`) USING INVERTED COMMENT 'keyword exact match',
    INDEX idx_trace_id(`trace_id`) USING INVERTED COMMENT 'keyword exact match',
    INDEX idx_message(`message`) USING INVERTED PROPERTIES("parser"="unicode", "support_phrase"="true") COMMENT 'full-text search',
    INDEX idx_tags(`tags`) USING INVERTED COMMENT 'array contains',
    INDEX idx_user(`user`) USING INVERTED COMMENT 'accelerate sub-column filtering'
)
DUPLICATE KEY(`@timestamp`)
AUTO PARTITION BY RANGE(date_trunc(`@timestamp`, 'day')) ()
DISTRIBUTED BY RANDOM BUCKETS AUTO
PROPERTIES (
    "replication_num" = "3",
    "compression" = "ZSTD",
    "inverted_index_storage_format" = "V3",
    "compaction_policy" = "time_series"
);
```

**告警输出（stderr）：**

```
WARNING: field='location': geo_point mapped to VARIANT. Original {lat, lon} structure is preserved. Doris has no native geospatial query support.
```

**工具自动完成了：**

- 30+ 种 ES 字段类型映射到 Doris 类型
- `object` 嵌套结构默认映射为 `VARIANT`，可通过 `flatten_fields` 配置项按需展平
- `text` + `.keyword` multi-fields 合并为单列，不重复建列
- keyword / text / VARIANT 列自动生成倒排索引（含 analyzer → parser 映射）
- Key 列、分区、分桶策略自动推断

---

## 参考手册

### 类型映射

**直接映射：**

| ES 类型 | Doris 类型 |
|---------|-----------|
| `byte` | `TINYINT` |
| `short` | `SMALLINT` |
| `integer` | `INT` |
| `long` | `BIGINT` |
| `unsigned_long` | `LARGEINT` |
| `float` / `half_float` | `FLOAT` |
| `double` | `DOUBLE` |
| `boolean` | `BOOLEAN` |
| `binary` | `STRING` |

**按参数决定：**

| ES 类型 | Doris 类型 | 规则 |
|---------|-----------|------|
| `keyword` / `constant_keyword` | `VARCHAR(N)` | N 取 `ignore_above`，超过 65533 时用 `STRING` |
| `wildcard` | `STRING` | — |
| `text` / `match_only_text` | `TEXT` | analyzer 记录在 COMMENT |
| `date` | `DATETIME(3)` | 含 format 时记录在 COMMENT |
| `date_nanos` | `DATETIME(6)` | 纳秒截断为微秒 |
| `scaled_float` | `DECIMAL(38, S)` | S = log10(scaling_factor) |
| `ip` | `IPv4` / `IPv6` | 由配置 `ip_type` 决定 |

**展开为多列：**

| ES 类型 | 展开的 Doris 列 |
|---------|---------------|
| `integer_range` / `long_range` | `{name}_gte INT/BIGINT`，`{name}_lte INT/BIGINT` |
| `float_range` / `double_range` | `{name}_gte FLOAT/DOUBLE`，`{name}_lte FLOAT/DOUBLE` |
| `date_range` | `{name}_gte DATETIME(3)`，`{name}_lte DATETIME(3)` |
| `ip_range` | `{name}_gte IPv6`，`{name}_lte IPv6` |

**特殊处理：**

| ES 类型 | Doris 类型 | 说明 |
|---------|-----------|------|
| `geo_point` | `VARIANT` | 保留 `{lat, lon}` 原始结构，输出告警 |
| `point` | `VARIANT` | 保留 `{x, y}` 原始结构，输出告警 |
| `object`（有子属性） | 递归展平 | `user.name` → `user_name` |
| `object`（`enabled: false`） | `VARIANT` | 保留原始结构，不解析子字段 |
| `nested` | `VARIANT` | 丢失嵌套关联语义，输出告警 |
| `flattened` | `VARIANT` | — |
| `dense_vector` | `ARRAY<FLOAT>` | 无 ANN 检索能力 |
| `aggregate_metric_double` | `DOUBLE` | 仅保留数值 |
| `alias` / `runtime` | 跳过 | 虚拟字段，无需存储 |
| `join` / `percolator` | **ERROR** | Doris 无等价结构 |

**数组字段：**

ES mapping 无法区分单值和数组。在 `array_fields` 中指定字段路径后，该字段会转为 `ARRAY<T>` 类型。支持嵌套路径，如 `user.tags`。

### 自动索引策略

| 列类型 | 自动生成的 Doris 索引 |
|--------|---------------------|
| `keyword` / `constant_keyword` 列 | `INVERTED`（精确匹配） |
| `text` / `search_as_you_type` 列 | `INVERTED`（含 parser 和 `support_phrase=true`） |
| `match_only_text` 列 | `INVERTED`（含 parser，`support_phrase=false`，ES 不存储位置信息） |
| `wildcard` 列 | `INVERTED` + `NGRAM_BF(gram_size=3, bf_size=1024)` |
| `ARRAY` 列（非浮点） | `INVERTED`（支持 `array_contains()`） |
| `VARIANT` 列 | `INVERTED`（支持子列过滤） |
| `FLOAT` / `DOUBLE` 列 | 不建索引（Doris 限制） |
| `index: false` 字段 | 跳过 |

**Analyzer → Parser 映射：**

| ES Analyzer | Doris Parser |
|------------|-------------|
| `standard` / `simple` / `pattern` | `unicode` |
| `english` / `whitespace` | `english` |
| `ik_max_word` / `ik_smart` | `ik`（Doris 3.1.0+） |
| `smartcn` | `chinese` |
| 其他 / 无 | 无 parser（INVERTED 不带参数） |

### 自动建表决策

| 决策项 | DUPLICATE KEY | UNIQUE KEY |
|--------|--------------|------------|
| Key 列 | 时间列（仅此一列，keyword 列需手动指定） | `_id`（如有），否则第一个可用列 |
| 分区 | 检测时间列，按天 AUTO PARTITION | 仅当时间列也是 Key 列时分区 |
| 分桶 | `RANDOM BUCKETS AUTO` | `HASH(first_key) BUCKETS AUTO` |

可通过 `key_columns`、`partition_field`、`bucket_strategy` 配置项手动覆盖。

### CLI 参数

**全局：**

| 参数 | 说明 |
|------|------|
| `-f, --job-file FILE` | 任务配置文件 |
| `-v, --verbose` | 输出 DEBUG 级日志 |
| `-q, --quiet` | 仅输出 ERROR 级日志 |
| `--version` | 显示版本号 |

**单文件转换（`convert` 子命令 或直接 `-i`）：**

| 参数 | 说明 |
|------|------|
| `-i, --input FILE` | ES mapping JSON 文件（必填） |
| `-o, --output FILE` | 输出文件路径（默认 stdout） |
| `-c, --table-properties PROPS` | 表设置 YAML 文件路径或 inline YAML 字符串，可多次指定 |
| `--table-name NAME` | 覆盖表名 |
| `--model MODEL` | `duplicate`（默认）或 `unique` |
| `--include-id` | 添加 `_id VARCHAR(128)` 列 |
| `--warnings-only` | 仅输出告警，不生成 DDL |

**批量转换（`batch` 子命令）：**

| 参数 | 说明 |
|------|------|
| `--es-url URL` | ES 集群地址 |
| `--input-dir DIR` | 本地 mapping 目录（与 `--es-url` 二选一） |
| `--es-index PATTERN` | 索引名模式（默认 `*`） |
| `--es-user USER` | HTTP Basic 认证用户名 |
| `--es-password PASS` | HTTP Basic 认证密码 |
| `--no-verify-ssl` | 禁用 SSL 证书验证 |
| `-o, --output-dir DIR` | 输出目录（必填） |
| `--table-prefix PREFIX` | 表名前缀 |
| `--exclude-index REGEX` | 排除匹配的索引名 |
| `-c, --table-properties PROPS` | 表设置 YAML 文件路径或 inline YAML 字符串，可多次指定 |
| `--model MODEL` | `duplicate` 或 `unique` |
| `--include-id` | 添加 `_id` 列 |
| `--execute` | 生成后在 Doris 执行建表 |
| `--doris-host HOST` | Doris FE 地址（默认 `127.0.0.1`） |
| `--doris-port PORT` | Doris FE 端口（默认 `9030`） |
| `--doris-user USER` | Doris 用户名（默认 `root`） |
| `--doris-password PASS` | Doris 密码 |
| `--doris-db DB` | 目标数据库（不存在时自动创建） |
| `--fail-fast` | 遇到第一个错误立即停止 |
| `--warnings-only` | 仅输出告警，不生成文件 |

### 退出码

| 退出码 | 含义 |
|-------|------|
| `0` | 全部成功，无告警 |
| `1` | 成功，但存在告警（检查 stderr） |
| `2` | 存在错误 |

### 支持的 Mapping 格式

工具自动识别三种常见格式，无需手动转换：

```
格式 1 — ES 7+ API 响应（curl 直接获取）
{"index_name": {"mappings": {"properties": {...}}}}

格式 2 — ES 6.x（含 type 层）
{"index_name": {"mappings": {"doc": {"properties": {...}}}}}

格式 3 — 简化格式
{"mappings": {"properties": {...}}}
```

---

## 开发

**安装开发依赖：**

```bash
pip install -e ".[dev]"
```

**运行单元测试：**

```bash
pytest tests/ --ignore=tests/test_e2e.py -v
```

**运行 E2E 测试（需要 Docker）：**

```bash
./docker/e2e_run.sh
```

**项目结构：**

```
es-ddl-converter/
├── es_ddl_converter/
│   ├── cli.py              # CLI 入口，参数解析，日志配置
│   ├── mapping_parser.py   # ES mapping 解析，字段展平
│   ├── type_mapping.py     # ES → Doris 类型映射（handler registry）
│   ├── index_strategy.py   # 倒排索引、NGRAM 索引策略
│   ├── table_builder.py    # Key 列、分区、分桶逻辑
│   ├── ddl_renderer.py     # Jinja2 DDL 渲染
│   ├── batch.py            # 批量转换编排
│   ├── es_client.py        # ES HTTP 客户端
│   ├── doris_executor.py   # Doris DDL 执行器（pymysql）
│   └── warnings.py         # 转换告警收集
├── tests/
│   └── fixtures/           # 测试用 mapping 和 config 文件
└── pyproject.toml
```

---

## License

Apache-2.0
