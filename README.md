# Quant Data Pipeline

本仓库用于管理量化数据管道任务。**线上环境在 GitHub Actions 中运行**；网络代理、Kaggle API Token、MinIO 凭证等程序所需配置，统一写在仓库的 **GitHub Secrets** 中，由 workflow 注入为环境变量。

## 项目说明

当前使用 [DuckDB](https://duckdb.org/) 的 `httpfs` 扩展从 MinIO（S3 兼容存储）读取 per-symbol Parquet，合并后写回 MinIO，并可直接发布到 Kaggle 数据集。合并完成后复用本地文件上传 Kaggle，**无需再从 MinIO 二次下载**。

另保留独立的 S3 → Kaggle 传输脚本，以及 Binance Vision 资金费率下载合并脚本。

## 目录结构

```
.
├── scripts/
│   ├── merge_parquet.py           # 合并 + 可选 Kaggle 发布（主入口）
│   ├── funding_rate_to_kaggle.py  # 下载 UM USDT 资金费率并发布 Kaggle
│   ├── s3_to_kaggle.py            # 仅 S3 → Kaggle 传输
│   ├── kaggle_publish.py          # Kaggle 发布公共模块
│   └── s3_client.py               # MinIO/S3 客户端公共模块
├── .github/
│   └── workflows/
│       ├── lint-pipelines.yml
│       ├── quant-data-pipeline.yml      # 合并 + 发布（主 workflow）
│       ├── funding-rate-to-kaggle.yml   # 资金费率下载合并 → Kaggle
│       └── s3-to-kaggle.yml             # 仅上传已有对象
├── requirements.txt
├── .env                        # 本地环境变量（勿提交）
└── README.md
```

## 环境变量

| 变量 | 说明 |
|------|------|
| `MINIO_ENDPOINT` | MinIO S3 API 地址，如 `https://fs.example.com` |
| `MINIO_ACCESS_KEY` | 访问密钥 |
| `MINIO_SECRET_KEY` | 密钥 |
| `MINIO_REGION` | 区域，默认 `us-east-1` |
| `INPUT_BUCKET` | 输入桶名 |
| `INPUT_GLOB` | 桶内对象 key 通配符，如 `binance/futures/um/klines/1h/**/*.parquet` |
| `OUTPUT_BUCKET` | 输出桶名（可与输入桶相同） |
| `OUTPUT_PATH` | 输出对象路径，如 `binance/futures/um/klines/1h.parquet` |
| `DUCKDB_MEMORY_LIMIT` | DuckDB 内存上限，默认 `4GB` |
| `DUCKDB_TEMP_DIRECTORY` | DuckDB 临时目录，默认 `/tmp/duckdb` |
| `SYMBOL_FROM_PATH` | 设为 `false` 时不从路径提取 `symbol` 列，默认开启 |
| `PUBLISH_TO_KAGGLE` | 设为 `true` 时合并后发布到 Kaggle |

### Kaggle 发布

| 变量 | 说明 |
|------|------|
| `KAGGLE_API_TOKEN` | Kaggle Access Token（推荐，`KGAT_` 开头） |
| `KAGGLE_USERNAME` | Legacy 认证用户名（与 `KAGGLE_KEY` 搭配） |
| `KAGGLE_KEY` | Legacy API Key（旧版 `kaggle.json` 中的 key） |
| `KAGGLE_DATASET` | 数据集 slug，如 `username/my-dataset` |
| `VERSION_NOTES` | 版本说明 |
| `KAGGLE_DATASET_TITLE` | 新建数据集时的标题（`--kaggle-create-new`） |
| `KAGGLE_LICENSE` | 数据集许可证，默认 `CC0-1.0` |
| `KAGGLE_STAGING_DIR` | 上传暂存目录，默认 `/tmp/kaggle-staging` |
| `KAGGLE_DIR_MODE` | 上传打包方式：`zip` 或 `tar`，默认 `zip` |
| `KAGGLE_PUBLIC` | 设为 `true` 时新建公开数据集（`--kaggle-create-new`） |

**线上**：上述敏感项与代理等配置放在仓库 Secrets，由 Actions 注入。  
**本地**：可用 `.env` 注入同名变量做调试（勿提交）。不要把密钥写进代码或 workflow 明文。

## 本地运行

```bash
pip install -r requirements.txt

# 加载环境变量
set -a && source .env && set +a

# 先统计匹配文件（不写入）
python scripts/merge_parquet.py --dry-run

# 仅合并并写回 MinIO
python scripts/merge_parquet.py --no-publish-to-kaggle

# 合并 + 发布到 Kaggle（一条命令完成）
python scripts/merge_parquet.py --publish-to-kaggle

# S3 → Kaggle：仅上传已有对象（不合并）
python scripts/s3_to_kaggle.py --dry-run
python scripts/s3_to_kaggle.py
python scripts/s3_to_kaggle.py --create-new --public
```

## 资金费率（Vision → Kaggle）

从 [Binance Vision](https://data.binance.vision/) 下载 UM 永续 **USDT** 月度 `fundingRate` ZIP，合并为单个 Parquet（含 `symbol`），直接发布到 Kaggle（不上 MinIO）。

字段：`symbol`, `calc_time`, `funding_interval_hours`, `last_funding_rate`

线上由 `funding-rate-to-kaggle.yml` 调度；代理与 Kaggle 认证从仓库 Secrets 注入（见下文「仓库 Secrets」）。代理与 `bn-data-collect` 合约 K 线采集一致：

| 变量 | 说明 |
|------|------|
| `FAPI_HTTP_PROXY` | 访问 `fapi.binance.com`（交易对） |
| `VISION_HTTP_PROXY` | 访问 `data.binance.vision`（ZIP） |

本地试跑几个合约（用 `uv`，勿依赖 bash 脚本；本地直连可用时可不设代理）：

```text
uv sync --no-dev
# 可选：FAPI_HTTP_PROXY=... VISION_HTTP_PROXY=...
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT \
START_MONTH=2025-01 \
END_MONTH=2025-02 \
WORK_DIR=/tmp/funding-rate-test \
PUBLISH_TO_KAGGLE=false \
uv run funding-rate-to-kaggle
```

网络连通性检查：

```text
uv run check-binance-network
```

全量并上传 Kaggle：

```text
KAGGLE_DATASET=yhydev97/quant-data \
KAGGLE_API_TOKEN=... \
PUBLISH_TO_KAGGLE=true \
uv run funding-rate-to-kaggle
```

默认输出路径：`binance/futures/um/fundingRate.parquet`。更新已有数据集时会先拉取现有文件再写入/覆盖该路径，避免冲掉已有 klines。

## GitHub Workflow（线上环境）

正式跑数、定时更新都在 **GitHub Actions** 上执行（`ubuntu-latest`）。workflow 从仓库 Secrets 读取配置并注入环境变量，脚本本身不硬编码密钥或代理。

| Workflow | 触发方式 | 说明 |
|----------|---------|------|
| `lint-pipelines.yml` | push / pull_request | Python 语法检查 |
| `quant-data-pipeline.yml` | `workflow_dispatch` / push | **主流程**：合并 + 写回 MinIO + 发布 Kaggle |
| `funding-rate-to-kaggle.yml` | `workflow_dispatch` / 每周一 | 资金费率下载合并 → Kaggle |
| `s3-to-kaggle.yml` | `workflow_dispatch` | 仅上传 MinIO 已有对象到 Kaggle |

### 仓库 Secrets

在仓库 **Settings → Secrets and variables → Actions** 中配置（按所用 workflow 需要）：

| Secret | 用途 | 相关 workflow |
|--------|------|----------------|
| `MINIO_ENDPOINT` / `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | MinIO 访问 | 主流程、`s3-to-kaggle` |
| `MINIO_REGION` / `INPUT_BUCKET` / `OUTPUT_BUCKET` | 区域与桶（可选/按需） | 主流程、`s3-to-kaggle` |
| `KAGGLE_API_TOKEN` | Kaggle 认证（推荐，`KGAT_` 开头） | 所有发布到 Kaggle 的流程 |
| `KAGGLE_USERNAME` + `KAGGLE_KEY` | Legacy Kaggle 认证（可选） | 同上 |
| `FAPI_HTTP_PROXY` | 访问 `fapi.binance.com` | `funding-rate-to-kaggle` |
| `VISION_HTTP_PROXY` | 访问 `data.binance.vision` | `funding-rate-to-kaggle` |

`FAPI_HTTP_PROXY` / `VISION_HTTP_PROXY` 与 `bn-data-collect` 合约采集侧代理约定一致。非敏感参数（如 `KAGGLE_DATASET`、时间范围）一般由 workflow 默认值或 `workflow_dispatch` 输入提供，不必放进 Secrets。

手动触发 **Quant Data Pipeline** 时可指定：

- `input_glob` — 输入通配符
- `output_path` — 输出路径
- `memory_limit` — DuckDB 内存上限
- `symbol_from_path` — 是否从文件路径提取 `symbol` 列
- `publish_to_kaggle` — 合并完成后发布到 Kaggle（默认开启）
- `kaggle_dataset` — Kaggle 数据集 slug（默认 `yhydev97/quant-data`）
- `version_notes` — Kaggle 版本说明
- `kaggle_dir_mode` — 上传打包方式（`zip` / `tar`）

## 数据流

```text
MinIO（per-symbol Parquet）
        ↓  DuckDB 读取并合并
本地临时 Parquet
        ↓  分段上传
MinIO（合并后单文件）
        ↓  复用本地文件（无需二次下载）
Kaggle 数据集
```

## 示例

合并所有 1h K 线 Parquet 并发布：

```env
INPUT_GLOB=binance/futures/um/klines/1h/**/*.parquet
OUTPUT_PATH=binance/futures/um/klines/1h.parquet
PUBLISH_TO_KAGGLE=true
KAGGLE_DATASET=yhydev97/quant-data
VERSION_NOTES=2025-09 monthly update
```

会匹配如下文件并合并为一个 Parquet：

```text
binance/futures/um/klines/1h/0GUSDT/0GUSDT-1h-2025-09.parquet
binance/futures/um/klines/1h/BTCUSDT/BTCUSDT-1h-2025-09.parquet
...
```

合并时会从每个 Parquet 文件的父目录名提取 `symbol` 列（例如 `.../BTCUSDT/BTCUSDT-1h-2025-09.parquet` → `BTCUSDT`）。若源文件本身已有 `symbol` 列，则优先保留非空值。
