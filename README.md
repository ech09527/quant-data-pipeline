# Quant Data Pipeline

本仓库用于管理量化数据管道任务，通过 GitHub Actions 从 MinIO 读取 Parquet 文件并合并输出。

## 项目说明

当前使用 [DuckDB](https://duckdb.org/) 的 `httpfs` 扩展直接读写 MinIO（S3 兼容存储）。相比全内存合并，DuckDB 会在内存不足时将中间结果溢写到磁盘，更适合大规模 Parquet 合并。

## 目录结构

```
.
├── scripts/
│   └── merge_parquet.py      # Parquet 合并脚本
├── .github/
│   └── workflows/
│       ├── lint-pipelines.yml
│       └── merge-parquet.yml
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

敏感信息请通过 GitHub Secrets 或本地 `.env` 注入，不要写入代码。

## 本地运行

```bash
pip install -r requirements.txt

# 加载环境变量
set -a && source .env && set +a

# 先统计匹配文件（不写入）
python scripts/merge_parquet.py --dry-run

# 执行合并
python scripts/merge_parquet.py
```

## GitHub Workflow

| Workflow | 触发方式 | 说明 |
|----------|---------|------|
| `lint-pipelines.yml` | push / pull_request | Python 语法检查 |
| `merge-parquet.yml` | `workflow_dispatch` | 手动触发 Parquet 合并 |

合并任务需在 GitHub Secrets 中配置：`MINIO_ENDPOINT`、`MINIO_ACCESS_KEY`、`MINIO_SECRET_KEY`、`INPUT_BUCKET`，以及可选的 `MINIO_REGION`、`OUTPUT_BUCKET`。

手动触发时可指定：

- `input_glob` — 输入通配符
- `output_path` — 输出路径
- `memory_limit` — DuckDB 内存上限

## 示例

合并所有 1h K 线 Parquet：

```env
INPUT_GLOB=binance/futures/um/klines/1h/**/*.parquet
OUTPUT_PATH=binance/futures/um/klines/1h.parquet
```

会匹配如下文件并合并为一个 Parquet：

```text
binance/futures/um/klines/1h/0GUSDT/0GUSDT-1h-2025-09.parquet
binance/futures/um/klines/1h/BTCUSDT/BTCUSDT-1h-2025-09.parquet
...
```
