# Quant Data Pipeline - 项目认知文档

## 项目定位
量化数据管道：Binance Vision 下载合约数据 → 本地合并 → 异常值处理 → 上传 Kaggle yhydev97/quant-data。
已移除 MinIO 环节，不再从 MinIO 读写。
技术栈：Python 3.12+, pandas, pyarrow, numpy, GitHub Actions。
仓库：git@github.com:ech09527/quant-data-pipeline.git

## 数据流
Binance Vision(ZIP/CSV) → 本地合并单Parquet → 异常值处理 → 上传Kaggle yhydev97/quant-data

## 代理策略
- Binance API(fapi.binance.com)需代理: FAPI_HTTP_PROXY
- Vision数据下载(data.binance.vision)无限制，可走代理加速: VISION_HTTP_PROXY

## Secrets管理
GitHub/Kaggle配置存Vault(https://vault.nocsdn.com)，团队成员可通过gh命令操作GitHub。

## 核心脚本
- scripts/merge_parquet.py - 主入口：本地parquet合并→异常值处理→Kaggle发布
- scripts/funding_rate_to_kaggle.py - 资金费率下载合并→Kaggle
- scripts/outlier_detection.py - K线异常值检测与清洗
- scripts/kaggle_publish.py - Kaggle发布模块
- scripts/s3_client.py - S3客户端（已废弃，待清理）

## 异常值处理标准
- OHLC一致性校验（自动修正high/low）
- 价格跳变检测(>10%阈值，标记_price_jump)
- 成交量异常（零成交量标记_zero_volume、极端放量>5σ标记_extreme_volume）
- 时间戳连续性（缺失标记_gap_after）
- 重复时间戳（去重）

## 测试要求
所有测试必须在GitHub Workflow中运行。smoke-test-outlier.yml包含8个测试用例。

## 环境变量
参考README.md。MINIO_*变量已废弃。
关键变量：INPUT_GLOB, OUTPUT_PATH, PUBLISH_TO_KAGGLE, KAGGLE_DATASET, KAGGLE_API_TOKEN
