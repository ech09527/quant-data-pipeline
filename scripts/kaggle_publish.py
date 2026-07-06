"""Kaggle 数据集发布工具。"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kaggle.api.kaggle_api_extended import KaggleApi


def configure_kaggle_auth() -> str:
    """配置 Kaggle 认证，优先使用 Access Token。"""
    api_token = os.environ.get("KAGGLE_API_TOKEN", "").strip()
    legacy_key = os.environ.get("KAGGLE_KEY", "").strip()
    username = os.environ.get("KAGGLE_USERNAME", "").strip()

    if not api_token and legacy_key.startswith("KGAT_"):
        os.environ["KAGGLE_API_TOKEN"] = legacy_key
        api_token = legacy_key
        print(
            "提示: 检测到 KAGGLE_KEY 为 Access Token，请改用 KAGGLE_API_TOKEN",
            file=sys.stderr,
        )

    if api_token:
        return "access_token"

    if username and legacy_key:
        return "legacy"

    print(
        "缺少 Kaggle 认证，请配置以下任一方式：\n"
        "  1. KAGGLE_API_TOKEN=<Access Token>\n"
        "  2. KAGGLE_USERNAME + KAGGLE_KEY=<Legacy API Key>",
        file=sys.stderr,
    )
    sys.exit(1)


def get_kaggle_api() -> KaggleApi:
    auth_method = configure_kaggle_auth()
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    print(f"Kaggle 认证成功 ({auth_method})")
    return api


def normalize_dataset_metadata(
    metadata_path: Path,
    *,
    dataset_slug: str,
    title: str,
    license_name: str,
) -> None:
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    else:
        metadata = {}

    metadata["id"] = dataset_slug
    metadata.setdefault("title", title)
    metadata.setdefault("licenses", [{"name": license_name}])

    owner, slug = dataset_slug.split("/", 1) if "/" in dataset_slug else ("", dataset_slug)
    info = metadata.setdefault("info", {})
    info.setdefault("slug", slug)
    if owner:
        info.setdefault("ownerSlug", owner)

    metadata_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def write_dataset_metadata(
    staging_dir: Path,
    *,
    dataset_slug: str,
    title: str,
    license_name: str,
) -> Path:
    metadata_path = staging_dir / "dataset-metadata.json"
    normalize_dataset_metadata(
        metadata_path,
        dataset_slug=dataset_slug,
        title=title,
        license_name=license_name,
    )
    return metadata_path


def ensure_kaggle_metadata(
    api: KaggleApi,
    staging_dir: Path,
    *,
    dataset_slug: str,
    title: str,
    license_name: str,
    create_new: bool,
) -> None:
    metadata_path = staging_dir / "dataset-metadata.json"
    if create_new:
        write_dataset_metadata(
            staging_dir,
            dataset_slug=dataset_slug,
            title=title,
            license_name=license_name,
        )
        print(f"已写入新建数据集元数据: {metadata_path}")
        return

    try:
        api.dataset_metadata(dataset_slug, path=str(staging_dir))
        print(f"已同步 Kaggle 数据集元数据: {metadata_path}")
    except Exception as exc:
        print(
            f"无法获取已有数据集元数据 ({dataset_slug}): {exc}",
            file=sys.stderr,
        )
        print("将使用本地元数据模板继续上传", file=sys.stderr)

    normalize_dataset_metadata(
        metadata_path,
        dataset_slug=dataset_slug,
        title=title,
        license_name=license_name,
    )
    print(f"已规范化数据集元数据: {metadata_path}")


def upload_to_kaggle(
    api: KaggleApi,
    staging_dir: Path,
    *,
    create_new: bool,
    version_notes: str,
    dir_mode: str,
    public: bool,
) -> None:
    folder = str(staging_dir)
    if create_new:
        print(f"创建 Kaggle 数据集: {folder}")
        api.dataset_create_new(
            folder,
            public=public,
            convert_to_csv=False,
            dir_mode=dir_mode,
        )
        return

    print(f"上传 Kaggle 数据集新版本: {folder}")
    api.dataset_create_version(
        folder,
        version_notes,
        convert_to_csv=False,
        dir_mode=dir_mode,
    )


def publish_staging_dir(
    staging_dir: Path,
    *,
    dataset_slug: str,
    version_notes: str,
    dataset_title: str | None = None,
    license_name: str = "CC0-1.0",
    create_new: bool = False,
    dir_mode: str = "zip",
    public: bool = False,
) -> None:
    title = dataset_title or dataset_slug.split("/", 1)[-1].replace("-", " ").title()
    api = get_kaggle_api()
    ensure_kaggle_metadata(
        api,
        staging_dir,
        dataset_slug=dataset_slug,
        title=title,
        license_name=license_name,
        create_new=create_new,
    )
    upload_to_kaggle(
        api,
        staging_dir,
        create_new=create_new,
        version_notes=version_notes,
        dir_mode=dir_mode,
        public=public,
    )
