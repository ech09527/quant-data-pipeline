#!/usr/bin/env python3
"""Check FAPI / Vision connectivity with optional per-endpoint proxies (no bash)."""

from __future__ import annotations

import os
import sys
from urllib.parse import urlsplit

import httpx

DEFAULT_FAPI_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
DEFAULT_VISION_URL = (
    "https://data.binance.vision/data/futures/um/monthly/fundingRate/"
    "BTCUSDT/BTCUSDT-fundingRate-2025-01.zip"
)


def _proxy_from_env(key: str) -> str | None:
    value = os.environ.get(key, "").strip()
    return value or None


def _mask_proxy(proxy: str | None) -> str:
    if not proxy:
        return "(none)"
    parts = urlsplit(proxy)
    host = parts.hostname or ""
    port = f":{parts.port}" if parts.port else ""
    scheme = f"{parts.scheme}://" if parts.scheme else ""
    return f"{scheme}{host}{port}"


def check(name: str, url: str, proxy: str | None) -> None:
    print("")
    print("=" * 30)
    print(f"CHECK: {name}")
    print(f"URL: {url}")
    print(f"PROXY: {_mask_proxy(proxy)}")

    kwargs: dict = {"timeout": 30.0, "follow_redirects": True}
    if proxy:
        kwargs["proxy"] = proxy

    with httpx.Client(**kwargs) as client:
        response = client.get(url, headers={"Range": "bytes=0-63"})
        print(f"http_code={response.status_code}")
        if response.status_code >= 400:
            raise SystemExit(f"ERROR: {name} returned HTTP {response.status_code}")


def main() -> None:
    fapi_url = os.environ.get("FAPI_URL", DEFAULT_FAPI_URL).strip() or DEFAULT_FAPI_URL
    vision_url = os.environ.get("VISION_URL", DEFAULT_VISION_URL).strip() or DEFAULT_VISION_URL
    fapi_proxy = _proxy_from_env("FAPI_HTTP_PROXY")
    vision_proxy = _proxy_from_env("VISION_HTTP_PROXY")

    check("FAPI", fapi_url, fapi_proxy)
    check("VISION", vision_url, vision_proxy)
    print("Network checks passed.")


if __name__ == "__main__":
    main()
