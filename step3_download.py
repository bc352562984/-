# -*- coding: utf-8 -*-
"""
Step 3: 下载 PDF -> output/pdf/
并输出：
  - output/pdf_download_success.csv
  - output/pdf_download_failed.csv

改进点（用于修复你这种“浏览器能下、Python 下不动”的 case）：
- 同时尝试：https/http × .PDF/.pdf 四种组合
- failed.csv 记录 status_code + last_url，便于定位真实原因
- 403/429/5xx 更长退避，降低撞风控概率
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError
from tqdm import tqdm

import config


def get_pdf_dir() -> Path:
    if config.PDF_DIR_OVERRIDE is not None:
        return Path(config.PDF_DIR_OVERRIDE)
    return config.OUTPUT_DIR / "pdf"


def safe_pdf_name(code: str, year: int) -> str:
    return f"{code}_{year}.pdf"


def normalize_date_str(s: str) -> Optional[str]:
    if not s:
        return None
    try:
        dt = pd.to_datetime(str(s).strip(), errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def pdf_url_from_detail_url(detail_url: str, ann_date_fallback: str = "") -> Optional[str]:
    if not detail_url:
        return None
    u = urlparse(detail_url)
    q = parse_qs(u.query)
    ann_id = (q.get("announcementId") or [None])[0]
    ann_time = (q.get("announcementTime") or [None])[0]
    if not ann_id:
        return None
    date = normalize_date_str(ann_time) or normalize_date_str(ann_date_fallback)
    if not date:
        return None
    # 注意：这里只生成一个“基准 url”，实际下载会尝试多种变体
    return f"https://static.cninfo.com.cn/finalpage/{date}/{ann_id}.PDF"


def is_pdf_file(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size < 1024:
            return False
        with open(path, "rb") as f:
            head = f.read(4)
        return head == b"%PDF"
    except Exception:
        return False


def candidate_urls(base_url: str) -> list[str]:
    """
    生成候选 URL：
    - https/http
    - .PDF/.pdf
    """
    if not base_url:
        return []
    urls = set()

    def add(u: str):
        if u:
            urls.add(u)

    add(base_url)

    # scheme swap
    if base_url.startswith("https://"):
        add("http://" + base_url[len("https://"):])
    elif base_url.startswith("http://"):
        add("https://" + base_url[len("http://"):])

    # ext swap (case)
    for u in list(urls):
        if u.endswith(".PDF"):
            add(u[:-4] + ".pdf")
        elif u.endswith(".pdf"):
            add(u[:-4] + ".PDF")

    # 再把 scheme+ext 组合补齐
    for u in list(urls):
        if u.startswith("https://"):
            add("http://" + u[len("https://"):])
        elif u.startswith("http://"):
            add("https://" + u[len("http://"):])

    # 保持稳定顺序：先 https，再 http；先 .pdf 再 .PDF（更贴近你浏览器拿到的形式）
    def key(u: str):
        return (
            0 if u.startswith("https://") else 1,
            0 if u.endswith(".pdf") else 1,
            u,
        )

    return sorted(urls, key=key)


def unwrap_exc(e: Exception) -> Exception:
    if isinstance(e, RetryError) and e.last_attempt:
        ex = e.last_attempt.exception()
        return ex if ex else e
    return e


@retry(
    retry=retry_if_exception_type((requests.RequestException,)),
    stop=stop_after_attempt(config.MAX_RETRY),
    wait=wait_exponential(multiplier=1, min=1, max=30),  # 退避加长
)
def _download_once(url: str, out_path: Path, session: requests.Session) -> tuple[int, int, str]:
    headers = {
        "User-Agent": config.USER_AGENT,
        "Referer": config.REFERER,
        "Accept": "application/pdf,application/octet-stream,*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    }

    with session.get(url, headers=headers, stream=True, timeout=config.REQUEST_TIMEOUT, allow_redirects=True) as r:
        status = r.status_code
        ctype = r.headers.get("Content-Type", "") or ""

        # 403/429 很常见：交给 tenacity 重试，但要“抛出 requests 异常”
        if status in (403, 429) or (500 <= status <= 599):
            r.raise_for_status()

        if status != 200:
            r.raise_for_status()

        out_path.parent.mkdir(parents=True, exist_ok=True)
        size = 0

        with open(out_path, "wb") as f:
            first = True
            for chunk in r.iter_content(chunk_size=1024 * 512):
                if not chunk:
                    continue
                if first:
                    first = False
                    # 如果不是 PDF 直接判失败（很多时候会返回 HTML）
                    if not chunk.startswith(b"%PDF"):
                        raise requests.RequestException("Not a PDF payload (magic mismatch)")
                f.write(chunk)
                size += len(chunk)

        return status, size, ctype


@dataclass
class DownloadResult:
    code: str
    name: str
    report_year: int
    ann_date: str
    title: str
    detail_url: str
    pdf_url: str
    pdf_path: str
    ok: bool
    status_code: str
    last_url: str
    file_size: int
    error_type: str
    error_message: str


def download_one(row: dict, pdf_dir: Path) -> DownloadResult:
    code = str(row.get("code", "")).strip()
    name = str(row.get("name", "")).strip()
    year = int(row.get("report_year", 0))
    ann_date = str(row.get("ann_date", "")).strip()
    title = str(row.get("title", "")).strip()
    detail_url = str(row.get("detail_url", "")).strip()
    base_pdf_url = str(row.get("pdf_url", "")).strip()

    if not base_pdf_url:
        base_pdf_url = pdf_url_from_detail_url(detail_url, ann_date_fallback=ann_date) or ""

    pdf_path = pdf_dir / safe_pdf_name(code, year)

    if is_pdf_file(pdf_path):
        return DownloadResult(code, name, year, ann_date, title, detail_url, base_pdf_url, str(pdf_path),
                              True, "200", str(pdf_path), int(pdf_path.stat().st_size), "", "")

    if not base_pdf_url:
        return DownloadResult(code, name, year, ann_date, title, detail_url, base_pdf_url, str(pdf_path),
                              False, "", "", 0, "MissingPDFURL", "无法生成 pdf_url")

    session = requests.Session()

    # 对“失败重试”建议降低节奏：每条任务随机睡一点点
    time.sleep(config.SLEEP_BETWEEN_REQUESTS + random.random() * 0.2)

    last_status = ""
    last_url = ""

    for u in candidate_urls(base_pdf_url):
        try:
            status, size, ctype = _download_once(u, pdf_path, session)

            if not is_pdf_file(pdf_path):
                try:
                    pdf_path.unlink(missing_ok=True)
                except Exception:
                    pass
                raise RuntimeError(f"下载内容不是 PDF（Content-Type={ctype}）")

            return DownloadResult(code, name, year, ann_date, title, detail_url, u, str(pdf_path),
                                  True, str(status), u, size, "", "")

        except Exception as e:
            ee = unwrap_exc(e)
            last_url = u
            if isinstance(ee, requests.HTTPError) and getattr(ee, "response", None) is not None:
                last_status = str(ee.response.status_code)
            else:
                last_status = ""
            # 403/429 再额外等一下（很关键）
            if last_status in ("403", "429"):
                time.sleep(10 + random.random() * 10)

    # 全部尝试失败
    try:
        if pdf_path.exists() and pdf_path.stat().st_size < 1024:
            pdf_path.unlink(missing_ok=True)
    except Exception:
        pass

    return DownloadResult(code, name, year, ann_date, title, detail_url, base_pdf_url, str(pdf_path),
                          False, last_status, last_url, 0, "DownloadFailed", f"最终失败，last_url={last_url}, status={last_status}")


def build_success_failed(manifest_df: pd.DataFrame, pdf_dir: Path, error_map: dict[tuple[str, int], DownloadResult]):
    rows_ok = []
    rows_bad = []

    for _, r in manifest_df.iterrows():
        code = str(r["code"]).strip()
        year = int(r["report_year"])
        pdf_path = pdf_dir / safe_pdf_name(code, year)

        base = {
            "code": code,
            "name": str(r.get("name", "")).strip(),
            "report_year": year,
            "ann_date": str(r.get("ann_date", "")).strip(),
            "title": str(r.get("title", "")).strip(),
            "detail_url": str(r.get("detail_url", "")).strip(),
            "pdf_url": str(r.get("pdf_url", "")).strip(),
            "pdf_path": str(pdf_path),
        }

        if is_pdf_file(pdf_path):
            base["file_size"] = int(pdf_path.stat().st_size)
            rows_ok.append(base)
        else:
            dr = error_map.get((code, year))
            base["status_code"] = dr.status_code if dr else ""
            base["last_url"] = dr.last_url if dr else ""
            base["error_type"] = dr.error_type if dr else ""
            base["error_message"] = dr.error_message if dr else ""
            rows_bad.append(base)

    return pd.DataFrame(rows_ok), pd.DataFrame(rows_bad)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only-failed", action="store_true", help="仅重试上一次 failed 列表（仍会重建 success/failed 输出）")
    parser.add_argument("--limit", type=int, default=None, help="只处理前 N 条（调试用）")
    parser.add_argument("--workers", type=int, default=config.DOWNLOAD_WORKERS, help="下载并发数")
    args = parser.parse_args(argv)

    out_dir = config.OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "report_manifest.csv"
    if not manifest_path.exists():
        raise FileNotFoundError(f"缺少 {manifest_path}，请先运行 step2_manifest.py")

    pdf_dir = get_pdf_dir()
    pdf_dir.mkdir(parents=True, exist_ok=True)

    success_path = out_dir / "pdf_download_success.csv"
    failed_path = out_dir / "pdf_download_failed.csv"

    manifest_df = pd.read_csv(manifest_path, dtype={"code": str}, keep_default_na=False)

    only_failed_keys = None
    if args.only_failed and failed_path.exists():
        failed_df = pd.read_csv(failed_path, dtype={"code": str}, keep_default_na=False)
        only_failed_keys = set((str(c).strip(), int(y)) for c, y in zip(failed_df["code"], failed_df["report_year"]))

    if args.limit:
        manifest_df = manifest_df.head(args.limit)

    need_rows = []
    for _, r in manifest_df.iterrows():
        code = str(r["code"]).strip()
        year = int(r["report_year"])
        if only_failed_keys is not None and (code, year) not in only_failed_keys:
            continue
        pdf_path = pdf_dir / safe_pdf_name(code, year)
        if not is_pdf_file(pdf_path):
            need_rows.append(r.to_dict())

    print(f"[Step3] 清单总数: {len(manifest_df)}，本次需尝试下载: {len(need_rows)}，workers={args.workers}")
    error_map: dict[tuple[str, int], DownloadResult] = {}

    if need_rows:
        with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = [ex.submit(download_one, row, pdf_dir) for row in need_rows]
            for fut in tqdm(cf.as_completed(futures), total=len(futures), desc="Step3 下载PDF"):
                res = fut.result()
                error_map[(res.code, res.report_year)] = res

    ok_df, bad_df = build_success_failed(manifest_df, pdf_dir, error_map)
    ok_df.to_csv(success_path, index=False, encoding="utf-8-sig")
    bad_df.to_csv(failed_path, index=False, encoding="utf-8-sig")

    print(f"[Step3] 成功: {len(ok_df)}，失败: {len(bad_df)}")
    print(f"[Step3] 输出 success: {success_path}")
    print(f"[Step3] 输出 failed : {failed_path}")
    print(f"[Step3] PDF目录      : {pdf_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
