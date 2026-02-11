# -*- coding: utf-8 -*-
"""
Step 2: 年报公告清单 -> output/report_manifest.csv
并在此阶段直接根据 detail_url 拼出 pdf_url（finalpage）。

运行：
  python step2_manifest.py
强制重建：
  python step2_manifest.py --rebuild
调试只跑前 50 家：
  python step2_manifest.py --limit 50
"""

from __future__ import annotations

import argparse
import re
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs

import pandas as pd
from tqdm import tqdm

import akshare as ak

import config


YEAR_PAT = re.compile(r"(\d{4})年?年度报告")


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


def pdf_url_from_detail_url(detail_url: str, ann_date_fallback: str = "") -> str:
    """
    detail_url -> https://static.cninfo.com.cn/finalpage/{YYYY-MM-DD}/{announcementId}.PDF
    """
    u = urlparse(detail_url)
    q = parse_qs(u.query)
    ann_id = (q.get("announcementId") or [None])[0]
    ann_time = (q.get("announcementTime") or [None])[0]
    if not ann_id:
        raise ValueError("detail_url 缺 announcementId")
    date = normalize_date_str(ann_time) or normalize_date_str(ann_date_fallback)
    if not date:
        raise ValueError("无法解析 announcementTime/ann_date")
    return f"https://static.cninfo.com.cn/finalpage/{date}/{ann_id}.PDF"


def disclosure_start_end() -> tuple[str, str]:
    start = f"{config.START_YEAR + config.ANN_START_YEAR_OFFSET}0101"
    end = f"{config.END_YEAR + config.ANN_END_YEAR_BUFFER}1231"
    return start, end


def parse_report_year(title: str) -> Optional[int]:
    m = YEAR_PAT.search(title or "")
    if not m:
        return None
    return int(m.group(1))


def title_excluded(title: str) -> bool:
    t = str(title or "")
    return any(pat in t for pat in config.TITLE_EXCLUDE_PATTERNS)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rebuild", action="store_true", help="忽略缓存，强制重建")
    parser.add_argument("--limit", type=int, default=None, help="只处理前 N 家（调试用）")
    args = parser.parse_args(argv)

    out_dir = config.OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    manu_path = out_dir / "manufacturing_stocks.csv"
    out_path = out_dir / "report_manifest.csv"

    if out_path.exists() and not args.rebuild:
        print(f"[Step2] 已存在，跳过：{out_path}")
        return 0

    if not manu_path.exists():
        raise FileNotFoundError(f"缺少 {manu_path}，请先运行 step1_industry.py")

    manu_df = pd.read_csv(manu_path, dtype={"code": str}, keep_default_na=False)
    if args.limit:
        manu_df = manu_df.head(args.limit)

    start_date, end_date = disclosure_start_end()
    all_rows = []

    for _, r in tqdm(manu_df.iterrows(), total=len(manu_df), desc="Step2 拉取年报清单"):
        code = str(r["code"])
        name = str(r.get("name", ""))

        try:
            df = ak.stock_zh_a_disclosure_report_cninfo(
                symbol=code,
                market="沪深京",
                category="年报",
                start_date=start_date,
                end_date=end_date,
            )
            if df is None or df.empty:
                time.sleep(config.SLEEP_BETWEEN_REQUESTS)
                continue

            # 统一列名
            rename_map = {}
            for c in df.columns:
                if c in ["代码", "证券代码"]:
                    rename_map[c] = "code"
                elif c in ["简称", "证券简称"]:
                    rename_map[c] = "name"
                elif c in ["公告标题", "标题"]:
                    rename_map[c] = "title"
                elif c in ["公告时间", "时间", "公告日期", "日期"]:
                    rename_map[c] = "ann_date"
                elif c in ["公告链接", "链接", "公告URL", "公告url", "url", "URL"]:
                    rename_map[c] = "detail_url"
            df = df.rename(columns=rename_map)

            # 年份解析 + 过滤
            df["report_year"] = df["title"].apply(parse_report_year)
            df = df[df["report_year"].between(config.START_YEAR, config.END_YEAR, inclusive="both")]

            # 标题过滤
            df = df[~df["title"].apply(title_excluded)]

            # 取同公司同年度最新一条
            df["ann_date"] = pd.to_datetime(df["ann_date"], errors="coerce")
            df = df.sort_values("ann_date").groupby(["code", "report_year"], as_index=False).tail(1)

            for _, rr in df.iterrows():
                ann_date = str(rr["ann_date"].date()) if pd.notna(rr["ann_date"]) else ""
                detail_url = str(rr.get("detail_url", "")).strip()
                title = str(rr.get("title", "")).strip()

                pdf_url = ""
                try:
                    if detail_url:
                        pdf_url = pdf_url_from_detail_url(detail_url, ann_date_fallback=ann_date)
                except Exception:
                    pdf_url = ""

                all_rows.append({
                    "code": code,
                    "name": name,
                    "report_year": int(rr["report_year"]),
                    "ann_date": ann_date,
                    "title": title,
                    "detail_url": detail_url,
                    "pdf_url": pdf_url,
                })

        except Exception as e:
            # 单公司失败不影响全局
            print(f"[Step2] 失败 {code}：{e}")

        time.sleep(config.SLEEP_BETWEEN_REQUESTS)

    out_df = pd.DataFrame(all_rows).sort_values(["code", "report_year"])
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[Step2] 完成：{out_path}，共 {len(out_df)} 份年报")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
