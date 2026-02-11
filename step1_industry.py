# -*- coding: utf-8 -*-
"""
Step 1: 行业筛选 -> output/manufacturing_stocks.csv

用 cninfo 行业变更接口判定“制造业”：
- 行业门类 == "制造业" 或 行业编码以 "C" 开头

运行：
  python step1_industry.py
强制重建：
  python step1_industry.py --rebuild
测试只跑前 200 家：
  python step1_industry.py --limit 200
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Optional

import pandas as pd
from tqdm import tqdm

import akshare as ak

import config


def markets_filter(code: str) -> bool:
    code = str(code)
    want = set(config.MARKETS)
    ok = False
    if "SH" in want and code.startswith("6"):
        ok = True
    if "SZ" in want and (code.startswith("0") or code.startswith("3")):
        ok = True
    if "BJ" in want and code.startswith("8"):
        ok = True
    return ok


def is_manufacturing(code: str) -> bool:
    """
    返回是否制造业
    """
    try:
        df = ak.stock_industry_change_cninfo(symbol=code, start_date="19900101", end_date="20991231")
        if df is None or df.empty:
            return False

        # 选最新一条
        if "变更日期" in df.columns:
            df["变更日期"] = pd.to_datetime(df["变更日期"], errors="coerce")
            df = df.sort_values("变更日期")
        latest = df.iloc[-1]

        gate = str(latest.get("行业门类", "")).strip()
        if gate == config.INDUSTRY_GATE:
            return True

        ind_code = str(latest.get("行业编码", "")).strip()
        if ind_code.startswith("C"):
            return True

        big = str(latest.get("行业大类", "")).strip()
        mid = str(latest.get("行业中类", "")).strip()
        return ("制造" in big) or ("制造" in mid)
    except Exception:
        return False


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rebuild", action="store_true", help="忽略缓存，强制重建")
    parser.add_argument("--limit", type=int, default=None, help="只处理前 N 家（调试用）")
    args = parser.parse_args(argv)

    out_dir = config.OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "manufacturing_stocks.csv"

    if out_path.exists() and not args.rebuild:
        print(f"[Step1] 已存在，跳过：{out_path}")
        return 0

    df = ak.stock_info_a_code_name()
    # 兼容列名
    if "code" not in df.columns and "代码" in df.columns:
        df = df.rename(columns={"代码": "code"})
    if "name" not in df.columns and "简称" in df.columns:
        df = df.rename(columns={"简称": "name"})
    df = df[["code", "name"]].copy()
    df["code"] = df["code"].astype(str)

    df = df[df["code"].apply(markets_filter)].reset_index(drop=True)

    if args.limit:
        df = df.head(args.limit)

    rows = []
    for _, r in tqdm(df.iterrows(), total=len(df), desc="Step1 行业筛选(制造业)"):
        code = str(r["code"])
        name = str(r["name"])
        if is_manufacturing(code):
            rows.append({"code": code, "name": name})
        time.sleep(config.SLEEP_BETWEEN_REQUESTS)

    out_df = pd.DataFrame(rows).sort_values(["code"])
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[Step1] 完成：{out_path}，共 {len(out_df)} 家")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
