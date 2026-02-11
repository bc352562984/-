# -*- coding: utf-8 -*-
"""
Step 4: 抽取 + 词频（多线程）
输入：output/pdf_download_success.csv
输出：output/keyword_counts.csv

运行：
  python step4_keyword.py
改并发：
  python step4_keyword.py --workers 12
重算（覆盖输出）：
  python step4_keyword.py --overwrite
调试只跑前 200 份：
  python step4_keyword.py --limit 200

说明：
- 这一步完全离线（不依赖 akshare / cninfo 网络）
- 非常适合你改关键词后反复重算
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

import pandas as pd
from tqdm import tqdm

import fitz  # PyMuPDF

import config


def clean_spaces(s: str) -> str:
    return re.sub(r"\s+", "", s or "")


@dataclass(frozen=True)
class KeywordItem:
    group: str
    raw: str
    norm: str
    has_ascii: bool


def prepare_keywords(groups: Dict[str, List[str]]) -> List[KeywordItem]:
    out: List[KeywordItem] = []
    for g, kws in groups.items():
        for kw in kws:
            kw = kw.strip()
            if not kw:
                continue
            norm = clean_spaces(kw)
            out.append(KeywordItem(group=g, raw=kw, norm=norm, has_ascii=bool(re.search(r"[A-Za-z]", norm))))
    return out


KW_ITEMS = prepare_keywords(config.KEYWORD_GROUPS)


def extract_text_from_pdf(pdf_path: Path) -> str:
    doc = fitz.open(pdf_path)
    chunks = []
    try:
        for page in doc:
            chunks.append(page.get_text("text"))
    finally:
        doc.close()
    return "\n".join(chunks)


def count_keywords(text: str, kw_items: List[KeywordItem]) -> Dict[str, int]:
    t_norm = clean_spaces(text)
    t_upper = t_norm.upper()
    counts: Dict[str, int] = {}
    for item in kw_items:
        if item.has_ascii:
            counts[item.raw] = t_upper.count(item.norm.upper())
        else:
            counts[item.raw] = t_norm.count(item.norm)
    return counts


def load_done_set(out_csv: Path) -> Set[Tuple[str, int]]:
    done: Set[Tuple[str, int]] = set()
    if not out_csv.exists():
        return done
    try:
        df = pd.read_csv(out_csv, dtype={"code": str}, keep_default_na=False)
        for _, r in df.iterrows():
            code = str(r.get("code", "")).strip()
            year = int(str(r.get("report_year", "0")).strip() or "0")
            if code and year:
                done.add((code, year))
    except Exception:
        pass
    return done


def process_one(row: dict) -> dict:
    code = str(row.get("code", "")).strip()
    name = str(row.get("name", "")).strip()
    year = int(row.get("report_year", 0))
    ann_date = str(row.get("ann_date", "")).strip()
    title = str(row.get("title", "")).strip()
    detail_url = str(row.get("detail_url", "")).strip()
    pdf_url = str(row.get("pdf_url", "")).strip()
    pdf_path = Path(str(row.get("pdf_path", "")).strip())

    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF 不存在: {pdf_path}")

    text = extract_text_from_pdf(pdf_path)

    if len(clean_spaces(text)) < config.TEXT_MIN_LEN:
        counts = {k.raw: 0 for k in KW_ITEMS}
        text_len = len(clean_spaces(text))
        scan_flag = 1
    else:
        counts = count_keywords(text, KW_ITEMS)
        text_len = len(clean_spaces(text))
        scan_flag = 0

    out = {
        "code": code,
        "name": name,
        "report_year": year,
        "ann_date": ann_date,
        "title": title,
        "detail_url": detail_url,
        "pdf_url": pdf_url,
        "pdf_path": str(pdf_path),
        "text_len": text_len,
        "scan_like": scan_flag,  # 1 表示可能扫描版（未做 OCR）
    }
    out.update(counts)
    return out


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=config.EXTRACT_WORKERS, help="抽取+词频并发数")
    parser.add_argument("--limit", type=int, default=None, help="只处理前 N 份（调试用）")
    parser.add_argument("--overwrite", action="store_true", help="覆盖输出（重新计算所有）")
    args = parser.parse_args(argv)

    out_dir = config.OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    in_path = out_dir / "pdf_download_success.csv"
    if not in_path.exists():
        raise FileNotFoundError(f"缺少 {in_path}，请先运行 step3_download.py")

    out_csv = out_dir / "keyword_counts.csv"
    if args.overwrite and out_csv.exists():
        out_csv.unlink(missing_ok=True)

    df = pd.read_csv(in_path, dtype={"code": str}, keep_default_na=False)
    if args.limit:
        df = df.head(args.limit)

    done = load_done_set(out_csv)
    kw_cols = [k.raw for k in KW_ITEMS]
    fieldnames = ["code", "name", "report_year", "ann_date", "title", "detail_url", "pdf_url", "pdf_path", "text_len", "scan_like"] + kw_cols

    write_header = not out_csv.exists()
    total = len(df)

    with open(out_csv, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()

        tasks = []
        with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
            for _, r in df.iterrows():
                code = str(r.get("code", "")).strip()
                year = int(str(r.get("report_year", "0")).strip() or "0")
                if not code or not year:
                    continue
                if (code, year) in done:
                    continue
                tasks.append(ex.submit(process_one, r.to_dict()))

            for fut in tqdm(cf.as_completed(tasks), total=len(tasks), desc="Step4 抽取+词频"):
                try:
                    row = fut.result()
                    writer.writerow(row)
                except Exception as e:
                    print(f"[Step4] 失败：{e}")

    print(f"[Step4] 完成：{out_csv}（新增 {len(tasks)} 行，输入 {total} 行）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
