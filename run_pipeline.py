# -*- coding: utf-8 -*-
"""
主控脚本：按需串联执行 4 个步骤

用法：
  python run_pipeline.py all
  python run_pipeline.py step1
  python run_pipeline.py step2
  python run_pipeline.py step3
  python run_pipeline.py step4
  python run_pipeline.py retry_failed      # 仅重试失败下载（仍会重建 success/failed 输出）

提示：
- 你也可以分别运行 step1~step4
- 参数请在 config.py 中统一修改
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import config


def run(cmd: list[str]) -> None:
    print("\n" + "=" * 80)
    print("[RUN]", " ".join(cmd))
    print("=" * 80)
    subprocess.check_call(cmd)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("target", choices=["all", "step1", "step2", "step3", "step4", "retry_failed"])
    args = parser.parse_args()

    py = sys.executable
    root = Path(__file__).resolve().parent

    if args.target in ("all", "step1"):
        run([py, str(root / "step1_industry.py")])

    if args.target in ("all", "step2"):
        run([py, str(root / "step2_manifest.py")])

    if args.target in ("all", "step3"):
        run([py, str(root / "step3_download.py")])

    if args.target == "retry_failed":
        run([py, str(root / "step3_download.py"), "--only-failed"])

    if args.target in ("all", "step4"):
        run([py, str(root / "step4_keyword.py")])

    print("\n[DONE] 输出目录：", config.OUTPUT_DIR)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
