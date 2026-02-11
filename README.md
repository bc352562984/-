# CNINFO 年报关键词词频流水线（4 步 + 主控）

## 目录结构（把整个文件夹发给别人即可）
- config.py                # 改参数只改这里
- step1_industry.py        # 行业筛选 -> manufacturing_stocks.csv
- step2_manifest.py        # 年报清单 -> report_manifest.csv（并拼 pdf_url）
- step3_download.py        # 下载 PDF -> pdf_download_success/failed.csv + pdf/
- step4_keyword.py         # 抽取+词频 -> keyword_counts.csv
- run_pipeline.py          # 主控：一键跑 all / 单步跑
- requirements.txt         # 依赖

## 安装依赖
建议新建虚拟环境：
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
pip install -r requirements.txt
```

## 运行（推荐）
一键跑：
```bash
python run_pipeline.py all
```

分步跑：
```bash
python step1_industry.py
python step2_manifest.py
python step3_download.py
python step4_keyword.py
```

仅重试下载失败：
```bash
python run_pipeline.py retry_failed
# 或
python step3_download.py --only-failed
```

## 改关键词后重算
仅需：
1) 修改 config.py 里的 KEYWORD_GROUPS
2) 删除 output/keyword_counts.csv（或 step4_keyword.py 加 --overwrite）
3) 重新跑 step4：
```bash
python step4_keyword.py --overwrite
```

## 磁盘满了怎么办？
在 config.py 设置：
```python
PDF_DIR_OVERRIDE = Path(r"D:\cninfo_pdfs")
```
把 PDF 放到大盘。
##一个问题如果想迁移到别的电脑上执行第四步
👉 第四步（step4）找 PDF，是通过
pdf_download_success.csv 里的 pdf_path 列

👉 前三部执行完之后再改config.py应该是没用的
只能批量替换一下pdf_download_success.csv中pdf_path的路径
