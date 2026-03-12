# -*- coding: utf-8 -*-
"""
在「受影响显著的一级行业_中信30_按月份分类」和「受影响显著一级行业_历届平均涨跌幅」的基础上，
生成按月份拆分的、便于实盘参考的月度显著行业表。

目标：方便你在某个月（比如 2026 年 4 月）准备做“会议行情”时，只看一张本月的清单，
里面包含：会议名称、时间窗口、一级行业、样本数、涨跌次数与概率、平均涨跌幅等。

- 输入：
  1) output/受影响显著的一级行业_中信30_按月份分类.csv
     - 关键字段：industry, window, meeting_family, 下一次月份
  2) output/受影响显著一级行业_历届平均涨跌幅_中信30.csv
     - 关键字段：meeting_family, window, industry, 届数, 涨跌次数/概率, 平均涨跌幅

- 处理流程：
  1) 用 (meeting_family, window, industry) 作为主键，将两张表 inner join 合并；
  2) 从「下一次预计召开时间」与「预测备注」解析出「下一次召开_具体日期」（如 3月5日、11月5日–10日、4月下旬）。
  3) 选出实盘最关心的字段（含下一次召开_具体日期）。
  4) 按月份拆分：对 下一次月份 = 1..12，分别导出一张 CSV（不含权重组列、不含上一届召开时间）。

- 输出（按你指定的命名方案 B）：
  对每个有数据的月份 m 生成：
    output/受影响显著一级行业_2026M{mm}_中信30.csv
  例如：
    output/受影响显著一级行业_2026M03_中信30.csv
    output/受影响显著一级行业_2026M04_中信30.csv
"""
from pathlib import Path
import re

import pandas as pd


BASE = Path(__file__).resolve().parent.parent
PATH_MONTH = BASE / "output" / "受影响显著的一级行业_中信30_按月份分类.csv"
PATH_STATS = BASE / "output" / "受影响显著一级行业_历届平均涨跌幅_中信30.csv"
OUT_DIR = BASE / "output"

# 目前这套预测是针对 2026 年的“下一次预计召开时间”
YEAR = 2026


def next_meeting_md(row) -> str:
    """
    从「下一次预计召开时间」和「预测备注」解析出下一次召开的具体日期（仅月日，无年份）。
    例如：3月5日、11月5日–10日、4月下旬、3月。
    """
    next_str = row.get("下一次预计召开时间") or ""
    remark = row.get("预测备注") or ""
    if pd.isna(next_str):
        next_str = ""
    else:
        next_str = str(next_str).strip()
    if pd.isna(remark):
        remark = ""
    else:
        remark = str(remark).strip()

    # 1) 下一次预计召开时间里已有「X月X日」或「X月X日至X日」
    m = re.search(r"(\d{1,2})月(\d{1,2})日\s*至\s*(\d{1,2})日", next_str)
    if m:
        mo, d1, d2 = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{mo}月{d1}日–{d2}日"
    m = re.search(r"(\d{1,2})月(\d{1,2})日", next_str)
    if m:
        return f"{int(m.group(1))}月{int(m.group(2))}日"

    # 2) 下旬/中旬/月上旬
    if "下旬" in next_str:
        mo = re.search(r"(\d{1,2})月", next_str)
        return f"{int(mo.group(1))}月下旬" if mo else "下旬"
    if "中旬" in next_str:
        mo = re.search(r"(\d{1,2})月", next_str)
        return f"{int(mo.group(1))}月中旬" if mo else "中旬"
    if "月上旬" in next_str:
        mo = re.search(r"(\d{1,2})月", next_str)
        return f"{int(mo.group(1))}月上旬" if mo else "月上旬"

    # 3) 月末、一季度（约3月末）等
    if "月末" in next_str or "3月末" in next_str:
        mo = re.search(r"(\d{1,2})月", next_str)
        if mo:
            return f"{int(mo.group(1))}月下旬"
        if "3月" in next_str:
            return "3月下旬"
        return "月末"

    # 4) 从预测备注里补具体日（如「人大一般3月5日开幕」）
    m = re.search(r"(\d{1,2})月(\d{1,2})日", remark)
    if m:
        return f"{int(m.group(1))}月{int(m.group(2))}日"
    if "月上旬" in remark:
        mo = re.search(r"(\d{1,2})月", remark)
        return f"{int(mo.group(1))}月上旬" if mo else ""
    if "下旬" in remark or "月末" in remark:
        mo = re.search(r"(\d{1,2})月", remark)
        return f"{int(mo.group(1))}月下旬" if mo else ""

    # 5) 仅从下一次预计召开时间取月份
    m = re.search(r"(\d{1,2})月", next_str)
    if m:
        return f"{int(m.group(1))}月"
    return ""


def run():
    """合并显著行业+月份 与 历届统计，并按月份拆分成多张表。"""
    if not PATH_MONTH.exists():
        print(f"未找到按月份分类的显著行业表：{PATH_MONTH}")
        return
    if not PATH_STATS.exists():
        print(f"未找到历届平均涨跌幅表：{PATH_STATS}")
        return

    # 1) 读取两张表
    # 使用 utf-8-sig 兼容从 Excel 导出的 CSV（带 BOM）
    df_month = pd.read_csv(PATH_MONTH, encoding="utf-8-sig")
    df_stats = pd.read_csv(PATH_STATS, encoding="utf-8-sig")

    key = ["meeting_family", "window", "industry"]
    for col in key:
        if col not in df_month.columns:
            print(f"按月份分类表缺少列：{col}")
            return
        if col not in df_stats.columns:
            print(f"历届平均涨跌幅表缺少列：{col}")
            return

    if "下一次月份" not in df_month.columns:
        print("按月份分类表中缺少列「下一次月份」，请先运行：受影响显著一级行业_按月份分类_中信30.py")
        return

    # 2) inner join：只保留两边都出现的 (meeting_family, window, industry)
    df_merged = pd.merge(
        df_month,
        df_stats,
        on=key,
        how="inner",
        suffixes=("", "_stats"),
    )
    if df_merged.empty:
        print("合并后无任何记录，请检查两张表的主键是否一致。")
        return

    # 3) 解析「下一次召开_具体日期」（从下一次预计召开时间+预测备注）
    if "下一次预计召开时间" not in df_merged.columns or "预测备注" not in df_merged.columns:
        print("合并表中缺少「下一次预计召开时间」或「预测备注」，无法解析下一次召开具体日期。")
        return
    df_merged["下一次召开_具体日期"] = df_merged.apply(next_meeting_md, axis=1)

    cols_wanted = [
        "下一次月份",
        "meeting_family",
        "下一次召开_具体日期",
        "window",
        "industry",
        "届数",
        "上涨次数",
        "下跌次数",
        "持平次数",
        "上涨概率",
        "下跌概率",
        "持平概率",
        "平均涨跌幅",
    ]
    missing = [c for c in cols_wanted if c not in df_merged.columns]
    if missing:
        print(f"合并结果中缺少以下列，无法生成月度表：{missing}")
        return

    df_base = df_merged[cols_wanted].copy()

    # 为了排序方便，确保月份是数值型（float/int 都可）
    # 如果有空值会保留为 NaN，在后面拆分时会自然被跳过
    try:
        df_base["下一次月份"] = pd.to_numeric(df_base["下一次月份"], errors="coerce")
    except Exception:
        pass

    # 4) 按月份拆分并输出
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    total_files = 0
    total_rows = 0

    for m in range(1, 13):
        df_m = df_base[df_base["下一次月份"] == m].copy()
        if df_m.empty:
            continue

        # 排序：先按会议族群，再按窗口、行业，便于阅读
        df_m = df_m.sort_values(["meeting_family", "window", "industry"]).reset_index(drop=True)

        fname = f"受影响显著一级行业_{YEAR}M{m:02d}_中信30.csv"
        path_out = OUT_DIR / fname
        df_m.to_csv(path_out, index=False, encoding="utf-8-sig")

        total_files += 1
        total_rows += len(df_m)
        print(f"已写入 {path_out}，月份={m}，共 {len(df_m)} 行。")

    if total_files == 0:
        print("没有任何月份产生数据，请检查「下一次月份」列是否正确。")
    else:
        print(f"共生成 {total_files} 个按月份拆分的显著行业表，总行数 {total_rows}。")


if __name__ == "__main__":
    run()

