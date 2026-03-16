# -*- coding: utf-8 -*-
"""
会议大全去重：
1. 按「中文名称」规范（去首尾空格）后去掉完全重复行，保留第一条。
2. 若存在“类型名”且存在对应“历届具体名”（如 中国共产党全国代表大会（党代会）与 中国共产党第X次全国代表大会），
   则删除类型名那一行，保留带具体时间的历届行。
"""
import csv
import re
from pathlib import Path

import pandas as pd

def main():
    # 项目根目录；会议大全在「会议名称」文件夹
    base = Path(__file__).resolve().parent.parent
    dir_name = base / "会议名称"
    path_csv = dir_name / "会议大全.csv"
    path_raw = dir_name / "会议大全"
    path_md = dir_name / "会议大全.md"

    df = pd.read_csv(path_csv, encoding="utf-8")
    n_before = len(df)

    # 规范中文名称（去首尾空格）
    df["event_name_cn"] = df["event_name_cn"].astype(str).str.strip()

    # 1. 按 event_name_cn 去重，保留第一条
    df = df.drop_duplicates(subset=["event_name_cn"], keep="first").reset_index(drop=True)
    n_after_exact = len(df)

    # 2. 类型名 vs 历届具体名：当存在「中国共产党第X次全国代表大会」时，删除「中国共产党全国代表大会（党代会）」
    generic_dangdai = "中国共产党全国代表大会（党代会）"
    has_specific = df["event_name_cn"].str.match(r"中国共产党第.+次全国代表大会", na=False).any()
    if has_specific and generic_dangdai in df["event_name_cn"].values:
        df = df[df["event_name_cn"] != generic_dangdai].reset_index(drop=True)
    n_after_generic = len(df)

    # 写回 CSV 与 会议大全（无后缀）
    df.to_csv(path_csv, index=False, encoding="utf-8")
    with open(path_raw, "w", encoding="utf-8", newline="") as f:
        df.to_csv(f, index=False, encoding="utf-8")

    # 更新 会议大全.md
    header = df.columns.tolist()
    cn_headers = ["中文名称", "英文名称", "频次", "常见月份", "首次年", "届数估计", "政策领域", "A股传导", "备注", "时间"]
    col_count = len(header)
    md_lines = [
        "# 会议大全",
        "",
        "已去重：按中文名称去重复；类型名与历届具体名二选一保留历届。",
        "",
        "| " + " | ".join(cn_headers[:col_count]) + " |",
        "| " + " | ".join(["---"] * col_count) + " |",
    ]
    for _, row in df.iterrows():
        cells = [str(row[c]).replace("|", "\\|").strip()[:60] for c in header]
        md_lines.append("| " + " | ".join(cells) + " |")
    with open(path_md, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))

    removed = n_before - n_after_generic
    print(f"去重完成：{n_before} 条 → {n_after_generic} 条，共移除 {removed} 条重复。")
    print("已更新：会议大全、会议大全.csv、会议大全.md")

if __name__ == "__main__":
    main()
