# -*- coding: utf-8 -*-
"""
将「历届会议名称与时间」中统计的会议合并进会议大全：
会议大全保留原有列，新增「时间」列；历届统计的 55 条追加到表尾，其他列留空。
"""
import csv
from pathlib import Path

import pandas as pd

def main():
    # 项目根目录；会议名称类在「会议名称」文件夹，时间类在「时间」文件夹
    base = Path(__file__).resolve().parent.parent
    dir_name = base / "会议名称"
    dir_time = base / "时间"

    # 1. 读取会议大全（优先用 CSV，列一致）
    with open(dir_name / "会议大全.csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows_daquan = list(reader)
    if not rows_daquan:
        raise SystemExit("会议大全.csv 为空")
    header_daquan = rows_daquan[0]
    data_daquan = rows_daquan[1:]

    # 2. 新增「时间」列（若尚无）
    if "时间" not in header_daquan:
        header_daquan = header_daquan + ["时间"]
        # 原有行时间留空
        data_daquan = [r + [""] for r in data_daquan]

    # 3. 读取历届会议名称与时间
    df_lijie = pd.read_excel(dir_time / "历届会议名称与时间.xlsx", sheet_name=0)
    name_col = "历届会议名称"
    time_col = "时间"
    if name_col not in df_lijie.columns or time_col not in df_lijie.columns:
        raise SystemExit("历届会议名称与时间.xlsx 需包含列：历届会议名称、时间")
    n_cols = len(header_daquan)
    time_idx = header_daquan.index("时间")

    # 4. 构造要追加的行：历届会议名称 -> 中文名称列，时间 -> 时间列，其余为空
    name_idx = header_daquan.index("event_name_cn") if "event_name_cn" in header_daquan else 0
    new_rows = []
    for _, row in df_lijie.iterrows():
        name = row[name_col]
        t = row[time_col]
        if pd.isna(t):
            t = ""
        else:
            t = str(t).strip()
        # 一行：除 event_name_cn 和 时间 外都为空
        arr = [""] * n_cols
        arr[name_idx] = name
        arr[time_idx] = t
        new_rows.append(arr)

    # 5. 合并：会议大全原有行 + 历届统计行
    merged_data = data_daquan + new_rows

    # 6. 写回 会议大全.csv 与 会议大全（无后缀）
    with open(dir_name / "会议大全.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header_daquan)
        w.writerows(merged_data)

    with open(dir_name / "会议大全", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header_daquan)
        w.writerows(merged_data)

    # 7. 更新 会议大全.md（表格含时间列）
    md_lines = [
        "# 会议大全",
        "",
        "含会议大全原有条目（按首次举办年排序）及历届会议名称与时间中的统计会议（附时间）。",
        "",
    ]
    cn_headers = ["中文名称", "英文名称", "频次", "常见月份", "首次年", "届数估计", "政策领域", "A股传导", "备注", "时间"]
    col_count = len(header_daquan)
    md_lines.append("| " + " | ".join(cn_headers[:col_count]) + " |")
    md_lines.append("| " + " | ".join(["---"] * col_count) + " |")
    for r in merged_data:
        cells = [str(c).replace("|", "\\|").strip()[:60] for c in r]
        if len(cells) < col_count:
            cells += [""] * (col_count - len(cells))
        md_lines.append("| " + " | ".join(cells[:col_count]) + " |")

    with open(dir_name / "会议大全.md", "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))

    print(f"已合并：会议大全原有 {len(data_daquan)} 条 + 历届统计 {len(new_rows)} 条 = 共 {len(merged_data)} 条。")
    print("已更新：会议大全、会议大全.csv、会议大全.md")

if __name__ == "__main__":
    main()
