# -*- coding: utf-8 -*-
"""
整理 会议大全 内容：解析 CSV、按首次举办年排序、统一格式并输出。
"""
import csv
import re
from pathlib import Path

def extract_year(s):
    """从 first_year 列提取数字年份，用于排序；无法提取则返回 9999 排到后面。"""
    if not s or not isinstance(s, str):
        return 9999
    m = re.search(r"(\d{4})", s)
    return int(m.group(1)) if m else 9999

def main():
    # 项目根目录；会议大全在「会议名称」文件夹
    base = Path(__file__).resolve().parent.parent
    dir_name = base / "会议名称"
    path_in = dir_name / "会议大全"
    path_csv = dir_name / "会议大全.csv"
    path_md = dir_name / "会议大全.md"

    # 读取原文件（按行，兼容无扩展名）
    with open(path_in, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        print("会议大全是空的，未做整理。")
        return

    header = rows[0]
    data = rows[1:]
    # 按首次举办年升序，同年按中文名称排序
    try:
        idx_year = header.index("first_year")
        idx_name = header.index("event_name_cn")
    except ValueError:
        idx_year = 4 if len(header) > 4 else 0
        idx_name = 0
    data_sorted = sorted(data, key=lambda r: (extract_year(r[idx_year]) if len(r) > idx_year else 9999, (r[idx_name] if len(r) > idx_name else "")))

    # 写回规范 CSV（统一引用含逗号、分号的字段）
    with open(path_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data_sorted)

    # 生成 Markdown 表格，便于阅读
    cn_headers = ["中文名称", "英文名称", "频次", "常见月份", "首次年", "届数估计", "政策领域", "A股传导", "备注"]
    en_to_cn = dict(zip(
        ["event_name_cn", "event_name_en", "frequency_type", "typical_month_window", "first_year", "count_estimate", "policy_domain_tags", "A_share_channel", "notes"],
        cn_headers
    ))
    col_order = [header.index(h) for h in en_to_cn if h in header]
    if len(col_order) != len(header):
        col_order = list(range(len(header)))

    def md_escape(s):
        if s is None:
            return ""
        s = str(s).replace("|", "\\|").strip()
        return s[:80] + "…" if len(s) > 80 else s

    md_lines = [
        "# 会议大全",
        "",
        "按**首次举办年**排序，便于查阅。",
        "",
        "| " + " | ".join(cn_headers[:5]) + " | " + " | ".join(cn_headers[5:]) + " |",
        "| " + " | ".join(["---"] * len(header)) + " |",
    ]
    for row in data_sorted:
        cells = [row[i] if i < len(row) else "" for i in col_order]
        md_lines.append("| " + " | ".join(md_escape(c) for c in cells) + " |")

    with open(path_md, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))

    print(f"已整理：共 {len(data_sorted)} 条会议，按首次举办年排序。")
    print(f"  - 规范 CSV：{path_csv}")
    print(f"  - 表格摘要：{path_md}")

if __name__ == "__main__":
    main()
