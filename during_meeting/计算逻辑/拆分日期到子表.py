# -*- coding: utf-8 -*-
"""
将「受影响显著的一级行业_中信30_最终版」中涉及大量日期的三列移到子表「日期」，
主表保留其余列，产出一份 Excel（.xlsx）包含两张表：主表 + 日期。
"""
from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
PATH_CSV = BASE / "output" / "受影响显著的一级行业_中信30_最终版.csv"
PATH_XLSX = BASE / "output" / "受影响显著的一级行业_中信30_最终版.xlsx"

# 只移到子表、主表不保留的列（长日期列表，不便在主表展示）
DATE_COLS_TO_SUB_ONLY = ["涉及的T日", "窗口开始", "窗口结束"]
# 主表和子表都保留的日期列（主表便于查看，子表做完整日期备份）
DATE_COLS_BOTH = ["最近一届时间", "下一次预计召开时间"]


def run():
    if not PATH_CSV.exists():
        print(f"未找到 {PATH_CSV}")
        return

    df = pd.read_csv(PATH_CSV, encoding="utf-8-sig")

    # 主表：只去掉长日期三列，保留 最近一届时间、下一次预计召开时间
    main_cols = [c for c in df.columns if c not in DATE_COLS_TO_SUB_ONLY]
    df_main = df[main_cols].copy()

    # 子表「日期」：键列 + 长日期三列 + 最近一届/下一次预计（与主表行序一致）
    key_cols = ["industry", "window", "meeting_family"]
    sub_date_cols = [c for c in DATE_COLS_TO_SUB_ONLY if c in df.columns]
    extra_cols = [c for c in DATE_COLS_BOTH if c in df.columns]
    sub_cols = key_cols + sub_date_cols + extra_cols
    df_date = df[sub_cols].copy()

    with pd.ExcelWriter(PATH_XLSX, engine="openpyxl") as w:
        df_main.to_excel(w, sheet_name="受影响显著的一级行业", index=False)
        df_date.to_excel(w, sheet_name="日期", index=False)

    print(f"已写入 {PATH_XLSX}")
    print("  - 表「受影响显著的一级行业」：主表保留 最近一届时间、下一次预计召开时间，仅去掉 涉及的T日、窗口开始、窗口结束")
    print("  - 表「日期」：industry, window, meeting_family + 上述日期列（与主表行序一致）")


if __name__ == "__main__":
    run()
