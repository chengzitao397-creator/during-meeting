# -*- coding: utf-8 -*-
"""
基于「受影响显著的一级行业_中信30_最终版」表，
筛选出下一次预计召开时间在 2026 年 3 月或 4 月的会议，产出一份统计 Excel。
"""
from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
# 数据源：用 CSV（含「下一次预计召开时间」列）做筛选
PATH_CSV = BASE / "output" / "受影响显著的一级行业_中信30_最终版.csv"
PATH_OUT = BASE / "output" / "26年三四月召开会议_显著行业筛选.xlsx"

# 输出时去掉的长日期列，避免表太乱
DROP_COLS = ["涉及的T日", "窗口开始", "窗口结束"]

# 下一次预计召开时间列中，匹配 2026 年 3 月或 4 月的模式（含“4月下旬”“一季度约3月末”等）
COL_NEXT = "下一次预计召开时间"
PATTERNS_26_03_04 = [
    "2026年3月", "2026年4月", "2026-03", "2026-04",
    "2026年4月下旬", "2026年一季度", "约3月末",
]


def _is_2026_march_april(s: str) -> bool:
    """判断该单元格是否表示 2026 年 3 月或 4 月召开。"""
    if pd.isna(s) or not isinstance(s, str):
        return False
    s = s.strip()
    return any(p in s for p in PATTERNS_26_03_04)


def run():
    if not PATH_CSV.exists():
        print(f"未找到 {PATH_CSV}")
        return

    df = pd.read_csv(PATH_CSV, encoding="utf-8-sig")
    if COL_NEXT not in df.columns:
        print(f"表中无列「{COL_NEXT}」")
        return

    # 筛选：下一次预计召开时间 为 2026 年 3 月或 4 月
    mask = df[COL_NEXT].apply(_is_2026_march_april)
    out = df.loc[mask].copy()

    # 按会议、窗口排序便于阅读
    out = out.sort_values(["meeting_family", "window", "industry"]).reset_index(drop=True)
    # 去掉涉及T日、窗口开始、窗口结束，表更简洁
    out = out.drop(columns=[c for c in DROP_COLS if c in out.columns], errors="ignore")

    with pd.ExcelWriter(PATH_OUT, engine="openpyxl") as w:
        out.to_excel(w, sheet_name="26年3-4月召开", index=False)

    print(f"已写入 {PATH_OUT}")
    print(f"  - 共 {len(out)} 行（筛选条件：下一次预计召开时间为 2026 年 3 月或 4 月）")
    if len(out) > 0:
        meetings = out["meeting_family"].unique().tolist()
        print(f"  - 涉及会议：{meetings}")


if __name__ == "__main__":
    run()
