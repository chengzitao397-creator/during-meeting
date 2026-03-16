# -*- coding: utf-8 -*-
"""
在「受影响显著的一级行业_中信30_最终版」的基础上，做一个简单的「按月份分类」标记。

- 输入：output/受影响显著的一级行业_中信30_最终版.csv
- 处理：
  - 从「下一次预计召开时间」中尽量提取月份（1-12），得到一列「下一次月份」；
  - 仅做粗粒度分类，主要用于后续按月份筛选、分配权重。
- 输出：output/受影响显著的一级行业_中信30_按月份分类.csv
  - 原表全部列 + 新增一列「下一次月份」（形如 1, 2, ..., 12）
  - 按「下一次月份, meeting_family, window, industry」排序，方便人工查看和后续处理。
"""
from pathlib import Path
import re
import pandas as pd


BASE = Path(__file__).resolve().parent.parent
PATH_SRC = BASE / "output" / "受影响显著的一级行业_中信30_最终版.csv"
PATH_OUT = BASE / "output" / "受影响显著的一级行业_中信30_按月份分类.csv"

COL_NEXT = "下一次预计召开时间"
COL_MONTH = "下一次月份"


def extract_month(cell) -> float | None:
    """
    从「下一次预计召开时间」的中文描述里，尽量提取月份（1-12）。

    优先规则（从上到下）：
    1. 匹配形如 “2026年3月” / “2026年03月” / “2026-03-xx” 这类带“月”的形式；
    2. 若字符串中没有“月”字，但有 “-03” / “-11” 这类，则用连字符后的两位；
    3. 若完全匹配不到，则返回 None。

    说明：
    - 这里的目标只是「按月份粗分类」，不追求所有描述都精确命中；
    - 类似 “2026年一季度”“上半年” 这类不会被识别到月份，将得到空值。
    """
    if pd.isna(cell):
        return None
    if not isinstance(cell, str):
        cell = str(cell)
    s = cell.strip()
    if not s:
        return None

    # 1) 先找 “X月” 模式：例如 “3月”“11月”“2026年3月”“每年11月乌镇”
    m = re.search(r"(\d{1,2})月", s)
    if m:
        try:
            month = int(m.group(1))
            if 1 <= month <= 12:
                return float(month)
        except ValueError:
            pass

    # 2) 再尝试 “-MM” 模式：例如 “2026-03”“2026-11-07”
    m = re.search(r"-(\d{2})", s)
    if m:
        try:
            month = int(m.group(1))
            if 1 <= month <= 12:
                return float(month)
        except ValueError:
            pass

    return None


def run():
    if not PATH_SRC.exists():
        print(f"未找到源表：{PATH_SRC}")
        return

    # 用 utf-8-sig 兼容从 Excel 导出的 CSV（带 BOM）
    df = pd.read_csv(PATH_SRC, encoding="utf-8-sig")
    if COL_NEXT not in df.columns:
        print(f"源表中不存在列「{COL_NEXT}」，无法按月份分类。")
        return

    # 新增一列：下一次月份（1-12），部分描述可能提取不到，结果为 NaN
    df[COL_MONTH] = df[COL_NEXT].apply(extract_month)

    # 为了方便筛选，可以把月份列放到靠后或靠前；这里直接放在原列后（pandas 默认行为即可）
    # 排序：先按月份，再按会议族群、窗口、行业，观感更好
    sort_cols = [COL_MONTH]
    for col in ["meeting_family", "window", "industry"]:
        if col in df.columns:
            sort_cols.append(col)
    df = df.sort_values(sort_cols, na_position="last").reset_index(drop=True)

    PATH_OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(PATH_OUT, index=False, encoding="utf-8-sig")

    total = len(df)
    with_month = df[COL_MONTH].notna().sum()
    print(f"已写入 {PATH_OUT}")
    print(f"  - 总行数：{total}")
    print(f"  - 成功识别月份的行数：{with_month}，约占 {with_month / max(total, 1):.1%}")


if __name__ == "__main__":
    run()

