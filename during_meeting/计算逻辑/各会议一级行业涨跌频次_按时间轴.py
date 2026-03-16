# -*- coding: utf-8 -*-
"""
按「会议」粒度的一级行业涨跌次数表（第一列是会议名称）。

基于明细表 `会议窗口数据/各会议各时间轴一级行业涨跌明细.csv`，对每一条
  (会议名称, T日, 窗口, 行业名称, 涨跌)
  生成 0/1 形式的涨跌次数：
    - 若 涨跌 == '涨' : up_count = 1, down_count = 0, zero_count = 0
    - 若 涨跌 == '跌' : up_count = 0, down_count = 1, zero_count = 0
    - 否则（平或缺失）: up_count = 0, down_count = 0, zero_count = 1

输出表：`会议窗口数据/各会议一级行业涨跌频次_按时间轴.csv`
  - 每行 = 一个会议 + 一个时间轴 + 一个行业
  - 列顺序：
      会议名称, T日, 窗口, 行业名称,
      up_count, down_count, zero_count

后续你可以：
  - 直接在 Excel 里按「会议名称」透视
  - 或再按会议类型归类做更高一层的统计
"""

from pathlib import Path
import csv


def run():
    base = Path(__file__).resolve().parent.parent
    src = base / "会议窗口数据" / "各会议各时间轴一级行业涨跌明细.csv"
    dst = base / "会议窗口数据" / "各会议一级行业涨跌频次_按时间轴.csv"

    if not src.exists():
        print(f"未找到明细文件: {src}")
        return

    rows_out = []
    with open(src, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            meeting = (row.get("会议名称") or "").strip()
            t_day = (row.get("T日") or "").strip()[:10]
            window = (row.get("窗口") or "").strip()
            industry = (row.get("行业名称") or "").strip()
            label = (row.get("涨跌") or "").strip()
            if not meeting or not window or not industry:
                continue

            if label == "涨":
                up, down, zero = 1, 0, 0
            elif label == "跌":
                up, down, zero = 0, 1, 0
            else:
                up, down, zero = 0, 0, 1

            rows_out.append(
                {
                    "会议名称": meeting,
                    "T日": t_day,
                    "窗口": window,
                    "行业名称": industry,
                    "up_count": up,
                    "down_count": down,
                    "zero_count": zero,
                }
            )

    fieldnames = [
        "会议名称",
        "T日",
        "窗口",
        "行业名称",
        "up_count",
        "down_count",
        "zero_count",
    ]

    with open(dst, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"已写入 {dst}，共 {len(rows_out)} 行（会议×时间轴×行业）。")


if __name__ == "__main__":
    run()

