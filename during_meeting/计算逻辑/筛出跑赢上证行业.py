# -*- coding: utf-8 -*-
"""
以各窗口表（窗口_T-5.csv 等）为本，筛出「相对上证指数跑赢」的行业：
差值 = 行业涨跌幅 - 上证指数涨跌幅 > 0，即比指数涨得更多、或跌得更少。
按差值降序排列，新增列「排名」（每组会议+窗口内按差值从高到低 1,2,3...），组间插空行。
结果写入 会议窗口数据/各窗口跑赢上证行业.csv。
"""
import csv
from pathlib import Path
from itertools import groupby


def run():
    base = Path(__file__).resolve().parent.parent
    dir_data = base / "会议窗口数据"
    windows = ["T-5", "T-1", "T", "T+1", "T+5"]
    out_rows = []
    out_fields = [
        "会议名称", "T日", "窗口", "窗口开始", "窗口结束",
        "市场涨跌幅", "上证指数涨跌幅", "行业名称", "涨跌幅", "差值", "排名",
    ]

    for w in windows:
        path_csv = dir_data / f"窗口_{w}.csv"
        if not path_csv.exists():
            continue
        with open(path_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not (row.get("行业名称") or "").strip():
                    continue
                diff_str = (row.get("差值") or "").strip()
                try:
                    diff = float(diff_str)
                except ValueError:
                    continue
                if diff <= 0:
                    continue
                out_rows.append({
                    "会议名称": (row.get("会议名称") or "").strip(),
                    "T日": (row.get("T日") or "").strip(),
                    "窗口": w,
                    "窗口开始": (row.get("窗口开始") or "").strip(),
                    "窗口结束": (row.get("窗口结束") or "").strip(),
                    "市场涨跌幅": row.get("市场涨跌幅", ""),
                    "上证指数涨跌幅": row.get("上证指数涨跌幅", ""),
                    "行业名称": (row.get("行业名称") or "").strip(),
                    "涨跌幅": row.get("涨跌幅", ""),
                    "差值": round(diff, 6),
                    "排名": "",  # 稍后按组填写
                })

    # 按（会议、窗口）分组，组内按差值降序排序并填写排名，组间插空行
    empty_row = {k: "" for k in out_fields}
    rows_to_write = []
    for key, group in groupby(out_rows, key=lambda r: (r["会议名称"], r["T日"], r["窗口"])):
        group_list = sorted(list(group), key=lambda r: r["差值"], reverse=True)
        for rank, r in enumerate(group_list, start=1):
            r["排名"] = rank
            rows_to_write.append(r)
        rows_to_write.append(empty_row)
    if rows_to_write and rows_to_write[-1] == empty_row:
        rows_to_write.pop()  # 最后一组后不插空行

    path_out = dir_data / "各窗口跑赢上证行业.csv"
    with open(path_out, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(rows_to_write)
    print(f"已写入 {path_out}，共 {len(out_rows)} 条（相对上证跑赢，差值>0），组间已插空行。")


if __name__ == "__main__":
    run()
