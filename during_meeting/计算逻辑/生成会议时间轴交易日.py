# -*- coding: utf-8 -*-
"""
从「会议窗口行业数据」解析出「以确定交易日为主」的时间轴表，供另一套环境按日跑龙头股使用。

输入：时间/会议窗口行业数据.csv（含 会议名称、T日、窗口、窗口开始、窗口结束）
输出：时间/会议时间轴交易日.csv
  - trade_date：该时间轴对应的唯一交易日（跑龙头股即用此日快照）
  - meeting_name：会议名称
  - window：T-5 / T-1 / T / T+1 / T+5
  - meeting_t_date：会议 T 日（召开首日），便于核对

规则：T-5/T-1/T/T+1 取「窗口开始」为 trade_date；T+5 取「窗口结束」为 trade_date（T+5 日当天）。
"""

from pathlib import Path
import csv


def run():
    base = Path(__file__).resolve().parent.parent
    path_in = base / "时间" / "会议窗口行业数据.csv"
    path_out = base / "时间" / "会议时间轴交易日.csv"

    if not path_in.exists():
        print(f"未找到输入文件: {path_in}")
        return

    rows_out = []
    with open(path_in, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            meeting_name = (row.get("会议名称") or "").strip()
            meeting_t = (row.get("T日") or "").strip()[:10]
            window = (row.get("窗口") or "").strip()
            start_str = (row.get("窗口开始") or "").strip()[:10]
            end_str = (row.get("窗口结束") or "").strip()[:10]
            if not meeting_name or not window or len(start_str) != 10:
                continue
            # T+5 为区间，取窗口结束日作为该时间轴的交易日；其余取窗口开始日
            trade_date = end_str if window == "T+5" else start_str
            rows_out.append({
                "trade_date": trade_date,
                "meeting_name": meeting_name,
                "window": window,
                "meeting_t_date": meeting_t,
            })

    if not rows_out:
        print("未解析到任何行")
        return

    with open(path_out, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["trade_date", "meeting_name", "window", "meeting_t_date"],
        )
        writer.writeheader()
        writer.writerows(rows_out)

    # 去重后的交易日数量（同一日可能对应多会议多窗口）
    trade_dates_unique = len({r["trade_date"] for r in rows_out})
    print(f"已写入 {path_out}，共 {len(rows_out)} 行（会议×时间轴）。")
    print(f"涉及 {trade_dates_unique} 个不重复交易日。")
    print("说明：将该 CSV 丢到另一套环境，按 trade_date 逐日跑龙头股，再按 meeting_name + window 汇总即得各会议各时间轴龙头股。")


if __name__ == "__main__":
    run()
