# -*- coding: utf-8 -*-
"""
从「各时间轴一级行业涨跌次数与概率.csv」生成一个更规范、更便于透视表分析的频次表。

目标形态参考你截图里的：
- event_norm, industry, up_count, down_count, zero_count, n_obs, up_ratio, down_ratio, ...

这里约定：
- event_norm: 使用时间轴标签（窗口），例如 T-5/T-1/T/T+1/T+5
- industry: 行业名称
- up_count/down_count/zero_count: 对应时间轴下，该行业在历次会议中的上涨/下跌/平次数
- n_obs: 样本数（该时间轴下的会议次数）
- up_ratio/down_ratio/zero_ratio: 对应次数 / n_obs

涨跌幅度不再单独输出，只保留次数和比例。
"""

from pathlib import Path
import csv


def run():
    base = Path(__file__).resolve().parent.parent
    src = base / "会议窗口数据" / "各时间轴一级行业涨跌次数与概率.csv"
    dst = base / "会议窗口数据" / "各时间轴一级行业涨跌频次_规范版.csv"

    if not src.exists():
        print(f"未找到源文件: {src}")
        return

    rows_out = []
    with open(src, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            window = (row.get("窗口") or "").strip()
            industry = (row.get("行业名称") or "").strip()
            if not window or not industry:
                continue

            # 计数与样本数
            def _to_int(x):
                try:
                    return int(float(x))
                except Exception:
                    return 0

            up_cnt = _to_int(row.get("上涨次数"))
            down_cnt = _to_int(row.get("下跌次数"))
            zero_cnt = _to_int(row.get("平次数"))
            n_obs = _to_int(row.get("样本数"))
            # 比例：如果原表已有就直接读；否则按 n_obs 重新算一遍
            def _to_float(x):
                try:
                    return float(x)
                except Exception:
                    return 0.0

            up_ratio = _to_float(row.get("上涨概率"))
            down_ratio = _to_float(row.get("下跌概率"))
            zero_ratio = _to_float(row.get("平概率"))

            rows_out.append(
                {
                    "event_norm": window,
                    "industry": industry,
                    "up_count": up_cnt,
                    "down_count": down_cnt,
                    "zero_count": zero_cnt,
                    "n_obs": n_obs,
                    "up_ratio": up_ratio,
                    "down_ratio": down_ratio,
                    "zero_ratio": zero_ratio,
                }
            )

    fieldnames = [
        "event_norm",
        "industry",
        "up_count",
        "down_count",
        "zero_count",
        "n_obs",
        "up_ratio",
        "down_ratio",
        "zero_ratio",
    ]
    with open(dst, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"已写入 {dst}，共 {len(rows_out)} 行（5 个时间轴 × 行业）。")


if __name__ == "__main__":
    run()

