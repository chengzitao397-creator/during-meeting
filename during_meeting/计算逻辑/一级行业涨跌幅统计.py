# -*- coding: utf-8 -*-
"""
对一级行业涨跌幅做汇总统计（按行业维度）：
  - 第一部分：收盘口径（来自 各会议各时间轴一级行业涨跌明细.csv 的「涨跌幅」列）
  - 第二部分：相对开盘价口径（来自 各会议各时间轴一级行业涨跌明细_相对开盘价.csv，若存在）

每个口径按「行业名称」聚合，输出：平均涨跌幅、最大涨跌幅、最小涨跌幅、样本数。
"""

import csv
from pathlib import Path


def _parse_ret(s):
    """解析涨跌幅字符串，无效返回 None。"""
    if not s or not str(s).strip():
        return None
    try:
        return float(s.strip())
    except ValueError:
        return None


def _agg_by_industry(rows, ret_key="涨跌幅"):
    """
    按行业名称聚合，对 ret_key 列计算 平均、最大、最小、样本数。
    返回 [(行业名称, 平均涨跌幅, 最大涨跌幅, 最小涨跌幅, 样本数), ...]
    """
    from collections import defaultdict
    by_ind = defaultdict(list)
    for r in rows:
        ind = (r.get("行业名称") or "").strip()
        if not ind:
            continue
        ret = _parse_ret(r.get(ret_key))
        if ret is None:
            continue
        by_ind[ind].append(ret)
    out = []
    for ind, vals in sorted(by_ind.items()):
        if not vals:
            continue
        out.append((
            ind,
            sum(vals) / len(vals),
            max(vals),
            min(vals),
            len(vals),
        ))
    return out


def run():
    base = Path(__file__).resolve().parent.parent
    data_dir = base / "会议窗口数据"

    # ----- 第一部分：收盘口径 -----
    path_close = data_dir / "各会议各时间轴一级行业涨跌明细.csv"
    if not path_close.exists():
        print(f"未找到 {path_close.name}，请先运行生成各会议各时间轴行业涨跌明细。")
        return
    rows_close = []
    with open(path_close, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_close = list(reader)
    stats_close = _agg_by_industry(rows_close, ret_key="涨跌幅")
    out_close = data_dir / "一级行业涨跌幅统计_收盘口径.csv"
    with open(out_close, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["行业名称", "平均涨跌幅", "最大涨跌幅", "最小涨跌幅", "样本数"])
        for ind, avg, mx, mn, cnt in stats_close:
            w.writerow([ind, round(avg, 6), round(mx, 6), round(mn, 6), cnt])
    print(f"已写入 {out_close.name}，共 {len(stats_close)} 个行业（收盘口径）。")

    # ----- 第二部分：相对开盘价口径（若存在明细则统计） -----
    path_open = data_dir / "各会议各时间轴一级行业涨跌明细_相对开盘价.csv"
    if not path_open.exists():
        print("未找到 各会议各时间轴一级行业涨跌明细_相对开盘价.csv，跳过相对开盘价口径统计；请先运行相对开盘价脚本生成该明细。")
        return
    rows_open = []
    with open(path_open, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_open = list(reader)
    # 相对开盘价明细中涨跌幅列名仍为「涨跌幅」
    stats_open = _agg_by_industry(rows_open, ret_key="涨跌幅")
    out_open = data_dir / "一级行业涨跌幅统计_相对开盘价.csv"
    with open(out_open, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["行业名称", "平均涨跌幅", "最大涨跌幅", "最小涨跌幅", "样本数"])
        for ind, avg, mx, mn, cnt in stats_open:
            w.writerow([ind, round(avg, 6), round(mx, 6), round(mn, 6), cnt])
    print(f"已写入 {out_open.name}，共 {len(stats_open)} 个行业（相对开盘价口径）。")


if __name__ == "__main__":
    run()
