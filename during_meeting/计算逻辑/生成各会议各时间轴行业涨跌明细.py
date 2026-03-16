# -*- coding: utf-8 -*-
"""
生成「更细分」表格：具体到每一个会议、每一个时间轴、每一个一级行业的涨跌明细。

输出：会议窗口数据/各会议各时间轴一级行业涨跌明细.csv
  - 每行 = 一个会议 + 一个时间轴(T-5/T-1/T/T+1/T+5) + 一个一级行业
  - 列：会议名称, T日, 窗口, 窗口开始, 窗口结束, 市场涨跌幅, 上证指数涨跌幅, 行业名称, 涨跌幅, 涨跌, 差值
  - 涨跌：根据涨跌幅填「涨」/「跌」/「平」，缺失或无效为「平」
"""

from pathlib import Path
import csv


def _parse_ret(s: str):
    """解析涨跌幅，无效返回 None。"""
    if not s or not str(s).strip():
        return None
    try:
        return float(s.strip())
    except ValueError:
        return None


def _ret_to_label(ret) -> str:
    if ret is None:
        return "平"
    if ret > 0:
        return "涨"
    if ret < 0:
        return "跌"
    return "平"


def run(suffix=""):
    """
    suffix: 可选。若为 "_相对开盘价"，则读 窗口_*_相对开盘价.csv，写 各会议各时间轴一级行业涨跌明细_相对开盘价.csv。
    """
    base = Path(__file__).resolve().parent.parent
    data_dir = base / "会议窗口数据"
    windows = ["T-5", "T-1", "T", "T+1", "T+5"]
    out_path = data_dir / f"各会议各时间轴一级行业涨跌明细{suffix}.csv"

    fieldnames = [
        "会议名称", "T日", "窗口", "窗口开始", "窗口结束",
        "市场涨跌幅", "上证指数涨跌幅", "行业名称", "涨跌幅", "涨跌", "差值",
    ]
    rows = []
    for win in windows:
        path = data_dir / f"窗口_{win}{suffix}.csv"
        if not path.exists():
            continue
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                meeting = (row.get("会议名称") or "").strip()
                industry = (row.get("行业名称") or "").strip()
                if not meeting or not industry:
                    continue
                ret = _parse_ret(row.get("涨跌幅"))
                rows.append({
                    "会议名称": meeting,
                    "T日": (row.get("T日") or "").strip()[:10],
                    "窗口": win,
                    "窗口开始": (row.get("窗口开始") or "").strip(),
                    "窗口结束": (row.get("窗口结束") or "").strip(),
                    "市场涨跌幅": (row.get("市场涨跌幅") or "").strip(),
                    "上证指数涨跌幅": (row.get("上证指数涨跌幅") or "").strip(),
                    "行业名称": industry,
                    "涨跌幅": row.get("涨跌幅", "").strip() or "",
                    "涨跌": _ret_to_label(ret),
                    "差值": (row.get("差值") or "").strip(),
                })

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    n_meetings = len(set((r["会议名称"], r["T日"]) for r in rows))
    print(f"已写入 {out_path}，共 {len(rows)} 行。")
    print(f"涉及 {n_meetings} 个会议（会议×T日），5 个时间轴，按 会议+时间轴+行业 细分。")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and (sys.argv[1] or "").strip() == "相对开盘价":
        run("_相对开盘价")
    else:
        run()
