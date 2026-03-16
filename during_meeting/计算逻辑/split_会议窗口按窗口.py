# -*- coding: utf-8 -*-
"""
按窗口（T-5、T-1、T、T+1、T+5）拆分会议窗口行业数据，
每个窗口一个 CSV，放入「会议窗口数据」文件夹。
格式与「各窗口上涨行业」一致：一行一行业，列含 会议名称、T日、窗口、行业名称、涨跌幅、差值 等，组间插空行。
"""
import csv
from pathlib import Path


def parse_industry_rets(s):
    """解析 行业涨跌幅 字符串，返回 [(行业名, 涨跌幅), ...]，无效或 nan 的跳过。"""
    if not (s and s.strip()):
        return []
    out = []
    for part in s.split(";"):
        part = part.strip()
        if ":" not in part:
            continue
        name, val = part.split(":", 1)
        name, val = name.strip(), val.strip().lower()
        if not name or val == "nan" or val == "":
            continue
        try:
            out.append((name, float(val)))
        except ValueError:
            continue
    return out


def run(suffix=""):
    """
    suffix: 可选后缀。若为 "_相对开盘价"，则读 会议窗口行业数据_相对开盘价.csv，写 窗口_T-5_相对开盘价.csv 等。
    """
    base = Path(__file__).resolve().parent.parent
    path_src = base / "时间" / f"会议窗口行业数据{suffix}.csv"
    dir_out = base / "会议窗口数据"
    dir_out.mkdir(parents=True, exist_ok=True)

    windows = ["T-5", "T-1", "T", "T+1", "T+5"]
    # 一行一行业，与各窗口上涨行业同结构（含涨跌幅、差值）
    fieldnames = [
        "会议名称", "T日", "窗口", "窗口开始", "窗口结束",
        "市场涨跌幅", "上证指数涨跌幅", "行业名称", "涨跌幅", "差值",
    ]

    with open(path_src, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_all = list(reader)
        if not rows_all:
            print("源文件无数据")
            return

    empty_row = {k: "" for k in fieldnames}
    for w in windows:
        rows_w = [r for r in rows_all if (r.get("窗口") or "").strip() == w]
        rows_to_write = []
        for i, r in enumerate(rows_w):
            meeting = (r.get("会议名称") or "").strip()
            t_day = (r.get("T日") or "").strip()
            win_start = (r.get("窗口开始") or "").strip()
            win_end = (r.get("窗口结束") or "").strip()
            market_ret = r.get("市场涨跌幅", "")
            bench_str = (r.get("上证指数涨跌幅") or "").strip()
            try:
                benchmark_ret = float(bench_str) if bench_str else None
            except ValueError:
                benchmark_ret = None
            for ind_name, ret in parse_industry_rets(r.get("行业涨跌幅", "")):
                diff = round(ret - benchmark_ret, 6) if benchmark_ret is not None else ""
                rows_to_write.append({
                    "会议名称": meeting,
                    "T日": t_day,
                    "窗口": w,
                    "窗口开始": win_start,
                    "窗口结束": win_end,
                    "市场涨跌幅": market_ret,
                    "上证指数涨跌幅": round(benchmark_ret, 6) if benchmark_ret is not None else "",
                    "行业名称": ind_name,
                    "涨跌幅": round(ret, 6),
                    "差值": diff,
                })
            if i < len(rows_w) - 1:
                rows_to_write.append(empty_row)
        path_out = dir_out / f"窗口_{w}{suffix}.csv"
        with open(path_out, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows_to_write)
        print(f"已写 {path_out.name}，{len(rows_w)} 组×多行业（组间已插空行）")

    print(f"已全部写入目录：{dir_out}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and (sys.argv[1] or "").strip() == "相对开盘价":
        run("_相对开盘价")
    else:
        run()
