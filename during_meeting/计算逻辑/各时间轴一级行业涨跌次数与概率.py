# -*- coding: utf-8 -*-
"""
会议期间，各时间轴（T-5/T-1/T/T+1/T+5）下每个一级行业的涨跌次数与涨跌概率。

研究目标：每个时间轴下，每个一级行业在「历次会议」中上涨/下跌的次数及概率（更有研究价值）。

输入：会议窗口数据/窗口_T-5.csv、窗口_T-1.csv、窗口_T.csv、窗口_T+1.csv、窗口_T+5.csv
     每行：会议名称、T日、窗口、行业名称、涨跌幅 等
输出：会议窗口数据/各时间轴一级行业涨跌次数与概率.csv
     列：窗口、行业名称、上涨次数、下跌次数、平次数、样本数、上涨概率、下跌概率、平概率
"""

from pathlib import Path
import csv


def _parse_ret(s: str):
    """将涨跌幅字符串转为 float，无效则返回 None（不计入涨也不计入跌）。"""
    if not s or not str(s).strip():
        return None
    try:
        return float(s.strip())
    except ValueError:
        return None


def run():
    base = Path(__file__).resolve().parent.parent
    data_dir = base / "会议窗口数据"
    windows = ["T-5", "T-1", "T", "T+1", "T+5"]
    out_path = data_dir / "各时间轴一级行业涨跌次数与概率.csv"

    # 按 (窗口, 行业名称) 汇总：上涨次数、下跌次数、平次数
    # 结构: (window, industry) -> {"up": n, "down": n, "flat": n}
    agg = {}
    for win in windows:
        path = data_dir / f"窗口_{win}.csv"
        if not path.exists():
            print(f"跳过不存在的文件: {path}")
            continue
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                industry = (row.get("行业名称") or "").strip()
                if not industry:
                    continue
                ret = _parse_ret(row.get("涨跌幅"))
                key = (win, industry)
                if key not in agg:
                    agg[key] = {"up": 0, "down": 0, "flat": 0}
                if ret is None:
                    continue
                if ret > 0:
                    agg[key]["up"] += 1
                elif ret < 0:
                    agg[key]["down"] += 1
                else:
                    agg[key]["flat"] += 1

    # 每个窗口下的会议数（用于算概率时分母）：同一窗口下 (会议名称, T日) 去重
    meeting_count_by_window = {}
    for win in windows:
        path = data_dir / f"窗口_{win}.csv"
        if not path.exists():
            continue
        meetings = set()
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = (row.get("会议名称") or "").strip()
                t = (row.get("T日") or "").strip()[:10]
                if name and t:
                    meetings.add((name, t))
        meeting_count_by_window[win] = len(meetings)

    # 写出：窗口、行业名称、上涨次数、下跌次数、平次数、样本数、上涨概率、下跌概率、平概率
    rows = []
    for (win, industry), cnt in sorted(agg.items(), key=lambda x: (x[0][0], x[0][1])):
        n_up = cnt["up"]
        n_down = cnt["down"]
        n_flat = cnt["flat"]
        n_total = n_up + n_down + n_flat
        sample = meeting_count_by_window.get(win, n_total)
        p_up = round(n_up / sample, 4) if sample else 0
        p_down = round(n_down / sample, 4) if sample else 0
        p_flat = round(n_flat / sample, 4) if sample else 0
        rows.append({
            "窗口": win,
            "行业名称": industry,
            "上涨次数": n_up,
            "下跌次数": n_down,
            "平次数": n_flat,
            "样本数": sample,
            "上涨概率": p_up,
            "下跌概率": p_down,
            "平概率": p_flat,
        })

    fieldnames = ["窗口", "行业名称", "上涨次数", "下跌次数", "平次数", "样本数", "上涨概率", "下跌概率", "平概率"]
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"已写入 {out_path}，共 {len(rows)} 行。")
    print("说明：样本数 = 该时间轴下的会议次数；上涨/下跌/平概率 = 对应次数 / 样本数。")


if __name__ == "__main__":
    run()
