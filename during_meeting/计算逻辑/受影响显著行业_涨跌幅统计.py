# -*- coding: utf-8 -*-
"""
在「受影响显著的一级行业」表基础上，为每一行（每个 行业+时间轴+会议族）追加 6 列涨跌幅：

  - 收盘口径 3 列：平均涨跌幅_收盘、最大涨跌幅_收盘、最小涨跌幅_收盘
  - 相对开盘价口径 3 列：平均涨跌幅_开盘、最大涨跌幅_开盘、最小涨跌幅_开盘

数据来源：各会议各时间轴一级行业涨跌明细（及相对开盘价明细），按 (行业, 窗口, 会议族) 聚合
该组合下历届会议的涨跌幅，算平均/最大/最小。输出为同一张表追加列，写至 受影响显著的一级行业_最终版.csv。
"""

import csv
import re
from pathlib import Path
from collections import defaultdict


def normalize_meeting_family(name: str) -> str:
    """将具体届次的会议名称归一为「会议族群名称」（与会议族群_各时间轴行业涨跌统计一致）。"""
    name = (name or "").strip()
    if not name:
        return ""
    if "（" in name:
        name = name.split("（", 1)[0].strip()
    m = re.match(r"^(\d{4})年(.+)$", name)
    if m:
        name = m.group(2).strip()
    family_keywords = [
        ("中国共产党", "全国代表大会", "中国共产党全国代表大会"),
        ("届三中全会", None, "三中全会"),
        ("中央经济工作会议", None, "中央经济工作会议"),
        ("全国金融工作会议", None, "全国金融工作会议"),
        ("中央农村工作会议", None, "中央农村工作会议"),
        ("全国住房城乡建设工作会议", None, "全国住房城乡建设工作会议"),
        ("全国科技创新大会", None, "全国科技创新大会"),
        ("证监会系统工作会议", None, "证监会系统工作会议"),
        ("央行货币政策委员会例会", None, "央行货币政策委员会例会"),
        ("美联储FOMC议息会议", None, "美联储FOMC议息会议"),
        ("冬季达沃斯", None, "冬季达沃斯"),
        ("夏季达沃斯", None, "夏季达沃斯"),
        ("博鳌亚洲论坛", None, "博鳌亚洲论坛"),
        ("中国国际进口博览会", None, "中国国际进口博览会"),
        ("G20领导人峰会", None, "G20领导人峰会"),
        ("一带一路国际合作高峰论坛", None, "一带一路国际合作高峰论坛"),
        ("IMF与世行春季会议", None, "IMF与世行春季会议"),
        ("IMF与世行秋季年会", None, "IMF与世行秋季年会"),
    ]
    for kw1, kw2, fam in family_keywords:
        if kw1 in name and (kw2 is None or kw2 in name):
            name = fam
            break
    return name


def _parse_ret(s):
    """解析涨跌幅字符串，无效返回 None。"""
    if not s or not str(s).strip():
        return None
    try:
        return float(str(s).strip())
    except ValueError:
        return None


def _agg_by_key(rows, ret_key="涨跌幅"):
    """
    按 (行业名称, 窗口, meeting_family) 聚合明细行，对 ret_key 列计算 平均、最大、最小。
    返回 dict: (ind, win, fam) -> (avg, max, min)，仅包含至少有一个有效涨跌幅的组合。
    """
    by_key = defaultdict(list)
    for r in rows:
        ind = (r.get("行业名称") or "").strip()
        win = (r.get("窗口") or "").strip()
        meeting = (r.get("会议名称") or "").strip()
        if not ind or not win:
            continue
        fam = normalize_meeting_family(meeting)
        ret = _parse_ret(r.get(ret_key))
        if ret is None:
            continue
        by_key[(ind, win, fam)].append(ret)
    out = {}
    for key, vals in by_key.items():
        if vals:
            out[key] = (sum(vals) / len(vals), max(vals), min(vals))
    return out


def _agg_dates_by_key(rows):
    """
    按 (行业名称, 窗口, meeting_family) 聚合明细行，收集该组合下涉及的 T日、窗口开始、窗口结束。
    返回 dict: (ind, win, fam) -> (t_dates_str, 窗口开始, 窗口结束)
    t_dates_str 为分号分隔的 T日 列表；窗口开始/结束 取该组合下首次届次（按 T 日排序）的值。
    """
    by_key = defaultdict(list)
    for r in rows:
        ind = (r.get("行业名称") or "").strip()
        win = (r.get("窗口") or "").strip()
        meeting = (r.get("会议名称") or "").strip()
        t_day = (r.get("T日") or "").strip()[:10]
        start = (r.get("窗口开始") or "").strip()
        end = (r.get("窗口结束") or "").strip()
        if not ind or not win:
            continue
        fam = normalize_meeting_family(meeting)
        by_key[(ind, win, fam)].append((t_day, start, end))
    out = {}
    for key, triples in by_key.items():
        if not triples:
            continue
        # 按 T 日排序后去重，保留顺序
        triples.sort(key=lambda x: x[0])
        seen_t = set()
        t_list = []
        for t, s, e in triples:
            if t and t not in seen_t:
                seen_t.add(t)
                t_list.append(t)
        first_start = triples[0][1] if triples else ""
        first_end = triples[0][2] if triples else ""
        t_dates_str = "; ".join(t_list) if t_list else ""
        out[key] = (t_dates_str, first_start, first_end)
    return out


def _resolve_sig_columns(raw_cols):
    """从表头解析 industry / window / meeting_family 列名（兼容 BOM/乱码）。"""
    col_industry = col_window = col_family = None
    for c in raw_cols or []:
        k = (c or "").strip()
        if "industry" in k or k == "industry":
            col_industry = c
        elif k == "window":
            col_window = c
        elif k == "meeting_family":
            col_family = c
    return col_industry, col_window, col_family


def run():
    base = Path(__file__).resolve().parent.parent
    data_dir = base / "会议窗口数据"

    # 读取原表：受影响显著的一级行业（保留所有列与行序）
    path_sig = data_dir / "受影响显著的一级行业.csv"
    if not path_sig.exists():
        print(f"未找到 {path_sig.name}，请先运行 industry_bias_filter 等生成该文件。")
        return
    with open(path_sig, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        sig_fieldnames = list(reader.fieldnames or [])
        sig_rows = list(reader)
    col_ind, col_win, col_fam = _resolve_sig_columns(sig_fieldnames)
    if not col_ind or not col_win or not col_fam:
        raise RuntimeError(f"表头需包含 industry/window/meeting_family，当前: {sig_fieldnames}")
    print(f"已加载原表 {len(sig_rows)} 行。")

    # 收盘口径：按 (行业, 窗口, 会议族) 聚合明细，得到 平均/最大/最小
    path_close = data_dir / "各会议各时间轴一级行业涨跌明细.csv"
    if not path_close.exists():
        print(f"未找到 {path_close.name}，请先运行生成各会议各时间轴行业涨跌明细。")
        return
    with open(path_close, "r", encoding="utf-8") as f:
        rows_close = list(csv.DictReader(f))
    stats_close = _agg_by_key(rows_close, ret_key="涨跌幅")
    # 从同一明细表按 (行业, 窗口, 会议族) 收集涉及的 T 日与窗口日期
    dates_by_key = _agg_dates_by_key(rows_close)

    # 相对开盘价口径（若存在）
    stats_open = None
    path_open = data_dir / "各会议各时间轴一级行业涨跌明细_相对开盘价.csv"
    if path_open.exists():
        with open(path_open, "r", encoding="utf-8") as f:
            rows_open = list(csv.DictReader(f))
        stats_open = _agg_by_key(rows_open, ret_key="涨跌幅")
        print("已加载相对开盘价明细，将追加开盘口径 3 列。")
    else:
        print("未找到 各会议各时间轴一级行业涨跌明细_相对开盘价.csv，开盘口径 3 列留空。")
        print("  若需填满：先运行「会议窗口行业数据_米筐_相对开盘价」→「split_会议窗口按窗口」选相对开盘价 →「生成各会议各时间轴行业涨跌明细」选相对开盘价，再重跑本脚本。")

    # 加载「会议下次召开时间」表，按会议族群匹配后追加预测日期几列
    time_dir = base / "时间"
    path_next = time_dir / "会议下次召开时间.csv"
    next_meeting_map = {}
    if path_next.exists():
        with open(path_next, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                fam = (r.get("会议族群") or "").strip()
                if fam:
                    next_meeting_map[fam] = (
                        (r.get("最近一届时间") or "").strip(),
                        (r.get("下一次预计召开时间") or "").strip(),
                        (r.get("备注") or "").strip(),
                    )
        print("已加载会议下次召开时间，将追加预测日期 3 列。")
    else:
        print("未找到 时间/会议下次召开时间.csv，预测日期 3 列留空（可先运行 统计会议下次召开时间.py）。")

    # 在原表每行后追加：日期 3 列 + 收盘 3 列 + 开盘 3 列 + 预测日期 3 列
    new_cols = [
        "涉及的T日", "窗口开始", "窗口结束",
        "平均涨跌幅_收盘", "最大涨跌幅_收盘", "最小涨跌幅_收盘",
        "平均涨跌幅_开盘", "最大涨跌幅_开盘", "最小涨跌幅_开盘",
        "最近一届时间", "下一次预计召开时间", "预测备注",
    ]
    out_fieldnames = sig_fieldnames + new_cols
    for row in sig_rows:
        ind = (row.get(col_ind) or "").strip()
        win = (row.get(col_win) or "").strip()
        fam = (row.get(col_fam) or "").strip()
        key = (ind, win, fam)
        # 日期：该组合下涉及的 T 日列表、首次窗口开始/结束
        d = dates_by_key.get(key)
        if d:
            row["涉及的T日"], row["窗口开始"], row["窗口结束"] = d[0], d[1], d[2]
        else:
            row["涉及的T日"] = row["窗口开始"] = row["窗口结束"] = ""
        # 收盘口径
        t = stats_close.get(key)
        if t:
            row["平均涨跌幅_收盘"], row["最大涨跌幅_收盘"], row["最小涨跌幅_收盘"] = round(t[0], 6), round(t[1], 6), round(t[2], 6)
        else:
            row["平均涨跌幅_收盘"] = row["最大涨跌幅_收盘"] = row["最小涨跌幅_收盘"] = ""
        # 相对开盘价口径
        if stats_open is not None:
            t = stats_open.get(key)
            if t:
                row["平均涨跌幅_开盘"], row["最大涨跌幅_开盘"], row["最小涨跌幅_开盘"] = round(t[0], 6), round(t[1], 6), round(t[2], 6)
            else:
                row["平均涨跌幅_开盘"] = row["最大涨跌幅_开盘"] = row["最小涨跌幅_开盘"] = ""
        else:
            row["平均涨跌幅_开盘"] = row["最大涨跌幅_开盘"] = row["最小涨跌幅_开盘"] = ""
        # 预测日期：按 meeting_family 匹配会议下次召开时间表
        pred = next_meeting_map.get(fam)
        if pred:
            row["最近一届时间"], row["下一次预计召开时间"], row["预测备注"] = pred[0], pred[1], pred[2]
        else:
            row["最近一届时间"] = row["下一次预计召开时间"] = row["预测备注"] = ""

    out_path = data_dir / "受影响显著的一级行业_最终版.csv"
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(sig_rows)
    print(f"已写入 {out_path.name}，共 {len(sig_rows)} 行，原表 + 日期 3 列 + 涨跌幅 6 列 + 预测日期 3 列。")


if __name__ == "__main__":
    run()
