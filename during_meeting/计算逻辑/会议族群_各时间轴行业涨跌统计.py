# -*- coding: utf-8 -*-
"""
按「会议族群（历届）」统计各时间轴下、各一级行业的涨跌次数。

例：电子行业在「G20领导人峰会」这一类会议的历届表现（所有年份合在一起），
在 T-5/T/T+1 等时间轴下分别有几次涨、几次跌。

输入：会议窗口数据/各会议一级行业涨跌频次_按时间轴.csv
    每行：会议名称, T日, 窗口, 行业名称, up_count(0/1), down_count(0/1), zero_count(0/1)

核心处理：
    - 将具体届次的会议名称归一为「会议族群名称」：
        * 若包含中文全角括号「（」，取括号之前的部分作为族群名。
          如「G20领导人峰会（2022）」→「G20领导人峰会」。
        * 若以“YYYY年”开头（如「2013年中央经济工作会议」），
          去掉年份前缀，保留后面的会议名「中央经济工作会议」。
    - 在 (会议族群, 窗口, 行业) 维度上，对 up_count / down_count / zero_count 求和。
    - 统计该维度下涉及的历届次数 n_sessions（不同届次的数量）。

输出：会议窗口数据/会议族群_各时间轴行业涨跌统计.csv
    列：
        meeting_family  会议族群名称（不含年份，如 G20领导人峰会）
        window          时间轴（T-5/T-1/T/T+1/T+5）
        industry        一级行业名称
        up_count        历届合计上涨次数
        down_count      历届合计下跌次数
        zero_count      历届合计平/缺失次数
        n_sessions      历届次数（该族群下有多少届次参与统计）
        up_ratio        上涨次数 / n_sessions
        down_ratio      下跌次数 / n_sessions
        zero_ratio      平次数 / n_sessions
"""

from pathlib import Path
import csv
import re
from collections import defaultdict


def normalize_meeting_family(name: str) -> str:
    """将具体届次的会议名称归一为「会议族群名称」."""
    name = (name or "").strip()
    if not name:
        return ""
    # 先按「（」截断：XXX（2022）→ XXX
    if "（" in name:
        name = name.split("（", 1)[0].strip()
    # 再处理以“YYYY年”开头的情况：2013年中央经济工作会议 → 中央经济工作会议
    m = re.match(r"^(\d{4})年(.+)$", name)
    if m:
        name = m.group(2).strip()
    # 统一归一的一些会议族群关键字（不区分历届）
    family_keywords = [
        ("中国共产党", "全国代表大会", "中国共产党全国代表大会"),
        ("届三中全会", None, "三中全会"),  # 十二届/十八届/十九届/二十届等三中全会 → 三中全会
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


def run():
    base = Path(__file__).resolve().parent.parent
    src = base / "会议窗口数据" / "各会议一级行业涨跌频次_按时间轴.csv"
    dst = base / "会议窗口数据" / "会议族群_各时间轴行业涨跌统计.csv"

    if not src.exists():
        print(f"未找到源文件: {src}")
        return

    # 聚合容器
    agg_counts: dict[tuple[str, str, str], dict[str, int]] = {}
    sessions_by_family_window: dict[tuple[str, str], set[str]] = defaultdict(set)

    with open(src, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            meeting_raw = (row.get("会议名称") or "").strip()
            window = (row.get("窗口") or "").strip()
            industry = (row.get("行业名称") or "").strip()
            if not meeting_raw or not window or not industry:
                continue

            family = normalize_meeting_family(meeting_raw)
            if not family:
                continue

            key = (family, window, industry)
            if key not in agg_counts:
                agg_counts[key] = {"up": 0, "down": 0, "zero": 0}

            # 累加次数（源表中是 0/1）
            def _to_int(x):
                try:
                    return int(float(x))
                except Exception:
                    return 0

            agg_counts[key]["up"] += _to_int(row.get("up_count"))
            agg_counts[key]["down"] += _to_int(row.get("down_count"))
            agg_counts[key]["zero"] += _to_int(row.get("zero_count"))

            # 记录该族群+时间轴下出现过的具体届次（用原始会议名称区分）
            sessions_by_family_window[(family, window)].add(meeting_raw)

    # 输出结果
    rows_out = []
    for (family, window, industry), cnt in sorted(
        agg_counts.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])
    ):
        n_sessions = len(sessions_by_family_window.get((family, window), set()))
        up = cnt["up"]
        down = cnt["down"]
        zero = cnt["zero"]
        if n_sessions <= 0:
            up_ratio = down_ratio = zero_ratio = 0.0
        else:
            up_ratio = round(up / n_sessions, 4)
            down_ratio = round(down / n_sessions, 4)
            zero_ratio = round(zero / n_sessions, 4)
        rows_out.append(
            {
                "meeting_family": family,
                "window": window,
                "industry": industry,
                "up_count": up,
                "down_count": down,
                "zero_count": zero,
                "n_sessions": n_sessions,
                "up_ratio": up_ratio,
                "down_ratio": down_ratio,
                "zero_ratio": zero_ratio,
            }
        )

    fieldnames = [
        "meeting_family",
        "window",
        "industry",
        "up_count",
        "down_count",
        "zero_count",
        "n_sessions",
        "up_ratio",
        "down_ratio",
        "zero_ratio",
    ]
    with open(dst, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"已写入 {dst}，共 {len(rows_out)} 行（会议族群 × 时间轴 × 行业）。")


if __name__ == "__main__":
    run()

