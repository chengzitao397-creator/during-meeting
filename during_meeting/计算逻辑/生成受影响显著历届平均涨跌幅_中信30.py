# -*- coding: utf-8 -*-
"""
基于「受影响显著的一级行业_中信30_最终版」与带涨跌幅的会议窗口数据，
按 (会议族群, 窗口, 行业) 聚合历届平均/最大/最小涨跌幅，输出到 output 目录。

- 输入1：output/会议窗口行业数据_中信30.csv（须含「行业涨跌幅明细」列，格式：行业:数值; ...）
- 输入2：output/受影响显著的一级行业_中信30_最终版.csv（列 industry, window, meeting_family）
- 输出：output/受影响显著一级行业_历届平均涨跌幅_中信30.csv
"""
from pathlib import Path
import csv
import re
from collections import defaultdict

# 与 生成受影响显著一级行业_中信30 一致的会议族群归一
def normalize_meeting_family(name: str) -> str:
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


def parse_industry_returns(detail_str: str):
    """解析「行业涨跌幅明细」字符串，返回 [(行业, 涨跌幅), ...]。"""
    out = []
    if not detail_str:
        return out
    for part in detail_str.split(";"):
        part = part.strip()
        if ":" not in part:
            continue
        name, val = part.split(":", 1)
        name, val = name.strip(), val.strip()
        if not name:
            continue
        try:
            out.append((name, float(val)))
        except ValueError:
            continue
    return out


def run():
    base = Path(__file__).resolve().parent.parent
    path_window = base / "output" / "会议窗口行业数据_中信30.csv"
    path_sig = base / "output" / "受影响显著的一级行业_中信30_最终版.csv"
    path_out = base / "output" / "受影响显著一级行业_历届平均涨跌幅_中信30.csv"

    if not path_window.exists():
        print(f"未找到会议窗口数据：{path_window}")
        return
    if not path_sig.exists():
        print(f"未找到受影响显著表：{path_sig}")
        return

    # 读取会议窗口数据，必须有「行业涨跌幅明细」
    with open(path_window, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows_window = list(reader)
    if "行业涨跌幅明细" not in fieldnames:
        print("会议窗口数据中缺少「行业涨跌幅明细」列。请先运行：计算逻辑/会议窗口行业数据_米筐_中信30.py 重新生成会议窗口数据。")
        return

    # 读取受影响显著表，得到 (industry, window, meeting_family) 集合
    # 使用 utf-8-sig 以兼容从 Excel 导出的 CSV（带 BOM 时列名会多 \ufeff）
    sig_keys = set()
    with open(path_sig, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ind = (row.get("industry") or "").strip()
            win = (row.get("window") or "").strip()
            fam = (row.get("meeting_family") or "").strip()
            if ind and win and fam:
                sig_keys.add((ind, win, fam))
    if not sig_keys:
        print("受影响显著表中无有效 (industry, window, meeting_family) 记录")
        return

    # 展开会议窗口行为 (meeting_family, window, industry, 涨跌幅)，只保留在 sig_keys 中的组合
    # 每行对应一届会议的一个窗口，所以每行内同一 (fam, win, ind) 只出现一次
    expanded = []
    for r in rows_window:
        meeting_name = (r.get("会议名称") or "").strip()
        window = (r.get("窗口") or "").strip()
        detail = r.get("行业涨跌幅明细") or ""
        fam = normalize_meeting_family(meeting_name)
        if not fam or not window:
            continue
        for ind_name, ret in parse_industry_returns(detail):
            if (ind_name, window, fam) in sig_keys:
                expanded.append((fam, window, ind_name, ret))

    # 按 (meeting_family, window, industry) 聚合：届数、平均、最大、最小 + 涨跌次数/概率
    # 使用 returns 保存历届涨跌幅明细，便于统一计算统计指标
    agg = defaultdict(lambda: {"returns": []})
    for fam, win, ind, ret in expanded:
        key = (fam, win, ind)
        agg[key]["returns"].append(ret)

    out_rows = []
    for (fam, win, ind), v in agg.items():
        rets = v["returns"]
        n = len(rets)
        # 统计上涨/下跌/持平的次数
        up_cnt = sum(1 for x in rets if x > 0)
        down_cnt = sum(1 for x in rets if x < 0)
        flat_cnt = sum(1 for x in rets if x == 0)
        # 概率 = 次数 / 届数
        up_rate = round(up_cnt / n, 6) if n else ""
        down_rate = round(down_cnt / n, 6) if n else ""
        flat_rate = round(flat_cnt / n, 6) if n else ""
        out_rows.append({
            "meeting_family": fam,
            "window": win,
            "industry": ind,
            "届数": n,
            "平均涨跌幅": round(sum(rets) / n, 6) if n else "",
            "最大涨跌幅": round(max(rets), 6) if n else "",
            "最小涨跌幅": round(min(rets), 6) if n else "",
            "上涨次数": up_cnt,
            "下跌次数": down_cnt,
            "持平次数": flat_cnt,
            "上涨概率": up_rate,
            "下跌概率": down_rate,
            "持平概率": flat_rate,
        })

    # 按会议族群、窗口、行业排序
    out_rows.sort(key=lambda r: (r["meeting_family"], r["window"], r["industry"]))

    path_out.parent.mkdir(parents=True, exist_ok=True)
    out_fieldnames = [
        "meeting_family",
        "window",
        "industry",
        "届数",
        "平均涨跌幅",
        "最大涨跌幅",
        "最小涨跌幅",
        "上涨次数",
        "下跌次数",
        "持平次数",
        "上涨概率",
        "下跌概率",
        "持平概率",
    ]
    with open(path_out, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)
    print(f"已写入 {path_out}，共 {len(out_rows)} 行（仅包含受影响显著表中的会议族群×窗口×行业）。")


if __name__ == "__main__":
    run()
