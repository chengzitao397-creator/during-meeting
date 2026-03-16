# -*- coding: utf-8 -*-
"""
按「每一年当现在」视角（过去进行时），为 2022–2026 每年、每月的会议显著行业拆表。

逻辑：
- 届数/涨跌统计：选某年 Y 时，只使用「T日 ≤ 该年12月31日」的届次计算届数、上涨/下跌次数与概率、平均涨跌幅。
- 对每一年 Y，以 Y-01-01 为基准日，从 会议历届时间_副本 中取各会议族群「下一次」召开日；
  该日的月份即为该年视角下的「下一次月份」，用于把 (会议, 窗口, 行业) 归到对应月。
- 每年产出 12 个月份 CSV，无数据的月写出空表（仅表头），保证网站可选任意年任意月。

输入：
  - output/受影响显著的一级行业_中信30_最终版.csv
  - output/会议窗口行业数据_中信30.csv（按 T 日截断后按年重算届数与涨跌统计）
  - 会议历届时间_副本.csv（项目根目录）

输出：
  - output/{年}/受影响显著一级行业_{年}M{月}_中信30.csv，年=2022..2026，月=01..12
"""
from pathlib import Path
import re
import csv
import math
from datetime import datetime
from collections import defaultdict

import pandas as pd


BASE = Path(__file__).resolve().parent.parent
PATH_FINAL = BASE / "output" / "受影响显著的一级行业_中信30_最终版.csv"
PATH_WINDOW = BASE / "output" / "会议窗口行业数据_中信30.csv"
PATH_HISTORY = BASE / "会议历届时间_副本.csv"
OUT_DIR = BASE / "output"

YEAR_MIN, YEAR_MAX = 2022, 2026
REFERENCE_DAY = "-01-01"  # 每年基准日：Y-01-01


def normalize_meeting_family(name: str) -> str:
    """与 生成受影响显著一级行业_中信30 一致：历届名称 → 会议族群。"""
    name = (name or "").strip()
    if not name:
        return ""
    # 去掉「第X届」前缀（如 第八届中国国际进口博览会）
    m = re.match(r"^第[一二三四五六七八九十\d]+届(.+)$", name)
    if m:
        name = m.group(1).strip()
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
        ("中央城镇化工作会议", None, "中央城镇化工作会议"),
        ("中央政治局会议", None, "中央政治局会议"),
        ("两会", None, "两会"),
    ]
    for kw1, kw2, fam in family_keywords:
        if kw1 in name and (kw2 is None or kw2 in name):
            return fam
    return name


def parse_industry_returns(detail_str: str):
    """解析「行业涨跌幅明细」字符串，返回 [(行业, 涨跌幅), ...]；跳过 nan。"""
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
            v = float(val)
            if math.isfinite(v):
                out.append((name, v))
        except ValueError:
            continue
    return out


def parse_start_date(time_str: str) -> str:
    """从「YYYY-MM-DD 至 YYYY-MM-DD」取起始日，返回 YYYY-MM-DD 或空。"""
    s = (time_str or "").strip()
    if " 至 " in s:
        s = s.split(" 至 ")[0].strip()
    s = s[:10]
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    return ""


def load_next_by_year() -> dict:
    """
    从 会议历届时间_副本 按年计算每个会议族群的「下一次」召开日（基准日=该年1月1日）。
    返回：{(meeting_family, year): (month_int, date_display_str)}
    """
    if not PATH_HISTORY.exists():
        return {}
    # 先按族群收集所有届次的起始日
    family_dates = defaultdict(list)  # family -> [ (yyyy-mm-dd, time_str), ... ]
    with open(PATH_HISTORY, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("会议名称") or "").strip()
            time_str = (row.get("时间") or "").strip()
            if not name or not time_str:
                continue
            fam = normalize_meeting_family(name)
            if not fam:
                continue
            start = parse_start_date(time_str)
            if not start:
                continue
            family_dates[fam].append((start, time_str))
    for fam in family_dates:
        family_dates[fam].sort(key=lambda x: x[0])
    # 对每个参考年，取该年 1 月 1 日之后的最近一届
    out = {}
    for year in range(YEAR_MIN, YEAR_MAX + 1):
        ref = f"{year}{REFERENCE_DAY}"  # e.g. 2022-01-01
        for fam, dates_list in family_dates.items():
            next_item = None
            for start, time_str in dates_list:
                if start >= ref:
                    next_item = (start, time_str)
                    break
            if next_item is None:
                continue
            start, _ = next_item
            try:
                # 月份 1-12
                month = int(start[5:7])
                if 1 <= month <= 12:
                    # 展示用：M月D日
                    day = int(start[8:10])
                    date_display = f"{month}月{day}日"
                    out[(fam, year)] = (month, date_display)
            except (ValueError, IndexError):
                pass
    return out


def compute_stats_by_year(sig_keys: set) -> dict:
    """
    按年截断会议窗口数据，对每一年 Y 只保留 T日 <= Y-12-31 的届次，
    按 (meeting_family, window, industry) 聚合届数、涨跌次数与概率、平均涨跌幅。
    返回：{ year: { (fam, win, ind): {届数, 上涨次数, 下跌次数, 持平次数, 上涨概率, 下跌概率, 持平概率, 平均涨跌幅}, ... }, ... }
    """
    if not PATH_WINDOW.exists():
        return {}
    with open(PATH_WINDOW, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows_window = list(reader)
    if "T日" not in fieldnames or "行业涨跌幅明细" not in fieldnames:
        return {}

    # 按年预计算：每年只保留 T日 <= 该年12月31日 的行，再展开并聚合
    cutoff_dates = {y: f"{y}-12-31" for y in range(YEAR_MIN, YEAR_MAX + 1)}
    result = {y: defaultdict(lambda: {"returns": []}) for y in range(YEAR_MIN, YEAR_MAX + 1)}

    for r in rows_window:
        t_day = (r.get("T日") or "").strip()[:10]
        if len(t_day) != 10 or t_day[4] != "-" or t_day[7] != "-":
            continue
        meeting_name = (r.get("会议名称") or "").strip()
        window = (r.get("窗口") or "").strip()
        detail = r.get("行业涨跌幅明细") or ""
        fam = normalize_meeting_family(meeting_name)
        if not fam or not window:
            continue
        for ind_name, ret in parse_industry_returns(detail):
            if (ind_name, window, fam) not in sig_keys:
                continue
            key = (fam, window, ind_name)
            for year in range(YEAR_MIN, YEAR_MAX + 1):
                if t_day <= cutoff_dates[year]:
                    result[year][key]["returns"].append(ret)

    # 把 returns 转成届数、涨跌次数、概率、平均涨跌幅；并为当年无届次的 (fam, win, ind) 补 0
    out = {}
    for year in range(YEAR_MIN, YEAR_MAX + 1):
        out[year] = {}
        for (fam, win, ind), v in result[year].items():
            rets = v["returns"]
            n = len(rets)
            up_cnt = sum(1 for x in rets if x > 0)
            down_cnt = sum(1 for x in rets if x < 0)
            flat_cnt = sum(1 for x in rets if x == 0)
            up_rate = round(up_cnt / n, 6) if n else None
            down_rate = round(down_cnt / n, 6) if n else None
            flat_rate = round(flat_cnt / n, 6) if n else None
            avg_ret = round(sum(rets) / n, 6) if n else None
            out[year][(fam, win, ind)] = {
                "届数": n,
                "上涨次数": up_cnt,
                "下跌次数": down_cnt,
                "持平次数": flat_cnt,
                "上涨概率": up_rate,
                "下跌概率": down_rate,
                "持平概率": flat_rate,
                "平均涨跌幅": avg_ret,
            }
        # 该年尚无任何届次的 (fam, win, ind) 也写入，届数=0，便于表格一致
        for (ind, win, fam) in sig_keys:
            key = (fam, win, ind)
            if key not in out[year]:
                out[year][key] = {
                    "届数": 0,
                    "上涨次数": 0,
                    "下跌次数": 0,
                    "持平次数": 0,
                    "上涨概率": None,
                    "下跌概率": None,
                    "持平概率": None,
                    "平均涨跌幅": None,
                }
    return out


def run():
    if not PATH_FINAL.exists():
        print(f"未找到：{PATH_FINAL}")
        return
    if not PATH_WINDOW.exists():
        print(f"未找到：{PATH_WINDOW}")
        return

    # 读取最终版，得到要输出的 (industry, window, meeting_family) 集合
    sig_keys = set()
    with open(PATH_FINAL, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ind = (row.get("industry") or "").strip()
            win = (row.get("window") or "").strip()
            fam = (row.get("meeting_family") or "").strip()
            if ind and win and fam:
                sig_keys.add((ind, win, fam))
    if not sig_keys:
        print("最终版中无有效 (industry, window, meeting_family) 记录")
        return

    # 按年截断并计算届数、涨跌统计（过去进行时）
    print("正在按年截断会议窗口数据并计算届数与涨跌统计…")
    stats_by_year = compute_stats_by_year(sig_keys)
    if not stats_by_year:
        print("未能从会议窗口数据计算出按年统计，请检查 会议窗口行业数据_中信30.csv 的 T日、行业涨跌幅明细 列。")
        return

    # 按年视角的「下一次月份」与「下一次召开_具体日期」
    next_by_year = load_next_by_year()
    if not next_by_year:
        print("未从 会议历届时间_副本 解析出任何 (会议族群, 年) 的下一届，请检查路径与格式。")
        return

    df_final = pd.read_csv(PATH_FINAL, encoding="utf-8-sig")
    key = ["meeting_family", "window", "industry"]
    for c in key:
        if c not in df_final.columns:
            print(f"最终版缺少列：{key}")
            return

    cols_wanted = [
        "下一次月份",
        "meeting_family",
        "下一次召开_具体日期",
        "window",
        "industry",
        "届数",
        "上涨次数",
        "下跌次数",
        "持平次数",
        "上涨概率",
        "下跌概率",
        "持平概率",
        "平均涨跌幅",
    ]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    total_files = 0
    total_rows = 0

    for year in range(YEAR_MIN, YEAR_MAX + 1):
        # 该年的届数/涨跌统计（过去进行时）
        year_stats = stats_by_year.get(year, {})
        rows_for_year = []
        for _, row in df_final.iterrows():
            fam = row["meeting_family"]
            win = row["window"]
            ind = row["industry"]
            tup = next_by_year.get((fam, year))
            if tup is None:
                continue
            next_month, next_date = tup
            st = year_stats.get((fam, win, ind), {})
            rows_for_year.append({
                "下一次月份": next_month,
                "meeting_family": fam,
                "下一次召开_具体日期": next_date,
                "window": win,
                "industry": ind,
                "届数": st.get("届数", ""),
                "上涨次数": st.get("上涨次数", ""),
                "下跌次数": st.get("下跌次数", ""),
                "持平次数": st.get("持平次数", ""),
                "上涨概率": st.get("上涨概率", ""),
                "下跌概率": st.get("下跌概率", ""),
                "持平概率": st.get("持平概率", ""),
                "平均涨跌幅": st.get("平均涨跌幅", ""),
            })
        df_y = pd.DataFrame(rows_for_year)

        year_dir = OUT_DIR / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        for month in range(1, 13):
            df_m = df_y[df_y["下一次月份"] == month]
            df_m = df_m.sort_values(["meeting_family", "window", "industry"]).reset_index(drop=True)
            out_cols = [c for c in cols_wanted if c in df_m.columns]
            if not out_cols:
                out_cols = cols_wanted
            df_out = df_m[out_cols] if not df_m.empty else pd.DataFrame(columns=out_cols)
            fname = f"受影响显著一级行业_{year}M{month:02d}_中信30.csv"
            path_out = year_dir / fname
            df_out.to_csv(path_out, index=False, encoding="utf-8-sig")
            total_files += 1
            total_rows += len(df_out)
            if len(df_out) > 0:
                print(f"已写入 {path_out.name}，{len(df_out)} 行。")
            else:
                print(f"已写入 {path_out.name}（空表）。")

    print(f"共生成 {total_files} 个月度表（{YEAR_MIN}–{YEAR_MAX} 年 × 12 月），总数据行数 {total_rows}。")


if __name__ == "__main__":
    run()
