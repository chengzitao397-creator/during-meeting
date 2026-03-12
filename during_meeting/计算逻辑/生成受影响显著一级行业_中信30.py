# -*- coding: utf-8 -*-
"""
从中信30会议窗口数据生成「受影响显著的一级行业」表（第一层次复刻）。

- 输入：output/会议窗口行业数据_中信30.csv（行业涨跌明细为「行业:涨/跌/平」）
- 展开明细 → 按 (会议族群, 时间轴, 行业) 聚合 → 算 up_rate/down_rate/bias_from_50/direction
- 筛选：|up_rate - 0.5| >= 0.20 且 total >= 6
- 输出：output/受影响显著的一级行业_中信30_最终版.csv，列与 最终版_副本 一致；填不了的列留空。
"""
from pathlib import Path
import csv
import re
from collections import defaultdict

# 与 会议族群_各时间轴行业涨跌统计 一致的会议族群归一
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


MIN_BIAS = 0.20
MIN_TOTAL = 6

# 基准日期：用于填写「下一次预计召开时间」的参考日（2026.3.5）
REFERENCE_DATE = "2026-03-05"

# 会议族群 → (下一次预计召开时间, 预测备注)，来源：公开信息检索（基准 2026年3月5日）
NEXT_MEETING_TABLE = {
    "两会": ("2027年3月", "每年3月，人大一般3月5日开幕"),
    "中央经济工作会议": ("2026年12月", "每年12月前后"),
    "中国国际进口博览会": ("2026年11月5日至10日", "每年11月上旬上海，第九届"),
    "G20领导人峰会": ("2026年12月14-15日", "2026年美国迈阿密主办"),
    "央行货币政策委员会例会": ("2026年一季度（约3月末）", "每季度一次"),
    "IMF与世行春季会议": ("2026年4月", "通常4月"),
    "IMF与世行秋季年会": ("2026年10月", "通常10月"),
    "中央农村工作会议": ("2026年12月下旬", "通常12月下旬"),
    "中央政治局会议": ("2026年4月下旬", "约每季度末"),
    "中国共产党全国代表大会": ("2027年10-11月", "每5年一次"),
    "三中全会": ("待定", "每届一次，时间不固定"),
    "全国金融工作会议": ("待定", "不定期，约隔5年"),
    "全国科技创新大会": ("待定", "不定期"),
    "证监会系统工作会议": ("2027年1-2月", "每年1-2月"),
    "冬季达沃斯": ("2027年1月下旬", "每年1月下旬瑞士达沃斯"),
    "夏季达沃斯": ("2026年6月或9月", "多在6月或9月中国城市"),
    "博鳌亚洲论坛": ("2027年3月下旬", "每年3月下旬海南博鳌"),
    "一带一路国际合作高峰论坛": ("待定", "年份与日期不固定"),
    "全国住房城乡建设工作会议": ("2026年底或2027年1月", "通常年底或次年1月"),
    "中央城镇化工作会议": ("待定", "不定期"),
    # 以下为补充：表中出现但原表未覆盖的会议族群
    "世界互联网大会": ("2026年11月", "每年11月乌镇"),
    "世界人工智能大会": ("2026年7月", "通常7月上海"),
    "中关村论坛": ("2026年4月", "通常4月北京"),
    "中国—东盟博览会": ("2026年9月", "通常9月南宁"),
    "中国发展高层论坛": ("2026年3月", "每年3月北京"),
    "广交会春季": ("2026年4月", "每年4-5月广州"),
    "广交会秋季": ("2026年10月", "每年10-11月广州"),
    "服贸会": ("2026年9月", "每年9月北京"),
    "美联储FOMC议息会议": ("2026年3月", "每年多次，约每6周"),
    "陆家嘴论坛": ("2026年6月", "通常6月上海"),
    "高交会": ("2026年11月", "通常11月深圳"),
}


def _parse_time_range(s):
    """从「YYYY-MM-DD 至 YYYY-MM-DD」解析起始日，用于比较先后。"""
    s = (s or "").strip()
    if " 至 " in s:
        return s.split(" 至 ")[0].strip()[:10]
    return s[:10] if len(s) >= 10 else ""


def _load_latest_session_by_family(base):
    """从 会议历届时间_副本.csv 按会议族群取最近一届时间。返回 dict: meeting_family -> 时间字符串"""
    path = base / "会议历届时间_副本.csv"
    if not path.exists():
        return {}
    latest = {}  # family -> (start_date, 时间)
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("会议名称") or "").strip()
            time_str = (row.get("时间") or "").strip()
            if not name or not time_str:
                continue
            fam = normalize_meeting_family(name)
            if not fam:
                continue
            start = _parse_time_range(time_str)
            if not start:
                continue
            if fam not in latest or start > latest[fam][0]:
                latest[fam] = (start, time_str)
    return {fam: time_str for fam, (_, time_str) in latest.items()}


def run():
    base = Path(__file__).resolve().parent.parent
    path_in = base / "output" / "会议窗口行业数据_中信30.csv"
    path_out = base / "output" / "受影响显著的一级行业_中信30_最终版.csv"

    if not path_in.exists():
        print(f"未找到输入: {path_in}")
        return

    # 历届时间表：会议族群 → 最近一届时间
    latest_session = _load_latest_session_by_family(base)

    # 1) 展开行业涨跌明细为 (会议, T日, 窗口, 窗口开始, 窗口结束, 行业, up, down, zero)
    detail_rows = []
    with open(path_in, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            meeting = (row.get("会议名称") or "").strip()
            t_day = (row.get("T日") or "").strip()[:10]
            window = (row.get("窗口") or "").strip()
            start = (row.get("窗口开始") or "").strip()
            end = (row.get("窗口结束") or "").strip()
            detail_str = (row.get("行业涨跌明细") or "").strip()
            if not meeting or not window or not detail_str:
                continue
            for part in detail_str.split(";"):
                part = part.strip()
                if not part:
                    continue
                if ":" not in part:
                    continue
                name, flag = part.split(":", 1)
                name = name.strip()
                flag = flag.strip()
                up = 1 if flag == "涨" else 0
                down = 1 if flag == "跌" else 0
                zero = 1 if flag not in ("涨", "跌") else 0
                detail_rows.append({
                    "会议名称": meeting,
                    "T日": t_day,
                    "窗口": window,
                    "窗口开始": start,
                    "窗口结束": end,
                    "行业": name,
                    "up_count": up,
                    "down_count": down,
                    "zero_count": zero,
                })

    # 2) 按 (meeting_family, window, industry) 聚合；窗口开始/结束收集历届所有日期（带年份）
    agg = defaultdict(lambda: {
        "up": 0, "down": 0, "zero": 0,
        "t_dates": [],
        "window_starts": [],
        "window_ends": [],
    })

    for r in detail_rows:
        fam = normalize_meeting_family(r["会议名称"])
        if not fam:
            continue
        win = r["窗口"]
        ind = r["行业"]
        key = (fam, win, ind)
        agg[key]["up"] += r["up_count"]
        agg[key]["down"] += r["down_count"]
        agg[key]["zero"] += r["zero_count"]
        if r["T日"]:
            agg[key]["t_dates"].append(r["T日"])
        if r["窗口开始"]:
            agg[key]["window_starts"].append(r["窗口开始"])
        if r["窗口结束"]:
            agg[key]["window_ends"].append(r["窗口结束"])

    # 3) 计算 total, up_rate, down_rate, bias_from_50, direction；筛选；填窗口开始/结束（历届日期）、最近一届、下一次预计
    out_rows = []
    for (fam, win, ind), v in agg.items():
        up = v["up"]
        down = v["down"]
        total = up + down
        if total == 0:
            continue
        up_rate = round(up / total, 10)
        down_rate = round(down / total, 10)
        bias_from_50 = round(abs(up_rate - 0.5), 10)
        if bias_from_50 < MIN_BIAS or total < MIN_TOTAL:
            continue
        direction = "mostly_up" if up_rate >= 0.70 else "mostly_down"

        t_list = sorted(set(v["t_dates"]))
        t_dates_str = "; ".join(t_list) if t_list else ""

        # 窗口开始/结束：历届所有日期（带年份），去重排序后用分号分隔
        starts_str = "; ".join(sorted(set(v["window_starts"]))) if v["window_starts"] else ""
        ends_str = "; ".join(sorted(set(v["window_ends"]))) if v["window_ends"] else ""

        next_info = NEXT_MEETING_TABLE.get(fam, ("", ""))
        next_time = next_info[0] if isinstance(next_info, (list, tuple)) else ""
        next_remark = next_info[1] if isinstance(next_info, (list, tuple)) and len(next_info) > 1 else ""

        out_rows.append({
            "industry": ind,
            "window": win,
            "meeting_family": fam,
            "up_count": up,
            "down_count": down,
            "total": total,
            "up_rate": up_rate,
            "down_rate": down_rate,
            "bias_from_50": bias_from_50,
            "direction": direction,
            "涉及的T日": t_dates_str,
            "窗口开始": starts_str,
            "窗口结束": ends_str,
            "平均涨跌幅_收盘": "",
            "最大涨跌幅_收盘": "",
            "最小涨跌幅_收盘": "",
            "平均涨跌幅_开盘": "",
            "最大涨跌幅_开盘": "",
            "最小涨跌幅_开盘": "",
            "最近一届时间": latest_session.get(fam, ""),
            "下一次预计召开时间": next_time,
            "预测备注": next_remark,
        })

    # 按 meeting_family, window, bias_from_50 降序
    out_rows.sort(key=lambda r: (r["meeting_family"], r["window"], -r["bias_from_50"], r["industry"]))

    fieldnames = [
        "industry", "window", "meeting_family",
        "up_count", "down_count", "total", "up_rate", "down_rate", "bias_from_50", "direction",
        "涉及的T日", "窗口开始", "窗口结束",
        "平均涨跌幅_收盘", "最大涨跌幅_收盘", "最小涨跌幅_收盘",
        "平均涨跌幅_开盘", "最大涨跌幅_开盘", "最小涨跌幅_开盘",
        "最近一届时间", "下一次预计召开时间", "预测备注",
    ]
    with open(path_out, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"已写入 {path_out}，共 {len(out_rows)} 行（筛选后）。")
    print("已填：窗口开始/结束（历届日期带年份）、最近一届时间（来自历届时间表）、下一次预计召开时间与预测备注（基准2026.3.5检索）。")
    print("已留空列：平均/最大/最小涨跌幅_收盘、平均/最大/最小涨跌幅_开盘。")


if __name__ == "__main__":
    run()
