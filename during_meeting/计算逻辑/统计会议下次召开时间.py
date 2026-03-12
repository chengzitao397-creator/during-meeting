# -*- coding: utf-8 -*-
"""
根据「会议历届时间.csv」统计各会议族群最近一届时间，并推断下一次预计召开时间。
输出：时间/会议下次召开时间.csv
"""

import csv
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime, date


def normalize_meeting_family(name: str) -> str:
    """将具体届次的会议名称归一为「会议族群」."""
    name = (name or "").strip()
    if not name:
        return ""
    # 去掉「第X届」前缀（如 第七届中国国际进口博览会）
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
        ("发改委会议", None, "发改委会议"),
        ("国务院常务会议", None, "国务院常务会议"),
        ("工信部会议", None, "工信部会议"),
        ("生态环境部会议", None, "生态环境部会议"),
    ]
    for kw1, kw2, fam in family_keywords:
        if kw1 in name and (kw2 is None or kw2 in name):
            return fam
    return name


def parse_start_date(time_str):
    """从「2022-11-15 至 2022-11-16」中解析开始日期，返回 (date_obj, 日期字符串)。"""
    if not time_str or not time_str.strip():
        return None, ""
    part = (time_str.strip().split("至")[0] or "").strip()[:10]
    if not part or len(part) != 10:
        return None, part
    try:
        return datetime.strptime(part, "%Y-%m-%d").date(), part
    except ValueError:
        return None, part


def _parse_inferred_date(next_str: str, family: str):
    """把推断出的时间字符串解析为近似日期，用于和「今天」比较。无法解析则返回 None。"""
    if not next_str or next_str == "待定":
        return None
    # 2026年一季度（约3月末）-> 2026-03-31
    m = re.search(r"(\d{4})年一季度", next_str)
    if m:
        return date(int(m.group(1)), 3, 31)
    m = re.search(r"(\d{4})年二季度", next_str)
    if m:
        return date(int(m.group(1)), 6, 30)
    m = re.search(r"(\d{4})年三季度", next_str)
    if m:
        return date(int(m.group(1)), 9, 30)
    m = re.search(r"(\d{4})年四季度", next_str)
    if m:
        return date(int(m.group(1)), 12, 31)
    # 2025年3月末 / 6月末 / 9月末 / 12月末（央行）
    for month in (3, 6, 9, 12):
        m = re.search(r"(\d{4})年.*" + str(month) + r"月末", next_str)
        if m:
            return date(int(m.group(1)), month, 28)
    # 2025年1-2月 / 2025年11-12月（区间取首月以便比较）
    m = re.search(r"(\d{4})年(\d{1,2})(?:-\d{1,2})?月", next_str)
    if m:
        y, mon = int(m.group(1)), int(m.group(2))
        if 1 <= mon <= 12:
            return date(y, mon, 1)
    # 2026年3月上旬 等（单月）
    m = re.search(r"(\d{4})年(\d{1,2})月", next_str)
    if m:
        y, mon = int(m.group(1)), int(m.group(2))
        if 1 <= mon <= 12:
            return date(y, mon, 1)
    # 约2027年10月
    m = re.search(r"约?(\d{4})年(\d{1,2})月", next_str)
    if m:
        y, mon = int(m.group(1)), int(m.group(2))
        if 1 <= mon <= 12:
            return date(y, mon, 1)
    return None


def _advance_inferred_date(d: date, family: str):
    """将推断日期顺延一档（年度会议+1年，央行+3个月）。"""
    if family == "央行货币政策委员会例会":
        # 约加 3 个月
        if d.month <= 3:
            return date(d.year, 6, min(d.day, 28))
        if d.month <= 6:
            return date(d.year, 9, min(d.day, 28))
        if d.month <= 9:
            return date(d.year, 12, min(d.day, 28))
        return date(d.year + 1, 3, min(d.day, 28))
    if family == "中央政治局会议":
        if d.month >= 10:
            return date(d.year + 1, 4, min(d.day, 28))
        if d.month >= 7:
            return date(d.year, 10, min(d.day, 28))
        if d.month >= 4:
            return date(d.year, 7, min(d.day, 28))
        return date(d.year, 4, min(d.day, 28))
    # 其余按年顺延
    return date(d.year + 1, d.month, min(d.day, 28))


def _format_inferred_date(d: date, family: str, remark: str) -> str:
    """把顺延后的日期格式化成「下一次预计召开时间」的展示字符串。"""
    if family == "央行货币政策委员会例会":
        if d.month <= 3:
            return f"{d.year}年一季度（约3月末）"
        if d.month <= 6:
            return f"{d.year}年二季度（约6月末）"
        if d.month <= 9:
            return f"{d.year}年三季度（约9月末）"
        return f"{d.year}年四季度（约12月末）"
    if family == "中央政治局会议":
        if d.month >= 10:
            return f"{d.year}年10月或12月"
        if d.month >= 7:
            return f"{d.year}年7月下旬"
        if d.month >= 4:
            return f"{d.year}年4月下旬"
        return f"{d.year}年4月下旬"
    if family == "两会":
        return f"{d.year}年3月上旬"
    if family == "中国共产党全国代表大会":
        return f"约{d.year}年10月"
    # 年度会议：按月份给区间
    if d.month <= 2:
        return f"{d.year}年1-2月"
    if d.month <= 4:
        return f"{d.year}年3-4月"
    if d.month <= 6:
        return f"{d.year}年6月左右"
    if d.month <= 9:
        return f"{d.year}年9-10月"
    return f"{d.year}年11-12月"


def ensure_future(next_str: str, family: str, today: date) -> str:
    """若推断出的下次时间早于今天，则顺延到未来再返回展示字符串。"""
    d = _parse_inferred_date(next_str, family)
    if d is None:
        return next_str
    while d < today:
        d = _advance_inferred_date(d, family)
    return _format_inferred_date(d, family, "")


def infer_next_time(family: str, latest_name: str, latest_date_str: str, latest_date_obj):
    """
    根据会议族群与最近一届信息，推断下一次预计召开时间。
    返回 (下一次预计时间字符串, 备注)。
    """
    if not latest_date_str and not latest_date_obj:
        return "待定", "无历届时间"

    # 央行货币政策委员会例会：按季度，下一季度
    if family == "央行货币政策委员会例会":
        m = re.search(r"(\d{4})-Q([1-4])", latest_name)
        if m:
            y, q = int(m.group(1)), int(m.group(2))
            if q == 4:
                return f"{y + 1}年一季度（约3月末）", "每季度一次"
            next_q = q + 1
            months = {1: "3月末", 2: "6月末", 3: "9月末", 4: "12月末"}
            return f"{y}年{months[next_q]}", "每季度一次"
        return "待定", "每季度一次"

    # 美联储FOMC：每年约8次，下一场按已公布日程
    if family == "美联储FOMC议息会议":
        m = re.search(r"（(\d{4})-(\d{2})）", latest_name)
        if m:
            y, mon = int(m.group(1)), int(m.group(2))
            if int(mon) >= 11:
                return f"{y + 1}年1月或2月", "每年约8次"
            return f"{y}年或{y+1}年下一场", "每年约8次"
        return "待定", "每年约8次"

    # 中央政治局会议：约每季度
    if family == "中央政治局会议":
        if latest_date_obj:
            y, m = latest_date_obj.year, latest_date_obj.month
            if m >= 10:
                return f"{y + 1}年4月下旬", "约每季度一次"
            if m >= 7:
                return f"{y}年10月或12月", "约每季度一次"
            if m >= 4:
                return f"{y}年7月下旬", "约每季度一次"
            return f"{y}年4月下旬", "约每季度一次"
        return "待定", "约每季度一次"

    # 不定期会议：只标待定
    if family in ("三中全会", "全国金融工作会议", "全国科技创新大会",
                  "中央城镇化工作会议", "一带一路国际合作高峰论坛",
                  "发改委会议", "国务院常务会议", "工信部会议", "生态环境部会议"):
        return "待定", "不定期召开"

    # 中国共产党全国代表大会：五年一届
    if family == "中国共产党全国代表大会":
        if latest_date_obj:
            return f"约{latest_date_obj.year + 5}年10月", "五年一届"
        return "待定", "五年一届"

    # 两会：每年3月
    if family == "两会":
        if latest_date_obj:
            return f"{latest_date_obj.year + 1}年3月上旬", "每年一次"
        return "待定", "每年一次"

    # 其余按「最近一届 + 1 年」推断（年度会议）
    if latest_date_obj:
        next_year = latest_date_obj.year + 1
        # 按月份给大致区间
        mon = latest_date_obj.month
        if mon <= 2:
            month_desc = f"{next_year}年1-2月"
        elif mon <= 4:
            month_desc = f"{next_year}年3-4月"
        elif mon <= 6:
            month_desc = f"{next_year}年6月左右"
        elif mon <= 9:
            month_desc = f"{next_year}年9-10月"
        else:
            month_desc = f"{next_year}年11-12月"
        return month_desc, "按历届规律推断"
    return "待定", ""


def run():
    base = Path(__file__).resolve().parent.parent
    time_dir = base / "时间"
    path_in = time_dir / "会议历届时间.csv"
    path_out = time_dir / "会议下次召开时间.csv"

    if not path_in.exists():
        print(f"未找到 {path_in.name}")
        return

    # 按会议族群分组，保留每族最近一届的 会议名称、时间、开始日期
    by_family = defaultdict(list)
    with open(path_in, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("会议名称") or "").strip()
            time_str = (row.get("时间") or "").strip()
            if not name:
                continue
            family = normalize_meeting_family(name)
            if not family:
                continue
            date_obj, start_str = parse_start_date(time_str)
            by_family[family].append((name, time_str, date_obj, start_str))

    # 每个族群取最近一届（按开始日期最大），推断下次时间并以「参考日」为基准顺延到未来（至少按 2026-02 起算，避免仍出现 2025）
    today = max(date.today(), date(2026, 2, 1))
    rows_out = []
    for family in sorted(by_family.keys()):
        list_items = by_family[family]
        # 有日期的按日期排序，无日期的放最后
        list_items.sort(key=lambda x: (x[2] is None, -(x[2].toordinal() if x[2] else 0)))
        latest_name, latest_time, latest_date, latest_start_str = list_items[0]
        next_str, remark = infer_next_time(family, latest_name, latest_start_str, latest_date)
        next_str = ensure_future(next_str, family, today)
        rows_out.append({
            "会议族群": family,
            "最近一届会议名称": latest_name,
            "最近一届时间": latest_time,
            "下一次预计召开时间": next_str,
            "备注": remark,
        })

    time_dir.mkdir(parents=True, exist_ok=True)
    with open(path_out, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["会议族群", "最近一届会议名称", "最近一届时间", "下一次预计召开时间", "备注"])
        writer.writeheader()
        writer.writerows(rows_out)
    print(f"已写入 {path_out}，共 {len(rows_out)} 个会议族群。")


if __name__ == "__main__":
    run()
