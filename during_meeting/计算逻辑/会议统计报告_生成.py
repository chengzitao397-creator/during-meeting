# -*- coding: utf-8 -*-
"""
根据 会议历届时间_副本、会议名单、按月份分类 数据，
统计：有哪些会议、新旧分类、是否定时、月份分布，并输出 Markdown 报告。

- 「有哪些会议」以 会议名单.csv 为准（完整名单，约 55 个）。
- 新旧/定时/月份分布 均按名单中每条统计；并单独说明「本项目纳入的会议」为其中 16 个。
"""
from pathlib import Path
import re
import csv
from collections import defaultdict

BASE = Path(__file__).resolve().parent.parent
PATH_HISTORY = BASE / "会议历届时间_副本.csv"
PATH_NAMELIST = BASE / "会议名单.csv"
PATH_BY_MONTH = BASE / "output" / "受影响显著的一级行业_中信30_按月份分类.csv"
PATH_REPORT = BASE / "会议统计报告.md"

# 与按年按月份拆表一致的会议族群归一（用于历届时间表）
def normalize_meeting_family(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return ""
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


def _list_name_to_family(list_name: str) -> str:
    """会议名单中的名称 -> 历届时间表用的会议族群（用于查首次年）。"""
    # 名单与历届 family 的对应：名单可能更具体（如 全国人大年度会议 -> 两会）
    if "两会" in list_name or "全国人大" in list_name or "全国政协" in list_name:
        return "两会"
    if "中共中央全会" in list_name or "三中全会" in list_name:
        return "三中全会"
    if "中国共产党全国代表大会" in list_name:
        return "中国共产党全国代表大会"
    if "中央经济工作会议" in list_name:
        return "中央经济工作会议"
    if "全国金融工作会议" in list_name:
        return "全国金融工作会议"
    if "广交会" in list_name or "中国进出口商品交易会" in list_name:
        return "广交会"  # 历届表分 广交会春季/秋季，下面用两者最早年赋给 广交会
    if "中国国际进口博览会" in list_name or "进博会" in list_name:
        return "中国国际进口博览会"
    if "证监会系统" in list_name:
        return "证监会系统工作会议"
    if "中国人民银行货币政策" in list_name or "央行货币政策" in list_name:
        return "央行货币政策委员会例会"
    if "美联储FOMC" in list_name:
        return "美联储FOMC议息会议"
    if "冬季达沃斯" in list_name or "世界经济论坛" in list_name:
        return "冬季达沃斯"
    if "夏季达沃斯" in list_name or "新领军者" in list_name:
        return "夏季达沃斯"
    if "博鳌" in list_name:
        return "博鳌亚洲论坛"
    if "IMF与世行春季" in list_name:
        return "IMF与世行春季会议"
    if "IMF与世行秋季" in list_name:
        return "IMF与世行秋季年会"
    if "中央农村工作会议" in list_name:
        return "中央农村工作会议"
    if "全国住房城乡建设" in list_name:
        return "全国住房城乡建设工作会议"
    if "中央城镇化" in list_name:
        return "中央城镇化工作会议"
    if "中央政治局" in list_name:
        return "中央政治局会议"
    if "全国科技创新大会" in list_name:
        return "全国科技创新大会"
    if "陆家嘴论坛" in list_name:
        return "陆家嘴论坛"
    if "中国发展高层论坛" in list_name:
        return "中国发展高层论坛"
    if "中关村论坛" in list_name:
        return "中关村论坛"
    if "世界互联网大会" in list_name or "乌镇" in list_name:
        return "世界互联网大会"
    if "世界人工智能大会" in list_name:
        return "世界人工智能大会"
    if "高交会" in list_name or "中国国际高新技术成果交易会" in list_name:
        return "高交会"
    if "服贸会" in list_name or "中国国际服务贸易交易会" in list_name:
        return "服贸会"
    if "中国—东盟博览会" in list_name or "东盟博览会" in list_name:
        return "中国—东盟博览会"
    if "中国国际消费品博览会" in list_name or "消博会" in list_name:
        return "中国国际消费品博览会"
    if "G20" in list_name or "二十国" in list_name:
        return "G20领导人峰会"
    if "一带一路" in list_name:
        return "一带一路国际合作高峰论坛"
    return None


def _is_scheduled(desc: str) -> str:
    """根据「大概召开时间」判断定时/不定时。"""
    if not desc:
        return "不定时"
    if "不定期" in desc or "不固定" in desc or "日期不固定" in desc:
        return "不定时"
    if "每年" in desc or "每季度" in desc or "每届" in desc:
        return "定时"
    return "不定时"


def _parse_months_from_desc(desc: str) -> list:
    """从「大概召开时间」解析提到的月份（1-12），如 每年3月、11-12月。"""
    if not desc:
        return []
    months = set()
    # X月 或 X-Y月
    for m in re.finditer(r"(\d{1,2})(?:-(\d{1,2}))?月", desc):
        start, end = int(m.group(1)), m.group(2)
        if end:
            for mm in range(start, min(int(end) + 1, 13)):
                if 1 <= mm <= 12:
                    months.add(mm)
        else:
            if 1 <= start <= 12:
                months.add(start)
    return sorted(months)


def main():
    # 1) 历届时间 -> 每个会议族群的首次举办年
    first_year = {}
    if PATH_HISTORY.exists():
        with open(PATH_HISTORY, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = (row.get("会议名称") or "").strip()
                time_str = (row.get("时间") or "").strip()[:10]
                if len(time_str) >= 4 and time_str[:4].isdigit():
                    y = int(time_str[:4])
                    fam = normalize_meeting_family(name)
                    if fam:
                        if fam not in first_year or y < first_year[fam]:
                            first_year[fam] = y
    # 广交会春季/秋季在历届表里，给「广交会」取两者最早年
    for fam in ("广交会春季", "广交会秋季"):
        if fam in first_year:
            y = first_year[fam]
            if "广交会" not in first_year or y < first_year["广交会"]:
                first_year["广交会"] = y

    # 2) 会议名单：全部条目（会议名称、意义、大概召开时间）
    namelist_rows = []
    if PATH_NAMELIST.exists():
        with open(PATH_NAMELIST, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = (row.get("会议名称") or "").strip()
                meaning = (row.get("意义") or "").strip()
                desc = (row.get("大概召开时间") or "").strip()
                if name:
                    namelist_rows.append({"会议名称": name, "意义": meaning, "大概召开时间": desc})

    # 3) 本项目纳入的会议族群（来自按月份分类）
    families_in_project = set()
    month_count_project = defaultdict(set)
    if PATH_BY_MONTH.exists():
        with open(PATH_BY_MONTH, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                fam = (row.get("meeting_family") or "").strip()
                if not fam:
                    continue
                families_in_project.add(fam)
                try:
                    m = row.get("下一次月份")
                    if m != "" and m is not None:
                        month_val = int(float(m))
                        if 1 <= month_val <= 12:
                            month_count_project[month_val].add(fam)
                except (ValueError, TypeError):
                    pass

    # 4) 名单中每条：匹配首次年、定时、月份
    CUTOFF_YEAR = 2011
    old_list, new_list, no_year_list = [], [], []
    scheduled_list, unscheduled_list = [], []
    month_count_namelist = defaultdict(int)  # 会议名单按「大概召开时间」提到的月份统计

    for r in namelist_rows:
        name, desc = r["会议名称"], r["大概召开时间"]
        fam = _list_name_to_family(name)
        y = first_year.get(fam) if fam else None
        sched = _is_scheduled(desc)
        months = _parse_months_from_desc(desc)

        if y is None:
            no_year_list.append((name, desc))
        elif y < CUTOFF_YEAR:
            old_list.append((name, y))
        else:
            new_list.append((name, y))

        if sched == "定时":
            scheduled_list.append(name)
        else:
            unscheduled_list.append(name)

        for m in months:
            month_count_namelist[m] += 1

    # 5) 月份分布表（会议名单 55 条按「大概召开时间」提到的月份）
    month_labels = ["一月", "二月", "三月", "四月", "五月", "六月", "七月", "八月", "九月", "十月", "十一月", "十二月"]
    month_stats_namelist = [(m, month_count_namelist.get(m, 0), month_labels[m - 1]) for m in range(1, 13)]
    month_stats_project = [(m, len(month_count_project.get(m, set())), month_labels[m - 1]) for m in range(1, 13)]

    # 6) 写出 Markdown 报告
    lines = []
    lines.append("# 会议统计报告\n")
    lines.append("## 一、有哪些会议（以 会议名单.csv 为准）\n")
    total = len(namelist_rows)
    lines.append(f"**会议名单** 共 **{total}** 个会议。\n")
    for r in namelist_rows:
        name, desc = r["会议名称"], r["大概召开时间"]
        lines.append(f"- **{name}** — {desc or '—'}")
    lines.append("")
    lines.append(f"其中，在本项目「受影响显著一级行业」中实际纳入统计的会议族群为 **{len(families_in_project)}** 个（来自按月份分类表）。\n")
    lines.append("")

    lines.append("## 二、新旧分类（按历届首次举办年）\n")
    lines.append(f"- **旧会议**（历届首次举办年在 {CUTOFF_YEAR} 年之前）：**{len(old_list)}** 个\n")
    for name, y in old_list:
        lines.append(f"  - {name}（首次 {y} 年）")
    lines.append("")
    lines.append(f"- **新增会议**（历届首次在 {CUTOFF_YEAR} 年及以后）：**{len(new_list)}** 个\n")
    for name, y in new_list:
        lines.append(f"  - {name}（首次 {y} 年）")
    lines.append("")
    if no_year_list:
        lines.append(f"- **历届表中暂无对应届次**：**{len(no_year_list)}** 个\n")
        for name, desc in no_year_list[:20]:  # 最多列 20 个
            lines.append(f"  - {name}")
        if len(no_year_list) > 20:
            lines.append(f"  - … 共 {len(no_year_list)} 个")
    lines.append("")

    lines.append("## 三、是否定时召开\n")
    lines.append("- **定时**：名单中「大概召开时间」为每年/每季度/每届且未标不定期、不固定。\n")
    lines.append("- **不定时**：不定期、日期不固定或未明确固定周期。\n\n")
    lines.append(f"- 定时：**{len(scheduled_list)}** 个\n")
    lines.append(f"- 不定时：**{len(unscheduled_list)}** 个\n")
    lines.append("")

    lines.append("## 四、月份分布\n")
    lines.append("### 4.1 会议名单中「大概召开时间」提到的月份（多少会议涉及该月）\n")
    lines.append("| 月份 | 涉及会议数 |")
    lines.append("|------|------------|")
    for m, cnt, label in month_stats_namelist:
        lines.append(f"| {label}（{m}月） | {cnt} |")
    max_n = max(c for _, c, _ in month_stats_namelist) if month_stats_namelist else 0
    busy = [label for _, c, label in month_stats_namelist if c == max_n and max_n > 0]
    lines.append("")
    if busy:
        lines.append(f"**会议名单中偏多的月份**：{', '.join(busy)}（各有 {max_n} 个会议涉及）。\n")
    lines.append("")
    lines.append("### 4.2 本项目纳入的会议「下一次召开」月份分布\n")
    lines.append("| 月份 | 会议族群数 |")
    lines.append("|------|------------|")
    for m, cnt, label in month_stats_project:
        lines.append(f"| {label}（{m}月） | {cnt} |")
    max_p = max(c for _, c, _ in month_stats_project) if month_stats_project else 0
    busy_p = [label for _, c, label in month_stats_project if c == max_p and max_p > 0]
    lines.append("")
    if busy_p:
        lines.append(f"**本项目会议偏多的月份**：{', '.join(busy_p)}（各有 {max_p} 个会议族群）。\n")
    lines.append("")

    PATH_REPORT.parent.mkdir(parents=True, exist_ok=True)
    with open(PATH_REPORT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"已写入报告：{PATH_REPORT}（会议名单共 {total} 条，本项目纳入 {len(families_in_project)} 个会议族群）")


if __name__ == "__main__":
    main()
