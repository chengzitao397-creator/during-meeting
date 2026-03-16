# -*- coding: utf-8 -*-
"""
梳理「会议名单 → 会议窗口行业数据 → 受影响显著最终版」流水线，
输出每类会议被保留或筛掉的详细原因（历届表是否收录、显著性是否通过）。
与 生成受影响显著一级行业_中信30.py 使用相同的 normalize 与筛选阈值。
"""
from pathlib import Path
import csv
import re
from collections import defaultdict
from typing import Optional

BASE = Path(__file__).resolve().parent.parent
PATH_HISTORY = BASE / "会议历届时间_副本.csv"
PATH_WINDOW = BASE / "output" / "会议窗口行业数据_中信30.csv"
PATH_FINAL = BASE / "output" / "受影响显著的一级行业_中信30_最终版.csv"
PATH_NAMELIST = BASE / "会议名单.csv"
PATH_REPORT = BASE / "流水线会议筛选原因_报告.md"

MIN_BIAS = 0.20
MIN_TOTAL = 6


# 与 生成受影响显著一级行业_中信30.py 完全一致
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


def list_name_to_family(list_name: str) -> Optional[str]:
    """会议名单中的名称 → 可能对应的 meeting_family（用于和流水线对照）。"""
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
        return "广交会春季"  # 历届表分春/秋，任取一个代表
    if "中国国际进口博览会" in list_name or "进博会" in list_name:
        return "中国国际进口博览会"
    if "证监会系统" in list_name:
        return "证监会系统工作会议"
    # 仅中国央行货币政策委员会例会；欧洲/英国/日本央行不映射到本项目族群
    if "中国人民银行货币政策" in list_name or ("央行货币政策" in list_name and "中国" in list_name):
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


def main():
    # ------ 1) 历届时间表：出现过的会议族群 ------
    families_in_history = set()
    if PATH_HISTORY.exists():
        with open(PATH_HISTORY, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = (row.get("会议名称") or "").strip()
                if not name:
                    continue
                fam = normalize_meeting_family(name)
                if fam:
                    families_in_history.add(fam)
    # 广交会：历届表用 广交会春季/广交会秋季，名单里统称广交会
    if "广交会春季" in families_in_history or "广交会秋季" in families_in_history:
        families_in_history.add("广交会春季")
        families_in_history.add("广交会秋季")

    # ------ 2) 会议窗口行业数据：按 (fam, window, industry) 聚合 up/down ------
    agg = defaultdict(lambda: {"up": 0, "down": 0})
    families_in_window = set()
    if PATH_WINDOW.exists():
        with open(PATH_WINDOW, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                meeting = (row.get("会议名称") or "").strip()
                window = (row.get("窗口") or "").strip()
                detail_str = (row.get("行业涨跌明细") or "").strip()
                if not meeting or not window or not detail_str:
                    continue
                fam = normalize_meeting_family(meeting)
                if not fam:
                    continue
                families_in_window.add(fam)
                for part in detail_str.split(";"):
                    part = part.strip()
                    if ":" not in part:
                        continue
                    name, flag = part.split(":", 1)
                    name, flag = name.strip(), flag.strip()
                    key = (fam, window, name)
                    if flag == "涨":
                        agg[key]["up"] += 1
                    elif flag == "跌":
                        agg[key]["down"] += 1

    # 每个 meeting_family 的「最佳」total / bias 及是否通过
    family_stats = {}
    for (fam, win, ind), v in agg.items():
        up, down = v["up"], v["down"]
        total = up + down
        if total == 0:
            continue
        up_rate = up / total
        bias = abs(up_rate - 0.5)
        if fam not in family_stats:
            family_stats[fam] = {"max_total": 0, "max_bias": 0.0, "pass_count": 0}
        st = family_stats[fam]
        st["max_total"] = max(st["max_total"], total)
        st["max_bias"] = max(st["max_bias"], round(bias, 4))
        if total >= MIN_TOTAL and bias >= MIN_BIAS:
            st["pass_count"] += 1

    # ------ 3) 最终版：通过的 16 个会议族群 ------
    families_in_final = set()
    if PATH_FINAL.exists():
        with open(PATH_FINAL, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                fam = (row.get("meeting_family") or "").strip()
                if fam:
                    families_in_final.add(fam)

    # ------ 4) 会议名单：每条对应的 family 及筛掉原因 ------
    namelist_rows = []
    if PATH_NAMELIST.exists():
        with open(PATH_NAMELIST, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = (row.get("会议名称") or "").strip()
                if name:
                    namelist_rows.append(name)

    # 构建「名单名称 → 筛掉原因」
    reason_by_list_name = {}
    for list_name in namelist_rows:
        fam = list_name_to_family(list_name)
        if fam is None:
            reason_by_list_name[list_name] = "未映射到流水线会议族群（名单名称与历届/窗口用名无对应）"
            continue
        # 广交会：名单一条对应春+秋两个族群
        check_fams = [fam]
        if fam == "广交会春季":
            check_fams = ["广交会春季", "广交会秋季"]
        in_window = any(f in families_in_window for f in check_fams)
        in_final = any(f in families_in_final for f in check_fams)
        if not in_window:
            # 历届表可能没有该 family（或名称归一后对不上）
            in_hist = any(f in families_in_history for f in check_fams)
            if not in_hist:
                reason_by_list_name[list_name] = "未进入流水线：会议历届时间_副本中无该会议届次（或名称无法归一为本项目会议族群）"
            else:
                reason_by_list_name[list_name] = "历届表有届次但未出现在会议窗口行业数据中（可能为数据生成脚本未跑或时间范围未覆盖）"
        elif not in_final:
            st = family_stats.get(check_fams[0]) or family_stats.get(check_fams[-1] if len(check_fams) > 1 else "")
            if not st:
                reason_by_list_name[list_name] = "已进入窗口数据，但无有效(窗口,行业)涨跌统计（可能全为平）"
            else:
                reason = f"已进入窗口数据，显著性未通过：要求「总届次≥{MIN_TOTAL}」且「|上涨概率−0.5|≥{MIN_BIAS}」。该族群下无任一(窗口,行业)同时满足；其中最大总届次={st['max_total']}，最大偏度={st['max_bias']:.2f}。"
                reason_by_list_name[list_name] = reason
        else:
            reason_by_list_name[list_name] = "通过：进入最终版（网站展示）"

    # ------ 5) 写出报告 ------
    lines = []
    lines.append("# 流水线会议筛选原因报告\n")
    lines.append("## 一、流水线步骤\n")
    lines.append("1. **会议历届时间_副本.csv**：所有「会议名称 + 时间」的届次，用于确定 T 日。\n")
    lines.append("2. **会议窗口行业数据_米筐_中信30.py**：对历届表中每一届，按 T 日与窗口（T-5/T-1/T/T+1/T+5）取中信一级行业涨跌，生成 **会议窗口行业数据_中信30.csv**。\n")
    lines.append("3. **生成受影响显著一级行业_中信30.py**：将窗口数据按 **(会议族群, 窗口, 行业)** 聚合，计算上涨次数/下跌次数、上涨概率、偏度；**筛选条件**：`总届次 ≥ 6` 且 `|上涨概率 − 0.5| ≥ 0.20`。通过者写入 **受影响显著的一级行业_中信30_最终版.csv**。\n")
    lines.append("4. **按年按月份拆表** 与网站展示均只使用 **最终版** 中的会议族群（共 **{}** 个）。\n".format(len(families_in_final)))
    lines.append("")
    lines.append("## 二、筛选阈值（与 生成受影响显著一级行业_中信30.py 一致）\n")
    lines.append("- **MIN_TOTAL** = 6：每个 (会议族群, 窗口, 行业) 至少 6 届有涨/跌记录才参与筛选。\n")
    lines.append("- **MIN_BIAS** = 0.20：|上涨概率 − 0.5| ≥ 0.20 才视为涨跌方向显著。\n")
    lines.append("")
    lines.append("## 三、历届表与窗口数据中的会议族群\n")
    lines.append("- **历届时间表中出现过的会议族群**（归一后）：{} 个。\n".format(len(families_in_history)))
    lines.append("- **会议窗口行业数据中出现的会议族群**：{} 个。\n".format(len(families_in_window)))
    lines.append("- **最终版（网站展示）中的会议族群**：**{}** 个。\n".format(len(families_in_final)))
    lines.append("")
    lines.append("### 通过筛选的 16 个会议族群\n")
    for fam in sorted(families_in_final):
        lines.append("- " + fam)
    lines.append("")
    lines.append("### 进入窗口数据但未通过显著性筛选的会议族群\n")
    failed = sorted(families_in_window - families_in_final)
    if not failed:
        lines.append("（无：凡进入窗口数据的族群均至少有一个 (窗口,行业) 通过筛选。）\n")
    else:
        for fam in failed:
            st = family_stats.get(fam, {})
            max_total = st.get("max_total", 0)
            max_bias = st.get("max_bias", 0)
            lines.append("- **{}**：最大总届次={}，最大偏度={:.2f}；不满足「总届次≥6 且 偏度≥0.20」的组合数不足。".format(fam, max_total, max_bias))
        lines.append("")
    lines.append("")
    lines.append("## 四、会议名单中各条目的筛选结果与原因\n")
    lines.append("| 会议名称（名单） | 结果 | 原因说明 |")
    lines.append("|------------------|------|----------|")
    for list_name in namelist_rows:
        reason = reason_by_list_name.get(list_name, "—")
        # 仅当明确写「通过：进入最终版」时标为通过
        result = "通过" if reason.strip().startswith("通过：进入最终版") else "未展示"
        lines.append("| {} | {} | {} |".format(list_name.replace("|", "｜"), result, reason.replace("|", "｜")))
    lines.append("")
    lines.append("## 五、汇总\n")
    passed_count = sum(1 for r in reason_by_list_name.values() if str(r).strip().startswith("通过：进入最终版"))
    lines.append("- 会议名单共 **{}** 条；其中映射到「通过最终版」的 **{}** 条（对应 16 个会议族群，多对一）。\n".format(len(namelist_rows), passed_count))
    lines.append("- 未展示原因主要为：**历届表无该会议届次**、**有届次但无(窗口,行业)同时满足届次≥6 且 偏度≥0.20**、或**名单名称未映射到本项目会议族群**。\n")

    PATH_REPORT.parent.mkdir(parents=True, exist_ok=True)
    with open(PATH_REPORT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"已写入：{PATH_REPORT}")


if __name__ == "__main__":
    main()
