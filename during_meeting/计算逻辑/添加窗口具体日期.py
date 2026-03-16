# -*- coding: utf-8 -*-
"""
在「受影响显著的一级行业.csv」前加一列：该 (meeting_family, window) 对应的交易时间轴具体日期，
格式 xxxx年xx月xx日；历届多天用顿号分隔。
"""

import re
from pathlib import Path
import pandas as pd
from collections import defaultdict

BASE_DIR = Path(__file__).resolve().parent.parent
DETAIL_CSV = BASE_DIR / "会议窗口数据" / "各会议各时间轴一级行业涨跌明细.csv"
INPUT_CSV = BASE_DIR / "会议窗口数据" / "受影响显著的一级行业.csv"
OUTPUT_CSV = BASE_DIR / "会议窗口数据" / "受影响显著的一级行业.csv"


def normalize_meeting_family(name: str) -> str:
    """与会议族群统计脚本一致的归一逻辑。"""
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


def date_to_cn(d: str) -> str:
    """2022-11-08 -> 2022年11月08日"""
    if pd.isna(d) or not d or not str(d).strip():
        return ""
    s = str(d).strip()[:10]
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        return s
    return f"{s[:4]}年{s[5:7]}月{s[8:10]}日"


def main():
    if not DETAIL_CSV.exists():
        print(f"[错误] 未找到明细: {DETAIL_CSV}")
        return
    if not INPUT_CSV.exists():
        print(f"[错误] 未找到: {INPUT_CSV}")
        return

    # 明细中 (meeting_family, 窗口) -> 去重后的 窗口开始 日期列表
    detail = pd.read_csv(DETAIL_CSV, encoding="utf-8")
    for c in ["会议名称", "窗口", "窗口开始"]:
        if c not in detail.columns:
            print(f"[错误] 明细缺少列: {c}")
            return

    detail["meeting_family"] = detail["会议名称"].astype(str).map(normalize_meeting_family)
    detail["日期_cn"] = detail["窗口开始"].astype(str).map(date_to_cn)
    # 按 (meeting_family, 窗口) 聚合成「顿号分隔的日期串」
    key_dates = (
        detail.groupby(["meeting_family", "窗口"])["日期_cn"]
        .apply(lambda x: "、".join(sorted(set(str(t) for t in x if str(t).strip()))))
        .to_dict()
    )

    # 读取受影响显著表（兼容 BOM/乱码表头）
    df = pd.read_csv(INPUT_CSV, encoding="utf-8-sig")
    # 统一列名：首列若含 industry 则改为 industry，确保 meeting_family / window 存在
    rename_map = {}
    for c in df.columns:
        cstr = str(c).strip()
        if "industry" in cstr.lower() and cstr != "industry":
            rename_map[c] = "industry"
        if "meeting_family" in cstr.lower() or ("meeting" in cstr and "family" in cstr):
            rename_map[c] = "meeting_family"
        if cstr == "窗口" or (cstr != "window" and "window" in cstr.lower()):
            rename_map[c] = "window"
    df = df.rename(columns=rename_map)
    if "meeting_family" not in df.columns or "window" not in df.columns:
        print("[错误] 表需包含 meeting_family 与 window 列。当前列:", list(df.columns))
        return

    df["窗口具体日期"] = df.apply(
        lambda r: key_dates.get((r["meeting_family"], r["window"]), ""),
        axis=1,
    )
    # 第一列固定为「窗口具体日期」，其余保持原顺序
    rest = [c for c in df.columns if c != "窗口具体日期"]
    df = df[["窗口具体日期"] + rest]
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"已在「受影响显著的一级行业.csv」首列加入「窗口具体日期」，已覆盖: {OUTPUT_CSV}，共 {len(df)} 行。")


if __name__ == "__main__":
    main()
