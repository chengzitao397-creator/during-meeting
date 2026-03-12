# -*- coding: utf-8 -*-
"""
复查「受影响显著的一级行业.csv」中「窗口具体日期」列是否与明细表一致。

复查方法：
1. 从 各会议各时间轴一级行业涨跌明细.csv 按与添加脚本相同的规则计算「标准答案」：
   (meeting_family, 窗口) -> 该组合在明细中出现的所有 窗口开始 日期的去重、排序、格式化为 xxxx年xx月xx日 后用顿号拼接。
2. 逐行比对 受影响显著的一级行业.csv 中的 窗口具体日期：拆成日期集合后与标准答案集合比较（忽略顺序）。
3. 汇报：一致行数、不一致行数；若有不一致则列出并展示该 key 在明细中的原始 窗口开始 与 会议名称 样本。
"""

import re
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DETAIL_CSV = BASE_DIR / "会议窗口数据" / "各会议各时间轴一级行业涨跌明细.csv"
TARGET_CSV = BASE_DIR / "会议窗口数据" / "受影响显著的一级行业.csv"


def normalize_meeting_family(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return ""
    if "（" in name:
        name = name.split("（", 1)[0].strip()
    m = re.match(r"^(\d{4})年(.+)$", name)
    if m:
        name = m.group(2).strip()
    for kw1, kw2, fam in [
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
    ]:
        if kw1 in name and (kw2 is None or kw2 in name):
            name = fam
            break
    return name


def date_to_cn(d: str) -> str:
    if pd.isna(d) or not d or not str(d).strip():
        return ""
    s = str(d).strip()[:10]
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        return s
    return f"{s[:4]}年{s[5:7]}月{s[8:10]}日"


def main():
    if not DETAIL_CSV.exists() or not TARGET_CSV.exists():
        print("缺少明细或目标文件，跳过复查。")
        return

    # ---------- 1) 从明细计算标准答案 ----------
    detail = pd.read_csv(DETAIL_CSV, encoding="utf-8")
    for c in ["会议名称", "窗口", "窗口开始"]:
        if c not in detail.columns:
            print(f"[错误] 明细缺少列: {c}")
            return
    detail["meeting_family"] = detail["会议名称"].astype(str).map(normalize_meeting_family)
    detail["日期_cn"] = detail["窗口开始"].astype(str).map(date_to_cn)
    # 标准答案：(meeting_family, 窗口) -> 排序后的日期集合（用 frozenset 便于比较）
    def to_set(ser):
        vals = sorted(set(str(t).strip() for t in ser if str(t).strip()))
        return frozenset(vals), "、".join(vals)

    expected = {}
    for (mf, win), g in detail.groupby(["meeting_family", "窗口"]):
        s, s_str = to_set(g["日期_cn"])
        expected[(mf, win)] = (s, s_str)

    # ---------- 2) 读取目标表，取 窗口具体日期 与 (meeting_family, window) ----------
    df = pd.read_csv(TARGET_CSV, encoding="utf-8-sig")
    # 列名统一
    for c in list(df.columns):
        cstr = str(c).strip()
        if "industry" in cstr.lower() and cstr != "industry":
            df = df.rename(columns={c: "industry"})
        if "meeting_family" in cstr.lower() or ("meeting" in cstr and "family" in cstr):
            df = df.rename(columns={c: "meeting_family"})
        if cstr == "窗口" or ("window" in cstr.lower() and cstr != "window"):
            df = df.rename(columns={c: "window"})
    date_col = None
    for c in df.columns:
        if "窗口具体日期" in str(c) or "具体日期" in str(c):
            date_col = c
            break
    if date_col is None:
        print("[复查] 未找到「窗口具体日期」列，当前列:", list(df.columns))
        print("将仅根据明细生成标准答案并展示若干样本，供人工对照。")
        for (mf, win), (_, s_str) in list(expected.items())[:5]:
            raw = detail[(detail["meeting_family"] == mf) & (detail["窗口"] == win)][["会议名称", "窗口开始"]].drop_duplicates()
            print(f"  样本 ({mf}, {win}) 标准日期串: {s_str[:80]}...")
            print(f"    明细原始: {raw.head(3).to_string()}")
        return
    if "meeting_family" not in df.columns or "window" not in df.columns:
        print("[错误] 目标表缺少 meeting_family 或 window 列")
        return

    # ---------- 3) 逐行比对 ----------
    match_count = 0
    mismatch_rows = []
    for i, row in df.iterrows():
        mf, win = row["meeting_family"], row["window"]
        actual_str = str(row[date_col] or "").strip()
        actual_set = frozenset(t.strip() for t in actual_str.split("、") if t.strip())
        key = (mf, win)
        if key not in expected:
            mismatch_rows.append((i, mf, win, "标准答案中无此 key", actual_str[:60], ""))
            continue
        exp_set, exp_str = expected[key]
        if actual_set == exp_set:
            match_count += 1
        else:
            only_actual = actual_set - exp_set
            only_expected = exp_set - actual_set
            mismatch_rows.append((i, mf, win, f"集合差: 仅文件有{len(only_actual)}个, 仅标准有{len(only_expected)}个", actual_str[:80], exp_str[:80]))

    # ---------- 4) 汇报 ----------
    n = len(df)
    print("=" * 60)
    print("复查方法说明")
    print("=" * 60)
    print("1. 标准答案来源: 各会议各时间轴一级行业涨跌明细.csv")
    print("   - 对每行取 会议名称→normalize→meeting_family, 窗口, 窗口开始→date_to_cn→xxxx年xx月xx日")
    print("   - 按 (meeting_family, 窗口) 分组，组内日期去重、排序、顿号拼接")
    print("2. 比对规则: 受影响显著的一级行业.csv 每行的「窗口具体日期」拆成日期集合，与标准答案集合比较（忽略顺序）")
    print("3. 归一与格式化逻辑与 添加窗口具体日期.py 完全一致")
    print("=" * 60)
    print(f"总行数: {n}")
    print(f"与标准答案一致: {match_count}")
    print(f"不一致或缺失: {len(mismatch_rows)}")
    if mismatch_rows:
        print("\n不一致样本（前 5 条）:")
        for item in mismatch_rows[:5]:
            idx, mf, win, msg, actual, exp = item
            print(f"  行{idx+2} ({mf}, {win}): {msg}")
            print(f"    文件中: {actual}...")
            print(f"    标准值: {exp}...")
        # 对第一条不一致，展示该 key 在明细中的原始 窗口开始
        if mismatch_rows:
            mf, win = mismatch_rows[0][1], mismatch_rows[0][2]
            raw = detail[(detail["meeting_family"] == mf) & (detail["窗口"] == win)][["会议名称", "窗口开始", "窗口结束"]].drop_duplicates()
            print(f"\n  第一条不一致 key 在明细中的原始 窗口开始/结束:")
            print(raw.to_string())
    else:
        print("\n结论: 所有行的「窗口具体日期」与明细表推导结果一致。")
    print("=" * 60)


if __name__ == "__main__":
    main()
