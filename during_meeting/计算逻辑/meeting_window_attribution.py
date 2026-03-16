# -*- coding: utf-8 -*-
"""
基于 industry_bias_filtered.csv 做定性归因：
判断每个会议在不同窗口对一级行业的联系更像「预期(pre)/消息释放(event)/后续影响(post)」。
不引入统计检验，仅做定性归因。
"""

import re
from pathlib import Path
import pandas as pd

# ============ 可配置参数 ============
DELTA = 0.05   # 显著差距阈值：strongest_strength >= second_strength + DELTA 才标为单一归因，否则 mixed/unclear

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_CSV = BASE_DIR / "会议窗口数据" / "industry_bias_filtered.csv"
OUTPUT_CSV = BASE_DIR / "会议窗口数据" / "meeting_window_story.csv"
OUTPUT_MD = BASE_DIR / "会议窗口数据" / "meeting_family_summary.md"


def parse_event_time_group(window_str: str) -> str:
    """将 window 解析为 event_time_group: pre / event / post。"""
    if pd.isna(window_str) or not isinstance(window_str, str):
        return "event"
    s = window_str.strip().upper()
    # T-5, T-1, T+1, T+5 或 T+数字 / T-数字
    m = re.match(r"T([+-])(\d+)", s)
    if m:
        k = int(m.group(2)) if m.group(1) == "+" else -int(m.group(2))
        if k < 0:
            return "pre"
        if k > 0:
            return "post"
        return "event"
    if s == "T" or s == "T0":
        return "event"
    return "event"


def main():
    if not INPUT_CSV.exists():
        print(f"[错误] 未找到输入文件: {INPUT_CSV}")
        return

    df = pd.read_csv(INPUT_CSV, encoding="utf-8")
    for col in ["industry", "window", "meeting_family", "bias_from_50", "direction"]:
        if col not in df.columns:
            print(f"[错误] 缺少列: {col}")
            return

    df["event_time_group"] = df["window"].astype(str).map(parse_event_time_group)

    # 对每个 (meeting_family, industry) 按 pre/event/post 取最大 bias 及对应 window、direction
    rows_out = []
    for (meeting_family, industry), g in df.groupby(["meeting_family", "industry"], dropna=False):
        pre = g.loc[g["event_time_group"] == "pre"]
        ev = g.loc[g["event_time_group"] == "event"]
        post = g.loc[g["event_time_group"] == "post"]

        def max_row(sub):
            if sub.empty:
                return 0.0, "", ""
            idx = sub["bias_from_50"].idxmax()
            r = sub.loc[idx]
            return r["bias_from_50"], r["window"], r["direction"]

        pre_strength, pre_window, pre_dir = max_row(pre)
        event_strength, event_window, event_dir = max_row(ev)
        post_strength, post_window, post_dir = max_row(post)

        # 三者排序取最强与次强
        triple = [
            ("pre", pre_strength, pre_window, pre_dir),
            ("event", event_strength, event_window, event_dir),
            ("post", post_strength, post_window, post_dir),
        ]
        triple_sorted = sorted(triple, key=lambda x: x[1], reverse=True)
        strongest_group = triple_sorted[0][0]
        strongest_strength = triple_sorted[0][1]
        strongest_window = triple_sorted[0][2]
        direction = triple_sorted[0][3]
        second_strength = triple_sorted[1][1]

        if strongest_strength >= second_strength + DELTA:
            label = strongest_group
        else:
            label = "mixed/unclear"

        # 中文标签便于 narrative
        label_cn = {"pre": "预期(T-n)", "event": "消息释放(T)", "post": "后续影响(T+n)"}.get(label, label)
        narrative = (
            f"{meeting_family} 对 {industry}：{label_cn}主导（{direction}），"
            f"最强窗口={strongest_window}，强度={strongest_strength:.4f}；"
            f"另外两组强度分别为 pre={pre_strength:.4f}, event={event_strength:.4f}, post={post_strength:.4f}。"
        )

        rows_out.append({
            "meeting_family": meeting_family,
            "industry": industry,
            "label": label,
            "direction": direction,
            "strongest_window": strongest_window,
            "strongest_strength": strongest_strength,
            "pre_strength": pre_strength,
            "event_strength": event_strength,
            "post_strength": post_strength,
            "narrative": narrative,
        })

    story = pd.DataFrame(rows_out)

    # 写出 meeting_window_story.csv
    story.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"已写出: {OUTPUT_CSV}，共 {len(story)} 行。")

    # 写出 meeting_family_summary.md
    lines = [
        "# 会议窗口定性归因汇总（按 meeting_family）",
        "",
        f"显著差距阈值 delta = {DELTA}。",
        "",
    ]
    for meeting_family, sg in story.groupby("meeting_family", dropna=False):
        lines.append(f"## {meeting_family}")
        lines.append("")
        # a) 各 label 行业数量占比
        cnt = sg["label"].value_counts()
        n = len(sg)
        lines.append("### 归因标签分布")
        lines.append("| 标签 | 行业数 | 占比 |")
        lines.append("|------|--------|------|")
        for lb in ["pre", "event", "post", "mixed/unclear"]:
            c = cnt.get(lb, 0)
            pct = c / n * 100 if n else 0
            lines.append(f"| {lb} | {c} | {pct:.1f}% |")
        lines.append("")
        # b) strongest_strength 均值/中位数
        lines.append("### 最强强度")
        lines.append(f"- 均值: {sg['strongest_strength'].mean():.4f}")
        lines.append(f"- 中位数: {sg['strongest_strength'].median():.4f}")
        lines.append("")
        # c) Top10 行业及 narrative
        lines.append("### strongest_strength Top10 行业及 narrative")
        top10 = sg.nlargest(10, "strongest_strength")
        for _, r in top10.iterrows():
            lines.append(f"- **{r['industry']}** (强度={r['strongest_strength']:.4f})：{r['narrative']}")
        lines.append("")
        lines.append("---")
        lines.append("")

    OUTPUT_MD.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"已写出: {OUTPUT_MD}。")


if __name__ == "__main__":
    main()
