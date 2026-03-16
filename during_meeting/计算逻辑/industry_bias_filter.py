# -*- coding: utf-8 -*-
"""
读取「一级行业在会议期间不同交易时间轴下的涨跌次数统计表」，
筛选出受会议影响显著、涨跌明显偏向一边的行业，输出全量汇总与过滤结果。

过滤规则：|up_rate - 0.5| >= 0.20（即 up_rate<=0.30 或 up_rate>=0.70），且 total >= 6。
"""

from pathlib import Path
import pandas as pd

# ============ 列名映射（按你的表头修改） ============
COL_INDUSTRY = "industry"           # 行业列名
COL_UP_COUNT = "up_count"           # 上涨次数列名
COL_DOWN_COUNT = "down_count"       # 下跌次数列名
COL_AXIS = "window"                 # 可选：时间轴列名，没有则设为 None
COL_MEETING = "meeting_family"      # 可选：会议类型/名称列名，没有则设为 None

# ============ 路径与阈值 ============
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_CSV = BASE_DIR / "会议窗口数据" / "会议族群_各时间轴行业涨跌统计.csv"
OUTPUT_DIR = BASE_DIR / "会议窗口数据"
OUTPUT_ALL = OUTPUT_DIR / "industry_bias_all.csv"
OUTPUT_FILTERED = OUTPUT_DIR / "industry_bias_filtered.csv"

MIN_BIAS = 0.20   # |up_rate - 0.5| >= 此值保留
MIN_TOTAL = 6     # total >= 此值保留


def main():
    # 读取
    if not INPUT_CSV.exists():
        print(f"[错误] 未找到输入文件: {INPUT_CSV}")
        return
    df = pd.read_csv(INPUT_CSV, encoding="utf-8")

    # 必选列检查
    for col, name in [(COL_INDUSTRY, "industry"), (COL_UP_COUNT, "up_count"), (COL_DOWN_COUNT, "down_count")]:
        if col and col not in df.columns:
            print(f"[错误] 缺少列: {col}（配置的{name}列名）")
            print(f"  当前表头: {list(df.columns)}")
            return

    # 分组列：行业 + 可选 axis / meeting
    group_cols = [c for c in [COL_INDUSTRY, COL_AXIS, COL_MEETING] if c and c in df.columns]
    agg_df = (
        df.groupby(group_cols, dropna=False)
        .agg(
            up_count=(COL_UP_COUNT, "sum"),
            down_count=(COL_DOWN_COUNT, "sum"),
        )
        .reset_index()
    )
    agg_df["total"] = agg_df["up_count"] + agg_df["down_count"]
    agg_df["up_rate"] = agg_df["up_count"] / agg_df["total"].replace(0, pd.NA)
    agg_df["down_rate"] = agg_df["down_count"] / agg_df["total"].replace(0, pd.NA)
    agg_df["bias_from_50"] = (agg_df["up_rate"] - 0.5).abs()

    # 全量汇总写出
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    agg_df.to_csv(OUTPUT_ALL, index=False, encoding="utf-8-sig")
    print(f"已写出全量汇总: {OUTPUT_ALL}，共 {len(agg_df)} 行。")

    # 过滤：|up_rate - 0.5| >= 0.20 且 total >= 6
    mask = (agg_df["bias_from_50"] >= MIN_BIAS) & (agg_df["total"] >= MIN_TOTAL)
    filtered = agg_df.loc[mask].copy()
    filtered["direction"] = filtered["up_rate"].apply(
        lambda x: "mostly_up" if x >= 0.70 else "mostly_down"
    )
    filtered = filtered.sort_values("bias_from_50", ascending=False).reset_index(drop=True)
    filtered.to_csv(OUTPUT_FILTERED, index=False, encoding="utf-8-sig")

    print(f"筛选阈值: |up_rate-0.5|>={MIN_BIAS}, total>={MIN_TOTAL}")
    print(f"已写出过滤结果: {OUTPUT_FILTERED}，保留 {len(filtered)} 行（按 bias_from_50 降序）。")


if __name__ == "__main__":
    main()
