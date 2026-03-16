# -*- coding: utf-8 -*-
"""
将 模版.xlsx 与 计算结果.xlsx 中涉及的会议合并为一张表，
仅保留两列：历届会议名称、时间。
"""
import pandas as pd
from pathlib import Path

def main():
    # 项目根目录；计算结果与模版在根目录，输出到「时间」文件夹
    base = Path(__file__).resolve().parent.parent
    dir_time = base / "时间"

    # 计算结果.xlsx 中的「会议时间」表已有历届会议名称与起止日期
    df_calc = pd.read_excel(base / "计算结果.xlsx", sheet_name="会议时间")
    # 统一列名并只保留两列：历届会议名称、时间
    df_calc = df_calc.rename(columns={"event_name": "历届会议名称"})
    # 时间列：用「开始日期 至 结束日期」表示
    start = pd.to_datetime(df_calc["start_date"]).dt.strftime("%Y-%m-%d")
    end = pd.to_datetime(df_calc["end_date"]).dt.strftime("%Y-%m-%d")
    df_calc["时间"] = start + " 至 " + end
    merged = df_calc[["历届会议名称", "时间"]].copy()

    # 模版.xlsx 中的会议类型（无具体历届时间），追加到第一列后面，时间列留空
    df_tpl = pd.read_excel(base / "模版.xlsx", sheet_name="event_industry_freq_detail_norm", header=0)
    template_events = df_tpl["event_norm"].unique().tolist()
    extra = pd.DataFrame({"历届会议名称": template_events, "时间": [""] * len(template_events)})
    merged = pd.concat([merged, extra], ignore_index=True)

    out_path = dir_time / "历届会议名称与时间.xlsx"
    merged.to_excel(out_path, index=False, sheet_name="会议列表")
    print(f"已生成合并表，共 {len(merged)} 条，保存至: {out_path}")
    return merged

if __name__ == "__main__":
    main()
