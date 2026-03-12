# -*- coding: utf-8 -*-
"""
月度会议查询：选年份+月份，查看该月有哪些会议、影响显著的行业、历届涨跌表现与幅度。

运行：在项目根目录执行
  streamlit run web/app_月度会议查询.py
或在 web 目录下执行
  streamlit run app_月度会议查询.py

支持两种视图：按会议分组、总表。
"""
from pathlib import Path
import re
import pandas as pd
import streamlit as st

# 项目根目录（web 的上一级）与月度 CSV 所在目录
BASE = Path(__file__).resolve().parent.parent
OUT_DIR = BASE / "output"
# 月度文件命名：受影响显著一级行业_{年}M{月}_中信30.csv
FILE_PATTERN = re.compile(r"受影响显著一级行业_(\d{4})M(\d{2})_中信30\.csv")


def scan_available_months():
    """扫描 output 下所有月度 CSV，返回 [(year, month), ...] 按年月排序。"""
    if not OUT_DIR.exists():
        return []
    out = []
    for f in OUT_DIR.iterdir():
        if not f.is_file() or f.suffix.lower() != ".csv":
            continue
        m = FILE_PATTERN.match(f.name)
        if m:
            y, mo = int(m.group(1)), int(m.group(2))
            if 1 <= mo <= 12:
                out.append((y, mo))
    out.sort(key=lambda x: (x[0], x[1]))
    return out


def load_month_data(year: int, month: int) -> pd.DataFrame | None:
    """加载指定年月的月度表，不存在返回 None。"""
    fname = f"受影响显著一级行业_{year}M{month:02d}_中信30.csv"
    path = OUT_DIR / fname
    if not path.exists():
        return None
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return None


def run():
    st.set_page_config(page_title="月度会议查询", page_icon="📅", layout="wide")
    st.title("月度会议与显著行业查询")
    st.caption("选择年份和月份，查看该月会议、受影响显著行业及历届涨跌表现与幅度。")

    available = scan_available_months()
    if not available:
        st.warning(f"未在 {OUT_DIR} 下找到任何月度数据文件（命名：受影响显著一级行业_年份M月份_中信30.csv）。请先运行「按月份拆表」脚本生成数据。")
        return

    # 年份、月份选项（仅展示有数据的年月）
    years = sorted(set(y for y, _ in available))
    year_labels = {y: f"{y}年" for y in years}
    month_labels = {m: f"{m}月" for m in range(1, 13)}

    col1, col2, _ = st.columns([1, 1, 2])
    with col1:
        year_sel = st.selectbox("年份", options=years, format_func=lambda y: year_labels.get(y, str(y)))
    with col2:
        # 该年有数据的月份
        months_in_year = [m for y, m in available if y == year_sel]
        if not months_in_year:
            month_sel = 1
            st.selectbox("月份", options=[1], format_func=lambda m: month_labels.get(m, str(m)), key="month")
        else:
            month_sel = st.selectbox(
                "月份",
                options=sorted(months_in_year),
                format_func=lambda m: month_labels.get(m, str(m)),
                key="month",
            )

    df = load_month_data(year_sel, month_sel)
    if df is None or df.empty:
        st.info(f"{year_sel}年{month_sel}月暂无数据。")
        return

    # 摘要
    meetings = df["meeting_family"].dropna().unique().tolist()
    n_meetings = len(meetings)
    n_rows = len(df)
    st.success(f"**{year_sel}年{month_sel}月** 共 **{n_meetings}** 场会议，**{n_rows}** 条「会议×窗口×行业」显著记录。")

    tab1, tab2 = st.tabs(["按会议", "总表"])

    with tab1:
        st.subheader("按会议查看")
        # 按会议分组，每组展示：会议名、具体日期（取第一条）、该会下的行业明细表
        for meeting in meetings:
            block = df[df["meeting_family"] == meeting]
            date_str = ""
            if "下一次召开_具体日期" in block.columns:
                first_date = block["下一次召开_具体日期"].dropna().iloc[0] if len(block) else ""
                if pd.notna(first_date) and str(first_date).strip():
                    date_str = str(first_date).strip()
            title = f"**{meeting}**"
            if date_str:
                title += f"（预计 {date_str}）"
            st.markdown(title)
            # 展示列：window, industry, 届数, 上涨次数, 下跌次数, 上涨概率, 下跌概率, 平均涨跌幅
            show_cols = ["window", "industry", "届数", "上涨次数", "下跌次数", "上涨概率", "下跌概率", "平均涨跌幅"]
            show_cols = [c for c in show_cols if c in block.columns]
            st.dataframe(block[show_cols], use_container_width=True, hide_index=True)
            st.divider()

    with tab2:
        st.subheader("总表（可排序、筛选）")
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.caption("数据来源：output/受影响显著一级行业_{年}M{月}_中信30.csv，由「按月份拆表」脚本生成。")


if __name__ == "__main__":
    run()
