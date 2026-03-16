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
import os
from datetime import datetime
from typing import Tuple, Optional
import pandas as pd
import streamlit as st

# 项目根目录（web 的上一级）。有的仓库结构为：
#   /.../during_meeting/web
#   /.../during_meeting/output
# 也有结构：
#   /.../during-meeting/during_meeting/web
#   /.../during-meeting/output
# 优先使用存在的 output 目录。
CUR = Path(__file__).resolve()
BASE = CUR.parent.parent
ALT_BASE = CUR.parents[2] if len(CUR.parents) >= 3 else BASE

# 支持环境变量 DATA_DIR 指定数据目录
data_dir_env = os.getenv("DATA_DIR")
if data_dir_env:
    OUT_DIR = Path(data_dir_env)
else:
    OUT_DIR = None
    for candidate in (BASE / "output", ALT_BASE / "output"):
        if candidate.exists():
            OUT_DIR = candidate
            break
    if OUT_DIR is None:
        OUT_DIR = BASE / "output"

# 月度 CSV 按年份放在 output/{年}/ 下，文件名：受影响显著一级行业_{年}M{月}_中信30.csv
FILE_PATTERN = re.compile(r"受影响显著一级行业_(\d{4})M(\d{2})_中信30\.csv")
# 年份范围：网站始终可选的年份（即使暂无该年数据也显示，避免“空空如也”）
YEAR_MIN, YEAR_MAX = 2022, 2027


def scan_available_months():
    """扫描 output 下按年子目录中的月度 CSV，返回 [(year, month), ...] 按年月排序。"""
    if not OUT_DIR.exists():
        return []
    out = []
    for year_dir in OUT_DIR.iterdir():
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        y = int(year_dir.name)
        for f in year_dir.iterdir():
            if not f.is_file() or f.suffix.lower() != ".csv":
                continue
            m = FILE_PATTERN.match(f.name)
            if m:
                mo = int(m.group(2))
                if 1 <= mo <= 12:
                    out.append((y, mo))
    out.sort(key=lambda x: (x[0], x[1]))
    return out


def load_month_data(year: int, month: int) -> Tuple[Optional[pd.DataFrame], Optional[float]]:
    """加载指定年月的月度表（从 output/{年}/ 下读取）。返回 (DataFrame 或 None, 文件修改时间戳或 None)。"""
    fname = f"受影响显著一级行业_{year}M{month:02d}_中信30.csv"
    path = OUT_DIR / str(year) / fname
    if not path.exists():
        return None, None
    try:
        mtime = os.path.getmtime(path) if path.exists() else None
        df = pd.read_csv(path, encoding="utf-8-sig")
        return df, mtime
    except Exception:
        return None, None


def run():
    st.set_page_config(page_title="月度会议查询", page_icon="📅", layout="wide")
    st.title("月度会议与显著行业查询")
    st.caption("选择年份和月份，查看该月会议、受影响显著行业及历届涨跌表现与幅度。")

    available = scan_available_months()
    # 年份：固定范围 2022–2027，保证每年都可选（暂无数据的年/月会显示“暂无数据”）
    years = list(range(YEAR_MIN, YEAR_MAX + 1))
    year_labels = {y: f"{y}年" for y in years}
    month_labels = {m: f"{m}月" for m in range(1, 13)}

    col1, col2, col3, _ = st.columns([1, 1, 1, 2])
    with col1:
        year_sel = st.selectbox("年份", options=years, format_func=lambda y: year_labels.get(y, str(y)))
    with col2:
        # 月份：始终 1–12，无数据的月份也会显示“本月暂无显著会议/行业数据”
        month_sel = st.selectbox(
            "月份",
            options=list(range(1, 13)),
            format_func=lambda m: month_labels.get(m, str(m)),
            key="month",
        )
    with col3:
        if st.button("🔄 重新加载数据", help="若刚运行过「按年按月份拆表」脚本，点此加载最新 CSV"):
            st.rerun()

    df, file_mtime = load_month_data(year_sel, month_sel)
    # 无数据时用空表 + 提示，不空白页面
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "meeting_family", "下一次召开_具体日期", "window", "industry",
            "届数", "上涨次数", "下跌次数", "上涨概率", "下跌概率", "平均涨跌幅",
        ])
        st.info(f"**{year_sel}年{month_sel}月** 暂无显著会议/行业数据；该月可能无会议或尚未生成该年月的数据。")
    else:
        # 显示数据文件更新时间，便于确认是否读到最新
        if file_mtime is not None:
            mod_time = datetime.fromtimestamp(file_mtime).strftime("%Y-%m-%d %H:%M")
            st.caption(f"📁 数据文件更新时间：{mod_time}。若刚重新运行过「按年按月份拆表」脚本，请 **刷新本页（F5）** 或 **重新选一次年月** 以加载最新数据。")

    # 摘要
    meetings = df["meeting_family"].dropna().unique().tolist()
    n_meetings = len(meetings)
    n_rows = len(df)
    st.success(f"**{year_sel}年{month_sel}月** 共 **{n_meetings}** 场会议，**{n_rows}** 条「会议×窗口×行业」显著记录。")

    tab1, tab2 = st.tabs(["按会议", "总表"])

    with tab1:
        st.subheader("按会议查看")
        if not meetings:
            st.caption("本月暂无显著会议/行业数据。")
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
            # 展示列：含持平次数/概率（若存在）
            show_cols = ["window", "industry", "届数", "上涨次数", "下跌次数", "持平次数", "上涨概率", "下跌概率", "持平概率", "平均涨跌幅"]
            show_cols = [c for c in show_cols if c in block.columns]
            st.dataframe(block[show_cols], use_container_width=True, hide_index=True)
            st.divider()

    with tab2:
        st.subheader("总表（可排序、筛选）")
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.caption("数据来源：output/{年}/受影响显著一级行业_{年}M{月}_中信30.csv，由「按年按月份拆表」脚本生成。届数、涨跌概率与平均涨跌幅按「过去进行时」计算（选某年则仅用该年及以前的届次）。")


if __name__ == "__main__":
    run()
