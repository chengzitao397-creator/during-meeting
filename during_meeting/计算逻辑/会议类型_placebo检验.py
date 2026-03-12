# -*- coding: utf-8 -*-
"""
会议类型 Placebo 检验：特定会议类型下，行业历届上榜次数是否显著高于随机。

输入：明细表 df，每行 = 会议×窗口×行业。
输出：result_industry（按 p_value 升序、topN_count_real 降序），及轻量日志。
含：真实统计、随机基准、p_value；并列/缺失处理规则见代码与日志。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional

# 默认列名映射（若 df 为中文列名则自动识别）
COL_MAP = {
    "meeting_name": ["会议名称", "meeting_name"],
    "window": ["窗口", "window"],
    "industry_name": ["行业名称", "industry_name"],
    "diff": ["差值", "diff"],
}


def _normalize_columns(df: pd.DataFrame, col_meeting: Optional[str], col_window: Optional[str], col_industry: Optional[str], col_diff: Optional[str]) -> pd.DataFrame:
    """将 df 列名统一为 meeting_name, window, industry_name, diff（仅复制需要的列，避免改原 df）。"""
    out = df.copy()
    mapping = {}
    for std_name, candidates in COL_MAP.items():
        if std_name == "meeting_name" and col_meeting:
            mapping[col_meeting] = "meeting_name"
        elif std_name == "window" and col_window:
            mapping[col_window] = "window"
        elif std_name == "industry_name" and col_industry:
            mapping[col_industry] = "industry_name"
        elif std_name == "diff" and col_diff:
            mapping[col_diff] = "diff"
        else:
            for c in candidates:
                if c in out.columns:
                    mapping[c] = std_name
                    break
    out = out.rename(columns=mapping)
    required = {"meeting_name", "window", "industry_name", "diff"}
    missing = required - set(out.columns)
    if missing:
        raise ValueError(f"列名映射后缺少列: {missing}。请通过 col_meeting/col_window/col_industry/col_diff 传入正确列名。")
    return out[list(required)]


def _get_topN_per_event(
    sub: pd.DataFrame,
    N: int,
    event_id_col: str,
    log_events: Optional[list] = None,
) -> pd.Series:
    """
    对 sub 按 event_id 分组，每组内按 diff 降序取 TopN 行业（缺省剔除，有效行业数<N 则取满并记录）。
    返回：每个 (event_id, industry_name) 是否上榜的 Series（index 为 industry_name 在 event 内，这里改为返回每届上榜的 industry_name 列表的汇总）。
    实际返回：该 sub 下每个行业在本组内的上榜次数（通常 sub 是一届，所以每届每行业最多 1 次）→ 返回该届上榜的 industry_name 集合更合适，由上层汇总。
    """
    # 剔除 diff 缺失
    sub = sub.dropna(subset=["diff"])
    sub = sub.loc[sub["industry_name"].astype(str).str.strip() != ""]
    events_topn = {}
    for eid, g in sub.groupby(event_id_col, sort=False):
        g = g.sort_values("diff", ascending=False).reset_index(drop=True)
        n_valid = len(g)
        k = min(N, n_valid)
        if log_events is not None and n_valid < N:
            log_events.append((eid, n_valid, N))
        topn_industries = g["industry_name"].iloc[:k].tolist()
        events_topn[eid] = topn_industries
    return events_topn


def run_placebo_test(
    df: pd.DataFrame,
    key_meeting: str,
    key_window: str = "T-5",
    N: int = 10,
    B: int = 1000,
    *,
    col_meeting: Optional[str] = None,
    col_window: Optional[str] = None,
    col_industry: Optional[str] = None,
    col_diff: Optional[str] = None,
    seed: int = 42,
) -> tuple[pd.DataFrame, dict]:
    """
    执行 Placebo 检验，返回 (result_industry, log_dict)。

    参数:
        df: 明细表，每行=会议×窗口×行业；列名可为中文（会议名称/窗口/行业名称/差值）或英文，自动识别。
        key_meeting: 会议类型关键词，meeting_name 包含即视为该类型。
        key_window: 窗口标签，如 'T-5'。
        N: TopN 上榜数，默认 10。
        B: 随机抽样次数，默认 1000。
        col_meeting, col_window, col_industry, col_diff: 若 df 列名与默认不符，可传入实际列名覆盖自动识别。
        seed: 随机种子，便于复现。
    """
    df = _normalize_columns(df, col_meeting, col_window, col_industry, col_diff)
    # 剔除空行（如组间空行）
    df = df.dropna(subset=["meeting_name", "industry_name"], how="all")
    df = df.loc[df["meeting_name"].astype(str).str.strip() != ""]
    df = df.loc[df["industry_name"].astype(str).str.strip() != ""]
    df["diff"] = pd.to_numeric(df["diff"], errors="coerce")

    rng = np.random.default_rng(seed)
    log_events_short_N: list = []

    # ---------- A) 真实统计 ----------
    real = df.loc[
        df["meeting_name"].astype(str).str.contains(key_meeting, na=False)
        & (df["window"].astype(str).str.strip() == key_window)
    ].copy()
    if real.empty:
        raise ValueError(f"筛选 key_meeting 包含 '{key_meeting}' 且 window=='{key_window}' 后无数据。")

    events_real = real["meeting_name"].unique().tolist()
    N_events = len(events_real)
    event_to_topn = _get_topN_per_event(real, N, "meeting_name", log_events_short_N)

    # 行业层面：上榜次数、diff 均值/中位数、win_rate
    topn_count_real = pd.Series(dtype=int)
    diff_records: dict[str, list] = {}  # industry -> list of diff in real events
    for eid, topn_list in event_to_topn.items():
        for ind in topn_list:
            topn_count_real[ind] = topn_count_real.get(ind, 0) + 1
        ev_df = real.loc[real["meeting_name"] == eid]
        for _, row in ev_df.iterrows():
            ind = row["industry_name"]
            if ind not in diff_records:
                diff_records[ind] = []
            diff_records[ind].append(row["diff"])

    all_industries = real["industry_name"].unique().tolist()
    n_industries = len(all_industries)
    for ind in all_industries:
        if ind not in topn_count_real.index:
            topn_count_real[ind] = 0
        if ind not in diff_records:
            diff_records[ind] = []

    topn_count_real = topn_count_real.reindex(all_industries, fill_value=0)
    topn_rate_real = topn_count_real / N_events
    mean_diff_real = pd.Series({ind: np.mean(diff_records[ind]) if diff_records[ind] else np.nan for ind in all_industries})
    median_diff_real = pd.Series({ind: np.median(diff_records[ind]) if diff_records[ind] else np.nan for ind in all_industries})
    win_rate_real = pd.Series({
        ind: np.mean(np.array(diff_records[ind]) > 0) if diff_records[ind] else np.nan
        for ind in all_industries
    })

    # ---------- B) Placebo 抽样池与抽样 ----------
    pool = df.loc[df["window"].astype(str).str.strip() == key_window]
    pool_events = pool["meeting_name"].unique().tolist()
    pool_size = len(pool_events)
    replace = pool_size < N_events
    if replace and pool_size > 0:
        log_replace = "是（抽样池事件数 < N_events，已放回抽样）"
    else:
        log_replace = "否"

    # 每个事件实例的 (meeting_name, 该届的 industry->diff 或 该届 topN 名单)：我们只需要每届的 topN 名单即可
    def get_event_topn_list(ev_name: str) -> list:
        ev_df = pool.loc[pool["meeting_name"] == ev_name].dropna(subset=["diff"])
        ev_df = ev_df.loc[ev_df["industry_name"].astype(str).str.strip() != ""]
        ev_df = ev_df.sort_values("diff", ascending=False)
        k = min(N, len(ev_df))
        return ev_df["industry_name"].iloc[:k].tolist()

    event_to_topn_pool = {ev: get_event_topn_list(ev) for ev in pool_events}

    # B 次抽样，每次得到各行业 topN_count_fake
    fake_counts_by_industry: dict[str, list] = {ind: [] for ind in all_industries}
    for _ in range(B):
        if replace:
            chosen = rng.choice(pool_events, size=N_events, replace=True)
        else:
            chosen = rng.choice(pool_events, size=N_events, replace=False)
        count_this = pd.Series(dtype=int)
        for ev in chosen:
            for ind in event_to_topn_pool.get(ev, []):
                count_this[ind] = count_this.get(ind, 0) + 1
        for ind in all_industries:
            fake_counts_by_industry[ind].append(count_this.get(ind, 0))

    topn_count_fake_mean = pd.Series({ind: np.mean(fake_counts_by_industry[ind]) for ind in all_industries})
    topn_count_fake_p95 = pd.Series({ind: np.percentile(fake_counts_by_industry[ind], 95) for ind in all_industries})
    # p_value = (#{fake >= real} + 1) / (B + 1)
    p_value = pd.Series({
        ind: (np.sum(np.array(fake_counts_by_industry[ind]) >= topn_count_real[ind]) + 1) / (B + 1)
        for ind in all_industries
    })

    # ---------- C) 结果表 ----------
    result_industry = pd.DataFrame({
        "industry_name": all_industries,
        "N_events": N_events,
        "topN_count_real": topn_count_real.values,
        "topN_rate_real": topn_rate_real.values,
        "mean_diff_real": mean_diff_real.values,
        "median_diff_real": median_diff_real.values,
        "win_rate_real": win_rate_real.values,
        "topN_count_fake_mean": topn_count_fake_mean.values,
        "topN_count_fake_p95": topn_count_fake_p95.values,
        "p_value": p_value.values,
    })
    result_industry = result_industry.sort_values(
        by=["p_value", "topN_count_real"],
        ascending=[True, False],
    ).reset_index(drop=True)

    # ---------- 日志 ----------
    log_dict = {
        "key_meeting": key_meeting,
        "key_window": key_window,
        "N_events": N_events,
        "抽样池大小": pool_size,
        "B": B,
        "是否放回抽样": log_replace,
        "TopN的N": N,
        "n_industries": n_industries,
        "当届有效行业数小于N的届": log_events_short_N,
    }

    # ---------- 验证 1) Spot check 一届会议 Top15 ----------
    one_event = events_real[0]
    spot = real.loc[real["meeting_name"] == one_event].dropna(subset=["diff"]).sort_values("diff", ascending=False).reset_index(drop=True)
    spot = spot.head(15).copy()
    spot["rank"] = np.arange(1, len(spot) + 1)
    log_dict["spot_check_一届"] = one_event
    log_dict["spot_check_top15"] = spot[["industry_name", "diff", "rank"]].to_dict("records")

    # ---------- 验证 2) topN_count_real 之和 ----------
    sum_topn = result_industry["topN_count_real"].sum()
    # 每届取 min(N, 当届有效行业数)，所以理论和 = sum over events of min(N, n_valid)
    theoretical_sum = 0
    for eid in events_real:
        ev_df = real.loc[real["meeting_name"] == eid].dropna(subset=["diff"])
        n_valid = len(ev_df)
        theoretical_sum += min(N, n_valid)
    log_dict["sum_topN_count_real"] = sum_topn
    log_dict["theoretical_sum_topN_slots"] = theoretical_sum
    log_dict["count_consistency_ok"] = abs(sum_topn - theoretical_sum) < 0.01

    # ---------- 验证 3) Placebo 均值 ≈ N_events * (N / n_industries) ----------
    expected_fake_mean = N_events * (N / n_industries)
    actual_fake_mean_avg = result_industry["topN_count_fake_mean"].mean()
    log_dict["expected_fake_mean_approx"] = expected_fake_mean
    log_dict["actual_fake_mean_avg"] = actual_fake_mean_avg
    log_dict["placebo_mean_reasonable"] = abs(actual_fake_mean_avg - expected_fake_mean) / (expected_fake_mean + 1e-8) < 0.5

    return result_industry, log_dict


def print_log(log_dict: dict) -> None:
    """打印轻量日志与验证信息。"""
    print("===== Placebo 检验 日志 =====")
    print(f"key_meeting: {log_dict['key_meeting']}")
    print(f"key_window: {log_dict['key_window']}")
    print(f"N_events: {log_dict['N_events']}")
    print(f"抽样池大小: {log_dict['抽样池大小']}")
    print(f"B: {log_dict['B']}")
    print(f"是否放回抽样: {log_dict['是否放回抽样']}")
    print(f"TopN 的 N: {log_dict['TopN的N']}")
    if log_dict.get("当届有效行业数小于N的届"):
        print("【提示】以下届别当届有效行业数 < N，该届 TopN 按有效行业数取满：", log_dict["当届有效行业数小于N的届"][:5], "..." if len(log_dict["当届有效行业数小于N的届"]) > 5 else "")
    print("----- 验证 1) Spot check 一届 Top15 -----")
    print("届:", log_dict.get("spot_check_一届"))
    for r in log_dict.get("spot_check_top15", [])[:15]:
        print(r)
    print("----- 验证 2) 计数一致性 -----")
    print(f"sum(topN_count_real) = {log_dict['sum_topN_count_real']}, 理论总上榜槽位 = {log_dict['theoretical_sum_topN_slots']}, 一致: {log_dict['count_consistency_ok']}")
    print("----- 验证 3) Placebo 均值合理性 -----")
    print(f"期望约 N_events*N/n_industries = {log_dict['expected_fake_mean_approx']:.4f}, 实际随机均值平均 = {log_dict['actual_fake_mean_avg']:.4f}, 合理: {log_dict['placebo_mean_reasonable']}")


def load_df_from_csv(path: str) -> pd.DataFrame:
    """从 各窗口跑赢上证行业 风格 CSV 加载，并过滤空行。列名保持中文，run_placebo_test 会自动映射。"""
    base = Path(__file__).resolve().parent.parent
    p = Path(path) if Path(path).is_absolute() else base / path
    df = pd.read_csv(p, encoding="utf-8")
    # 去掉组间空行（会议名称或行业名称为空）
    meeting_col = [c for c in df.columns if "会议" in c][:1]
    if meeting_col:
        df = df.loc[df[meeting_col[0]].astype(str).str.strip() != ""]
    industry_col = [c for c in df.columns if "行业" in c and "名称" in c][:1]
    if industry_col:
        df = df.loc[df[industry_col[0]].astype(str).str.strip() != ""]
    return df


def save_result_as_frequency_name(result_industry: pd.DataFrame, key_window: str, key_meeting: str, out_dir: str = "会议窗口数据") -> Path:
    """将 result_industry 保存为「(对应窗口)的一级行业(对应会议)上榜频率.csv」。"""
    base = Path(__file__).resolve().parent.parent
    dir_path = base / out_dir
    dir_path.mkdir(parents=True, exist_ok=True)
    # 文件名：T-5的一级行业G20领导人峰会上榜频率.csv（括号内按实际 key_window、key_meeting 填充）
    safe_meeting = "".join(c for c in key_meeting if c not in r'\/:*?"<>|')
    fname = f"{key_window}的一级行业{safe_meeting}上榜频率.csv"
    out_path = dir_path / fname
    result_industry.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


def _meeting_name_to_type(name: str) -> str:
    """
    将单届会议名称归一化为「会议类型」，用于做时间的纵向对比：同一会议历届放在一起统计。
    - 去掉末尾括号及内容：G20领导人峰会（2022）→ G20领导人峰会；央行例会（2023-Q1）→ 央行例会
    - 去掉开头 YYYY年：2022年两会→ 两会；2013年中央经济工作会议→ 中央经济工作会议
    - 去掉开头 第X届：第三届中国国际进口博览会→ 中国国际进口博览会
    唯一的时间维度是窗口（T-5、T-1、T、T+1、T+5），不是会议年份或届次。
    """
    import re
    if not name or not str(name).strip():
        return ""
    key = str(name).strip()
    key = re.sub(r"[（(][^）)]*[）)]\s*$", "", key).strip()  # 末尾（含年份/季度）括号
    key = re.sub(r"^\d{4}年", "", key).strip()  # 开头 YYYY年
    # 开头 第X届 或 X届（含十六届、十七届、二十届等）
    key = re.sub(r"^第?[一二三四五六七八九十百千\d]+届\s*", "", key).strip()
    return key


def get_all_meeting_types_and_windows(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    """
    从 df 中解析出所有「会议类型」与「窗口」列表。
    会议类型 = 同一种会议的历届归一化名（两会、中央经济工作会议、G20领导人峰会等），
    筛「meeting_name 包含 key_meeting」时会把该类型所有届放在一起，N_events = 历届总数。
    """
    meeting_col = "meeting_name" if "meeting_name" in df.columns else "会议名称"
    window_col = "window" if "window" in df.columns else "窗口"
    meeting_names = df[meeting_col].dropna().astype(str).str.strip().unique()
    key_meetings = set()
    for name in meeting_names:
        key = _meeting_name_to_type(name)
        if key:
            key_meetings.add(key)
    windows = df[window_col].dropna().astype(str).str.strip().unique().tolist()
    windows = sorted([w for w in windows if w])
    return sorted(key_meetings), windows


def run_all_placebo_and_save(
    df: pd.DataFrame,
    N: int = 10,
    B: int = 1000,
    out_dir: str = "会议窗口数据",
    min_events: int = 1,
    seed: int = 42,
) -> list[Path]:
    """
    对「所有会议类型 × 所有窗口」跑 placebo，并将结果保存为「(窗口)的一级行业(会议)上榜频率.csv」。
    会议类型 = 同一种会议历届合并（两会、中央经济工作会议等），N_events = 该类型历届总数；时间维度仅为窗口 T-5/T-1 等。
    若某组合下 N_events < min_events 则跳过。返回已保存文件路径列表。
    """
    key_meetings, windows = get_all_meeting_types_and_windows(df)
    saved: list[Path] = []
    total = len(key_meetings) * len(windows)
    n = 0
    for key_meeting in key_meetings:
        for key_window in windows:
            n += 1
            try:
                result_industry, log_dict = run_placebo_test(
                    df, key_meeting=key_meeting, key_window=key_window, N=N, B=B, seed=seed
                )
                if log_dict["N_events"] < min_events:
                    continue
                out_path = save_result_as_frequency_name(result_industry, key_window, key_meeting, out_dir)
                saved.append(out_path)
                print(f"[{n}/{total}] {key_window} + {key_meeting} -> N_events={log_dict['N_events']} 已保存 {out_path.name}")
            except Exception as e:
                print(f"[{n}/{total}] {key_window} + {key_meeting} 跳过: {e}")
    return saved


if __name__ == "__main__":
    path_csv = "会议窗口数据/各窗口跑赢上证行业.csv"
    df = load_df_from_csv(path_csv)

    # 全量：所有会议类型 × 所有窗口，各保存一份「(窗口)的一级行业(会议)上榜频率.csv」
    # 批量时 B=500 以控制耗时；单次重要会议可用 run_placebo_test(..., B=1000) 再算一遍
    print("===== 全量 Placebo：所有会议类型 × 所有窗口 =====\n")
    saved_paths = run_all_placebo_and_save(df, N=10, B=500, out_dir="会议窗口数据", min_events=1)
    print(f"\n共保存 {len(saved_paths)} 个文件。")
