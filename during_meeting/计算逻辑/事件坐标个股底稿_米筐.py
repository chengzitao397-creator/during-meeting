# -*- coding: utf-8 -*-
"""
事件坐标个股底稿：利用 rqdatac 从「上榜行业」生成 (event_id, date, event_time_k, stock_code, ...) 日线底稿。
- 输入：各窗口跑赢上证行业.csv 或任一含 (会议名称, T日, 窗口, 行业名称) 的行业上榜表，提取 (event_id, meeting_date, industry_name)。
- 申万一级行业名 -> 米筐申万行业指数 801xxx.INDX，用 rqdatac.index_components 取历史成分股（当日截面）。
- 日线 [meeting_date±5 交易日]：open, close, total_turnover；market_cap 米筐日线无此字段，输出列留空可后续用 get_fundamentals 补。
- event_time_k：meeting_date 当天为 0，前负后正；is_buyable：T+1 日若 open==close==涨停则 False；daily_return 为前收→今收。
- 行业内 turnover_rank_pct、ret_rank 按 (event_id, event_time_k) 组内计算。
- 输出：stock_event_daily_rq.csv（含 event_id, date, event_time_k, stock_code, industry_name, is_buyable, daily_return, turnover_value, market_cap, turnover_rank_pct, ret_rank）。
依赖：rqdatac，config_local 或环境变量 RQDATA_USERNAME / RQDATA_PASSWORD。
"""
from __future__ import annotations

import os
import sys
import csv
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# 申万一级行业名 -> 米筐申万行业指数代码（用于 get_index_components 取历史成分股）
SW1_INDUSTRY_INDEX = {
    "交通运输": "801170.INDX",
    "传媒": "801760.INDX",
    "农林牧渔": "801010.INDX",
    "医药": "801150.INDX",
    "商贸零售": "801200.INDX",
    "国防军工": "801740.INDX",
    "基础化工": "801030.INDX",
    "家电": "801110.INDX",
    "建材": "801710.INDX",
    "建筑": "801720.INDX",
    "房地产": "801180.INDX",
    "有色金属": "801050.INDX",
    "机械": "801070.INDX",
    "汽车": "801880.INDX",
    "消费者服务": "801210.INDX",
    "煤炭": "801950.INDX",
    "电力及公用事业": "801160.INDX",
    "电力设备及新能源": "801730.INDX",
    "电子": "801080.INDX",
    "石油石化": "801960.INDX",
    "纺织服装": "801130.INDX",
    "综合": "801230.INDX",
    "综合金融": "801190.INDX",
    "计算机": "801750.INDX",
    "轻工制造": "801140.INDX",
    "通信": "801770.INDX",
    "钢铁": "801040.INDX",
    "银行": "801780.INDX",
    "非银行金融": "801790.INDX",
    "食品饮料": "801120.INDX",
}


def _get_rqdata_credentials():
    """从环境变量或 config_local 读取米筐账号。"""
    u = os.environ.get("RQDATA_USERNAME", "").strip()
    p = os.environ.get("RQDATA_PASSWORD", "").strip()
    if u and p:
        return u, p
    base = Path(__file__).resolve().parent.parent
    if str(base) not in sys.path:
        sys.path.insert(0, str(base))
    try:
        import config_local as cfg
        u = getattr(cfg, "RQDATA_USERNAME", "") or ""
        p = getattr(cfg, "RQDATA_PASSWORD", "") or ""
        if u and p:
            return u, p
    except ImportError:
        pass
    return "", ""


def _init_rq():
    import rqdatac
    u, p = _get_rqdata_credentials()
    if u and p:
        rqdatac.init(username=u, password=p)
    else:
        rqdatac.init()
    return rqdatac


def load_event_industry_list(path_csv: str) -> pd.DataFrame:
    """
    读取行业上榜表，提取 (event_id, meeting_date, industry_name)。
    支持列名：会议名称/T日/窗口/行业名称；或 meeting_name, T日, window, industry_name。
    event_id = 会议名称 + '|' + T日 + '|' + 窗口。
    """
    base = Path(__file__).resolve().parent.parent
    p = Path(path_csv) if Path(path_csv).is_absolute() else base / path_csv
    df = pd.read_csv(p, encoding="utf-8")
    # 列名兼容
    col_meeting = "会议名称" if "会议名称" in df.columns else "meeting_name"
    col_t = "T日" if "T日" in df.columns else "meeting_date"
    col_window = "窗口" if "窗口" in df.columns else "window"
    col_ind = "行业名称" if "行业名称" in df.columns else "industry_name"
    for c in [col_meeting, col_t, col_window, col_ind]:
        if c not in df.columns:
            raise ValueError(f"缺少列: {c}，当前列: {list(df.columns)}")
    df = df.dropna(subset=[col_meeting, col_ind])
    df = df.loc[df[col_meeting].astype(str).str.strip() != ""]
    df = df.loc[df[col_ind].astype(str).str.strip() != ""]
    df["event_id"] = df[col_meeting].astype(str).str.strip() + "|" + df[col_t].astype(str).str.strip() + "|" + df[col_window].astype(str).str.strip()
    df["meeting_date"] = pd.to_datetime(df[col_t], errors="coerce").dt.date
    df["industry_name"] = df[col_ind].astype(str).str.strip()
    return df[["event_id", "meeting_date", "industry_name"]].drop_duplicates()


def get_industry_index_code(industry_name: str) -> str | None:
    """申万一级行业名 -> 米筐行业指数代码 801xxx.INDX。"""
    return SW1_INDUSTRY_INDEX.get(industry_name.strip())


def get_historical_components(rq, industry_index_code: str, as_of_date) -> list[str]:
    """获取该行业指数在 as_of_date 的历史成分股 order_book_id 列表。使用 rqdatac.index_components。"""
    try:
        if hasattr(as_of_date, "strftime"):
            dt = as_of_date
        else:
            dt = pd.Timestamp(str(as_of_date)[:10]).date()
        # 米筐 API 为 index_components(order_book_id, date=...)
        api = getattr(rq, "index_components", None) or getattr(rq, "get_index_components", None)
        if api is None:
            return []
        comp = api(industry_index_code, date=dt)
        if comp is None or (isinstance(comp, (list, tuple)) and len(comp) == 0):
            return []
        if isinstance(comp, pd.DataFrame):
            if "order_book_id" in comp.columns:
                return comp["order_book_id"].dropna().astype(str).tolist()
            if "symbol" in comp.columns:
                return comp["symbol"].dropna().astype(str).tolist()
            return comp.index.astype(str).tolist()
        return list(comp)
    except Exception as e:
        print(f"  [WARN] index_components({industry_index_code}, date={as_of_date}) 失败: {e}")
        return []


def get_trading_dates_rq(rq, start_date, end_date) -> list:
    """交易日列表，元素转为 date。"""
    dates = rq.get_trading_dates(start_date=start_date, end_date=end_date)
    if dates is None:
        return []
    return [d.date() if hasattr(d, "date") and callable(getattr(d, "date")) else d for d in dates]


def get_price_multi(rq, order_book_ids: list, start_date, end_date, fields: list) -> dict[str, pd.DataFrame]:
    """拉取多标的多字段日线，返回 {field: DataFrame(index=date, columns=order_book_id)}。"""
    if not order_book_ids:
        return {f: pd.DataFrame() for f in fields}
    try:
        df = rq.get_price(
            order_book_ids,
            start_date=start_date,
            end_date=end_date,
            frequency="1d",
            fields=fields,
            expect_df=True,
        )
        if df is None or df.empty:
            return {f: pd.DataFrame() for f in fields}
        # long format: index 多为 (order_book_id, datetime)，columns = [open, close, ...]
        if isinstance(df.index, pd.MultiIndex):
            df = df.reset_index()
            # 找出日期列和标的列（米筐常见为 order_book_id + 日期层）
            date_col = None
            ob_col = None
            for c in df.columns:
                if c in ("date", "datetime", "time") or (hasattr(df[c].dtype, "name") and "datetime" in str(df[c].dtype)):
                    date_col = c
                if c in ("order_book_id", "symbol") or "order_book" in str(c).lower():
                    ob_col = c
            if date_col is None and len(df.index) > 0:
                date_col = df.columns[1] if len(df.columns) > 2 else df.columns[0]
            if ob_col is None:
                ob_col = df.columns[0]
            out = {}
            for f in fields:
                if f not in df.columns:
                    out[f] = pd.DataFrame()
                    continue
                if date_col and ob_col:
                    sub = df.pivot_table(index=date_col, columns=ob_col, values=f)
                else:
                    sub = pd.DataFrame()
                try:
                    sub.index = pd.DatetimeIndex(sub.index).date
                except Exception:
                    pass
                out[f] = sub
            return out
        return {f: pd.DataFrame() for f in fields}
    except Exception as e:
        print(f"  [WARN] get_price 失败: {e}")
        return {f: pd.DataFrame() for f in fields}


def run(
    path_industry_csv: str = "会议窗口数据/各窗口跑赢上证行业.csv",
    path_out: str = "会议窗口数据/stock_event_daily_rq.csv",
    trading_window: int = 5,
    limit_events: int | None = None,
):
    """
    主流程：读上榜行业 -> 历史成分股 -> 日线 -> event_time_k / is_buyable / daily_return / 排名 -> 导出 CSV。
    limit_events: 若为 None 则处理全部事件；设为整数可仅处理前 N 个 event_id（试跑时建议 3～5）。
    """
    base = Path(__file__).resolve().parent.parent
    path_out = Path(path_out) if Path(path_out).is_absolute() else base / path_out
    path_out.parent.mkdir(parents=True, exist_ok=True)

    events_df = load_event_industry_list(path_industry_csv)
    if events_df.empty:
        print("未解析到任何 (event_id, meeting_date, industry_name)")
        return
    if limit_events is not None:
        u = events_df["event_id"].unique()[:limit_events]
        events_df = events_df.loc[events_df["event_id"].isin(u)]
    print(f"已加载 {len(events_df)} 条 (event, industry)，涉及 {events_df['event_id'].nunique()} 个事件。")

    rq = _init_rq()
    trading_dates_all = get_trading_dates_rq(rq, "2000-01-01", "2030-12-31")
    if not trading_dates_all:
        print("获取交易日历失败")
        return

    rows_out = []
    # 米筐 get_price 日线不支持 market_cap，仅拉取 open/close/total_turnover；market_cap 可后续用 get_fundamentals 补
    fields = ["open", "close", "total_turnover"]
    # 涨停容差（开盘/收盘达到昨日收盘*1.099 视为涨停，10%）
    limit_pct = 0.10
    tol = 1e-6

    for idx, row in events_df.iterrows():
        event_id = row["event_id"]
        meeting_date = row["meeting_date"]
        industry_name = row["industry_name"]
        if pd.isna(meeting_date):
            continue
        if isinstance(meeting_date, str):
            meeting_date = datetime.strptime(meeting_date[:10], "%Y-%m-%d").date()
        ind_code = get_industry_index_code(industry_name)
        if not ind_code:
            continue
        comp = get_historical_components(rq, ind_code, meeting_date)
        if not comp:
            continue
        # 交易日 [meeting_date - trading_window, meeting_date + trading_window]
        try:
            i = trading_dates_all.index(meeting_date)
        except ValueError:
            i = min(range(len(trading_dates_all)), key=lambda j: abs((trading_dates_all[j] - meeting_date).days if hasattr(trading_dates_all[j], "days") else 0))
            meeting_date = trading_dates_all[i]
        i_lo = max(0, i - trading_window)
        i_hi = min(len(trading_dates_all) - 1, i + trading_window)
        # 多取前一日以便计算首日 daily_return（前收→今收）
        start_d = trading_dates_all[i_lo - 1] if i_lo >= 1 else trading_dates_all[i_lo]
        end_d = trading_dates_all[i_hi]
        price_data = get_price_multi(rq, comp, start_d, end_d, fields)
        if price_data["close"].empty:
            continue
        close_df = price_data["close"]
        open_df = price_data["open"]
        turn_df = price_data["total_turnover"]
        cap_df = pd.DataFrame()  # 米筐日线无 market_cap，输出时填空
        dates_sorted = sorted(close_df.index.tolist())
        try:
            k0_idx = dates_sorted.index(meeting_date)
        except ValueError:
            k0_idx = 0
        # 仅保留 [meeting_date - trading_window, meeting_date + trading_window] 内的交易日
        dates_in_window = [d for d in dates_sorted if d >= trading_dates_all[i_lo] and d <= end_d]
        for d in dates_in_window:
            event_time_k = dates_sorted.index(d) - k0_idx
            for stock in close_df.columns:
                try:
                    c = close_df.loc[d, stock]
                    o = open_df.loc[d, stock] if stock in open_df.columns else c
                    t = turn_df.loc[d, stock] if stock in turn_df.columns else 0
                    cap = cap_df.loc[d, stock] if not cap_df.empty and stock in cap_df.columns else np.nan
                except Exception:
                    continue
                if pd.isna(c) or c <= 0:
                    continue
                # daily_return: 用前收 -> 今收（dates_sorted 已含多取的前一日）
                prev_close = None
                idx_in_full = dates_sorted.index(d)
                if idx_in_full >= 1:
                    prev_d = dates_sorted[idx_in_full - 1]
                    try:
                        prev_close = close_df.loc[prev_d, stock]
                    except Exception:
                        pass
                if prev_close is None or prev_close <= 0:
                    daily_return = np.nan
                else:
                    daily_return = c / prev_close - 1.0
                # is_buyable: 若为 T+1 日（event_time_k==1），检查是否开盘即封死涨停
                is_buyable = True
                if event_time_k == 1 and prev_close and prev_close > 0 and not pd.isna(o) and o > 0:
                    limit_up = prev_close * (1 + limit_pct)
                    if abs(o - limit_up) < tol and abs(c - limit_up) < tol:
                        is_buyable = False
                rows_out.append({
                    "event_id": event_id,
                    "date": d,
                    "event_time_k": event_time_k,
                    "stock_code": stock,
                    "industry_name": industry_name,
                    "is_buyable": is_buyable,
                    "daily_return": round(daily_return, 6) if not pd.isna(daily_return) else "",
                    "turnover_value": round(float(t), 0) if not pd.isna(t) else "",
                    "market_cap": round(float(cap), 0) if not pd.isna(cap) else "",
                })
        if (idx + 1) % 200 == 0:
            print(f"  已处理 {idx + 1} 行 …")

    if not rows_out:
        print("无日线数据可写")
        return
    df_out = pd.DataFrame(rows_out)
    # 行业内排名：每个 (event_id, event_time_k) 组内
    df_out["turnover_rank_pct"] = np.nan
    df_out["ret_rank"] = np.nan
    for (eid, k), g in df_out.groupby(["event_id", "event_time_k"]):
        if g["turnover_value"].notna().any():
            r = g["turnover_value"].rank(pct=True, ascending=False)
            df_out.loc[g.index, "turnover_rank_pct"] = r.round(4)
        if g["daily_return"].notna().any():
            r = g["daily_return"].rank(ascending=False, method="min")
            df_out.loc[g.index, "ret_rank"] = r.astype(int)
    # 输出列顺序
    out_cols = [
        "event_id", "date", "event_time_k", "stock_code", "industry_name",
        "is_buyable", "daily_return", "turnover_value", "market_cap",
        "turnover_rank_pct", "ret_rank",
    ]
    df_out = df_out[out_cols]
    df_out.to_csv(path_out, index=False, encoding="utf-8-sig")
    print(f"已写入 {path_out}，共 {len(df_out)} 行。")


if __name__ == "__main__":
    # 试跑：limit_events=3；全量跑可改为 limit_events=None
    run(
        path_industry_csv="会议窗口数据/各窗口跑赢上证行业.csv",
        path_out="会议窗口数据/stock_event_daily_rq.csv",
        trading_window=5,
        limit_events=3,
    )
