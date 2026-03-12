# -*- coding: utf-8 -*-
"""
会议窗口行业数据 - 相对开盘价口径（米筐）：
- 窗口内每日收益 r_t = (close_t - open_t) / open_t，再按复利累积 (1+r1)*(1+r2)*...*(1+rn) - 1。
- 与 会议窗口行业数据_米筐.py 的会议、窗口、行业一致，仅涨跌幅计算方式不同。
- 输出：时间/会议窗口行业数据_相对开盘价.csv（结构与 会议窗口行业数据.csv 相同）。
"""
from pathlib import Path
import csv
import sys

# 复用会议、交易日、行情拉取与行业列表（避免重复维护）
from 会议窗口行业数据_米筐 import (
    parse_meeting_dates,
    get_trading_dates_rq,
    get_price_rq,
    _get_rqdata_credentials,
    SW30_INDICES,
    MARKET_INDEX,
    BENCHMARK_INDEX,
)

# 复利累积：(1+r1)*...*(1+rn)-1，忽略 NaN
from cumret import cumret as cumret_chain


def cumret_vs_open(opens_df, closes_df, oid, day_list):
    """
    相对开盘价的窗口累积收益：窗口内每日 r_t = (close-open)/open，再连乘 (1+r_t)-1。
    opens_df / closes_df：index=日期，columns=标的；oid 为列名；day_list 为窗口内交易日列表。
    返回 float 或 None（无有效日时）。
    """
    if not day_list or oid not in closes_df.columns or oid not in opens_df.columns:
        return None
    r_list = []
    for d in day_list:
        if d not in opens_df.index or d not in closes_df.index:
            continue
        o = opens_df.loc[d, oid]
        c = closes_df.loc[d, oid]
        if getattr(o, "__float__", None) is None or o == 0:
            continue
        try:
            r = (float(c) - float(o)) / float(o)
            r_list.append(r)
        except (TypeError, ValueError):
            continue
    if not r_list:
        return None
    cr, _ = cumret_chain(r_list)
    return cr if cr == cr else None  # 排除 nan


def run():
    base = Path(__file__).resolve().parent.parent
    path_meetings = base / "时间" / "会议历届时间.csv"
    path_out = base / "时间" / "会议窗口行业数据_相对开盘价.csv"

    meetings = parse_meeting_dates(path_meetings)
    if not meetings:
        print("未解析到任何会议日期")
        return

    try:
        import rqdatac
        u, p = _get_rqdata_credentials()
        if u and p:
            rqdatac.init(username=u, password=p)
        else:
            rqdatac.init()
    except Exception as e:
        print("请先安装并配置 rqdatac；在 config_local.py 或环境变量中设置 RQDATA_USERNAME、RQDATA_PASSWORD。", e)
        return

    from datetime import datetime, timedelta
    all_dates = set()
    for _, t_str in meetings:
        t = datetime.strptime(t_str, "%Y-%m-%d").date()
        for k in range(-30, 30):
            all_dates.add(t + timedelta(days=k))
    start_date = min(all_dates).isoformat()
    end_date = max(all_dates).isoformat()

    trading = get_trading_dates_rq(start_date, end_date)
    if not trading:
        print("获取交易日历失败")
        return
    trading = [d.date() if hasattr(d, "date") and callable(getattr(d, "date")) else d for d in trading]

    ids = [MARKET_INDEX, BENCHMARK_INDEX] + [code for _, code in SW30_INDICES]
    opens = get_price_rq(ids, start_date, end_date, field="open")
    closes = get_price_rq(ids, start_date, end_date, field="close")
    if opens is None or opens.empty or closes is None or closes.empty:
        print("获取 open/close 行情失败")
        return

    # 与 会议窗口行业数据_米筐 相同的窗口定义
    rows_out = []
    for name, t_str in meetings:
        t_date = datetime.strptime(t_str, "%Y-%m-%d").date()
        if t_date not in trading:
            for d in trading:
                if d >= t_date:
                    t_date = d
                    break
            else:
                continue
        try:
            i = trading.index(t_date)
        except ValueError:
            continue
        i_m5 = max(0, i - 5)
        i_m1 = max(0, i - 1)
        i_p1 = min(len(trading) - 1, i + 1)
        i_p5 = min(len(trading) - 1, i + 5)
        d_m5, d_m1, d_t, d_p1, d_p5 = trading[i_m5], trading[i_m1], trading[i], trading[i_p1], trading[i_p5]

        windows = [
            ("T-5", d_m5, d_m5, (trading[i_m5 - 1], d_m5) if i_m5 >= 1 else (d_m5, d_m5)),
            ("T-1", d_m1, d_m1, (trading[i_m1 - 1], d_m1) if i_m1 >= 1 else (d_m1, d_m1)),
            ("T", d_t, d_t, (trading[i - 1], d_t) if i >= 1 else (d_t, d_t)),
            ("T+1", d_p1, d_p1, (trading[i_p1 - 1], d_p1) if i_p1 >= 1 else (d_p1, d_p1)),
            ("T+5", d_p1, d_p5, (d_p1, d_p5)),
        ]
        for tag, d_start, d_end, (ret_start, ret_end) in windows:
            window_days = [d for d in trading if ret_start <= d <= ret_end]
            if not window_days:
                continue
            # 市场、基准：相对开盘价累积
            market_ret = cumret_vs_open(opens, closes, MARKET_INDEX, window_days) if MARKET_INDEX in closes.columns else None
            benchmark_ret = cumret_vs_open(opens, closes, BENCHMARK_INDEX, window_days) if BENCHMARK_INDEX in closes.columns else None
            up_cnt, down_cnt = 0, 0
            industry_rets = []
            for ind_name, oid in SW30_INDICES:
                if oid not in closes.columns:
                    continue
                r = cumret_vs_open(opens, closes, oid, window_days)
                if r is not None:
                    industry_rets.append((ind_name, round(r, 6)))
                    if r > 0:
                        up_cnt += 1
                    elif r < 0:
                        down_cnt += 1
            row = {
                "会议名称": name,
                "T日": t_str,
                "窗口": tag,
                "窗口开始": str(d_start),
                "窗口结束": str(d_end),
                "行业上涨家数": up_cnt,
                "行业下跌家数": down_cnt,
                "市场涨跌幅": round(market_ret, 6) if market_ret is not None else "",
                "上证指数涨跌幅": round(benchmark_ret, 6) if benchmark_ret is not None else "",
                "行业成交额_合计": "",
                "换手率": "",
            }
            row["行业涨跌幅"] = "; ".join(f"{n}:{r}" for n, r in industry_rets)
            rows_out.append(row)

    if not rows_out:
        print("未计算出任何会议窗口数据（相对开盘价）")
        return

    fieldnames = ["会议名称", "T日", "窗口", "窗口开始", "窗口结束", "行业上涨家数", "行业下跌家数", "市场涨跌幅", "上证指数涨跌幅", "行业成交额_合计", "换手率", "行业涨跌幅"]
    with open(path_out, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)
    print(f"已写入 {path_out}，共 {len(rows_out)} 行（相对开盘价口径）。")


if __name__ == "__main__":
    run()
