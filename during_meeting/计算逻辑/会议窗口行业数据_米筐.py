# -*- coding: utf-8 -*-
"""
会议窗口 A 股行业数据（米筐 rqdatac）：
- 以会议召开第一天为 T 日，取 T-5、T-1、T、T+1、T+5 五个时点；
- T±n 的涨跌幅用累积收益（区间收益）；
- 输出：行业涨跌个数、行业涨跌幅、市场涨跌幅、行业成交额、换手率。

依赖：pip install rqdatac
账号密码：环境变量 RQDATA_USERNAME / RQDATA_PASSWORD，或项目根目录 config_local.py 中配置。
"""
from pathlib import Path
import csv
import os
import sys
import pandas as pd


def _get_rqdata_credentials():
    """从环境变量或 config_local.py 读取米筐账号，返回 (username, password)。"""
    u = os.environ.get("RQDATA_USERNAME", "").strip()
    p = os.environ.get("RQDATA_PASSWORD", "").strip()
    if u and p:
        return u, p
    # 尝试项目根目录的 config_local.py（勿提交到 git）
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

# 申万一级行业（30 类，与模版口径一致）— 米筐指数代码 .INDX
# 若你使用中信 30 行业，需替换为中信对应指数或成分聚合
SW30_INDICES = [
    ("交通运输", "801170.INDX"),
    ("传媒", "801760.INDX"),
    ("农林牧渔", "801010.INDX"),
    ("医药", "801150.INDX"),
    ("商贸零售", "801200.INDX"),
    ("国防军工", "801740.INDX"),
    ("基础化工", "801030.INDX"),
    ("家电", "801110.INDX"),
    ("建材", "801710.INDX"),
    ("建筑", "801720.INDX"),
    ("房地产", "801180.INDX"),
    ("有色金属", "801050.INDX"),
    ("机械", "801070.INDX"),
    ("汽车", "801880.INDX"),
    ("消费者服务", "801210.INDX"),
    ("煤炭", "801950.INDX"),
    ("电力及公用事业", "801160.INDX"),
    ("电力设备及新能源", "801730.INDX"),
    ("电子", "801080.INDX"),
    ("石油石化", "801960.INDX"),
    ("纺织服装", "801130.INDX"),
    ("综合", "801230.INDX"),
    ("综合金融", "801190.INDX"),  # 或 非银+银行 合并
    ("计算机", "801750.INDX"),
    ("轻工制造", "801140.INDX"),
    ("通信", "801770.INDX"),
    ("钢铁", "801040.INDX"),
    ("银行", "801780.INDX"),
    ("非银行金融", "801790.INDX"),
    ("食品饮料", "801120.INDX"),
]
MARKET_INDEX = "000300.XSHG"   # 沪深300 作为市场
BENCHMARK_INDEX = "000001.XSHG"  # 上证指数，用于与行业涨跌幅比较（差值）


def parse_meeting_dates(path_csv):
    """从 会议历届时间.csv 解析 (会议名称, T日)"""
    out = []
    with open(path_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("会议名称") or "").strip()
            time_str = (row.get("时间") or "").strip()
            if not name or " 至 " not in time_str:
                continue
            start_str = time_str.split(" 至 ")[0].strip()[:10]
            if len(start_str) == 10:
                out.append((name, start_str))
    return out


def get_trading_dates_rq(start_date, end_date):
    """调用 rqdatac 获取交易日列表。未安装或未 init 时返回 None。"""
    try:
        import rqdatac
        u, p = _get_rqdata_credentials()
        if u and p:
            rqdatac.init(username=u, password=p)
        else:
            rqdatac.init()
        dates = rqdatac.get_trading_dates(start_date=start_date, end_date=end_date)
        if dates is None:
            print("  [提示] rqdatac.get_trading_dates 返回了 None，请检查账号权限或日期范围。")
            return None
        return list(dates)
    except Exception as e:
        print(f"  [错误] 获取交易日历时异常: {e}")
        return None


def get_price_rq(order_book_ids, start_date, end_date, field="close"):
    """调用 rqdatac.get_price 获取日线。多标的单字段返回 DataFrame(index=日期, columns=order_book_id)。"""
    try:
        import rqdatac
        df = rqdatac.get_price(
            order_book_ids,
            start_date=start_date,
            end_date=end_date,
            frequency="1d",
            fields=field,
            expect_df=True,
        )
        if df is None or df.empty:
            return None
        # 若为 long format：米筐多标的时 index 为 (order_book_id, datetime)，unstack 后为 index=标的、columns=(field, 日期)
        if hasattr(df, "index") and isinstance(df.index, pd.MultiIndex):
            df = df.unstack(level=-1)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(1)  # 取日期层，columns 变为 DatetimeIndex
            df = df.T  # 转为 index=日期, columns=order_book_id
        # 若 columns 仍是 MultiIndex（多字段时），取第一层
        if isinstance(df.columns, pd.MultiIndex):
            df = df[field] if field in df.columns.get_level_values(0) else df
        # 统一 index 为 date，便于与 get_trading_dates 的 date 比较
        try:
            if isinstance(df.index, pd.MultiIndex):
                df.index = df.index.get_level_values(-1)  # 取日期层（T 后可能为 (field, date)）
            df.index = pd.DatetimeIndex(df.index).date
        except Exception:
            pass
        return df
    except Exception:
        return None


def cumret(prices, d_start, d_end):
    """区间 [d_start, d_end] 的累积收益 (end/start - 1)。"""
    if d_start not in prices or d_end not in prices or prices[d_start] == 0:
        return None
    return prices[d_end] / prices[d_start] - 1.0


def run():
    base = Path(__file__).resolve().parent.parent
    path_meetings = base / "时间" / "会议历届时间.csv"
    path_out = base / "时间" / "会议窗口行业数据.csv"

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

    # 需要拉取的日期范围：所有会议 T 的 [T-5, T+5] 并集
    from datetime import datetime, timedelta
    all_dates = set()
    for _, t_str in meetings:
        t = datetime.strptime(t_str, "%Y-%m-%d").date()
        for d in [t + timedelta(days=k) for k in range(-30, 30)]:
            all_dates.add(d)
    start_date = min(all_dates).isoformat()
    end_date = max(all_dates).isoformat()

    trading = get_trading_dates_rq(start_date, end_date)
    if not trading:
        print("获取交易日历失败。请确认：1) config_local.py 中 RQDATA_USERNAME、RQDATA_PASSWORD 正确；2) 网络可访问米筐；3) 账号有交易日历权限。")
        return
    # 米筐可能返回 datetime，统一为 date 以便与 closes.index 和会议 T 日比对
    trading = [d.date() if hasattr(d, "date") and callable(getattr(d, "date")) else d for d in trading]

    # 行业 + 市场 + 上证指数（基准）
    ids = [MARKET_INDEX, BENCHMARK_INDEX] + [code for _, code in SW30_INDICES]
    closes = get_price_rq(ids, start_date, end_date, field="close")
    turnover_df = get_price_rq(ids, start_date, end_date, field="total_turnover")
    if closes is None or closes.empty:
        print("获取行情(close)失败")
        return
    # 区间成交额：对 [d_start, d_end] 内日度 total_turnover 求和（单位：元）
    def sum_turnover(tdf, d_start, d_end):
        if tdf is None or tdf.empty:
            return None
        try:
            mask = (tdf.index >= d_start) & (tdf.index <= d_end)
            return tdf.loc[mask].sum(axis=0)
        except Exception:
            return None

    # 对每个会议计算 T-5, T-1, T, T+1, T+5 的累积收益等
    rows_out = []
    for name, t_str in meetings:
        t_date = datetime.strptime(t_str, "%Y-%m-%d").date()
        if t_date not in trading:
            # 取最近交易日为 T
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

        # 各窗口：单日窗口用「前一交易日→当日」算当日收益，否则用区间起止日算累积收益
        windows = [
            ("T-5", d_m5, d_m5, (trading[i_m5 - 1], d_m5) if i_m5 >= 1 else (d_m5, d_m5)),
            ("T-1", d_m1, d_m1, (trading[i_m1 - 1], d_m1) if i_m1 >= 1 else (d_m1, d_m1)),
            ("T", d_t, d_t, (trading[i - 1], d_t) if i >= 1 else (d_t, d_t)),
            ("T+1", d_p1, d_p1, (trading[i_p1 - 1], d_p1) if i_p1 >= 1 else (d_p1, d_p1)),
            ("T+5", d_p1, d_p5, (d_p1, d_p5)),
        ]
        for tag, d_start, d_end, (ret_start, ret_end) in windows:
            if d_start not in closes.index or d_end not in closes.index:
                continue
            if ret_start not in closes.index or ret_end not in closes.index:
                continue
            # 市场涨跌幅（沪深300）、上证指数涨跌幅（基准，用于与行业比较差值）
            if MARKET_INDEX in closes.columns:
                market_ret = cumret(closes[MARKET_INDEX].to_dict(), ret_start, ret_end)
            else:
                market_ret = None
            if BENCHMARK_INDEX in closes.columns:
                benchmark_ret = cumret(closes[BENCHMARK_INDEX].to_dict(), ret_start, ret_end)
            else:
                benchmark_ret = None
            # 30 行业涨跌幅、涨跌个数、行业成交额区间合计（成交额仍用窗口日 d_start~d_end）
            up_cnt, down_cnt = 0, 0
            industry_rets = []
            industry_turnover_sum = None
            for ind_name, oid in SW30_INDICES:
                if oid not in closes.columns:
                    continue
                r = cumret(closes[oid].to_dict(), ret_start, ret_end)
                if r is not None:
                    industry_rets.append((ind_name, round(r, 6)))
                    if r > 0:
                        up_cnt += 1
                    elif r < 0:
                        down_cnt += 1
            if turnover_df is not None:
                ser = sum_turnover(turnover_df, d_start, d_end)
                if ser is not None:
                    industry_codes = [c for _, c in SW30_INDICES]
                    industry_turnover_sum = ser.reindex(industry_codes).sum()
            # 换手率：指数日线无该字段，需成分股聚合时再补，此处留空
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
                "行业成交额_合计": round(float(industry_turnover_sum), 0) if industry_turnover_sum is not None and pd.notna(industry_turnover_sum) else "",
                "换手率": "",  # 指数无该字段，后续用成分股聚合可补
            }
            # 行业涨跌幅：存为「行业1:r1; 行业2:r2」便于后续解析
            row["行业涨跌幅"] = "; ".join(f"{n}:{r}" for n, r in industry_rets)
            rows_out.append(row)

    if not rows_out:
        print("未计算出任何会议窗口数据")
        return

    # 写出 CSV
    fieldnames = ["会议名称", "T日", "窗口", "窗口开始", "窗口结束", "行业上涨家数", "行业下跌家数", "市场涨跌幅", "上证指数涨跌幅", "行业成交额_合计", "换手率", "行业涨跌幅"]
    with open(path_out, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)
    print(f"已写入 {path_out}，共 {len(rows_out)} 行（会议×窗口）。")
    print("说明：换手率需用成分股聚合时再补；当前行业为申万一级指数，若需中信30行业请用成分股+中信分类聚合。")


if __name__ == "__main__":
    run()
