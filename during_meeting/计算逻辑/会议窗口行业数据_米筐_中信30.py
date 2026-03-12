# -*- coding: utf-8 -*-
"""
会议窗口 A 股行业数据（米筐 rqdatac）— 中信 30 个一级行业，只统计涨/跌不统计幅度。

- 以会议召开第一天为 T 日，取 T-5、T-1、T、T+1、T+5 五个时间轴；
- 每个窗口内用区间累积收益判断各行业涨（>0）/跌（<0）/平（=0）；
- 输出：行业上涨家数、行业下跌家数、行业涨跌明细（仅方向：涨/跌/平），不输出涨跌幅数值。

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


# 中信一级行业（米筐 29 个指数，常称“中信 30”口径）
# 来源：米筐指数列表 CI005001.INDX ~ CI005029.INDX
CITIC30_INDICES = [
    ("石油石化", "CI005001.INDX"),
    ("煤炭", "CI005002.INDX"),
    ("有色金属", "CI005003.INDX"),
    ("电力及公用事业", "CI005004.INDX"),
    ("钢铁", "CI005005.INDX"),
    ("基础化工", "CI005006.INDX"),
    ("建筑", "CI005007.INDX"),
    ("建材", "CI005008.INDX"),
    ("轻工制造", "CI005009.INDX"),
    ("机械", "CI005010.INDX"),
    ("电力设备及新能源", "CI005011.INDX"),
    ("国防军工", "CI005012.INDX"),
    ("汽车", "CI005013.INDX"),
    ("商贸零售", "CI005014.INDX"),
    ("消费者服务", "CI005015.INDX"),
    ("家电", "CI005016.INDX"),
    ("纺织服装", "CI005017.INDX"),
    ("医药", "CI005018.INDX"),
    ("食品饮料", "CI005019.INDX"),
    ("农林牧渔", "CI005020.INDX"),
    ("银行", "CI005021.INDX"),
    ("非银行金融", "CI005022.INDX"),
    ("房地产", "CI005023.INDX"),
    ("交通运输", "CI005024.INDX"),
    ("电子", "CI005025.INDX"),
    ("通信", "CI005026.INDX"),
    ("计算机", "CI005027.INDX"),
    ("传媒", "CI005028.INDX"),
    ("综合", "CI005029.INDX"),
]
MARKET_INDEX = "000300.XSHG"
BENCHMARK_INDEX = "000001.XSHG"


def _meeting_dates_path(base):
    """会议历届时间 CSV：优先 时间/会议历届时间.csv，否则用根目录 会议历届时间_副本.csv。"""
    p = base / "时间" / "会议历届时间.csv"
    if p.exists():
        return p
    p = base / "会议历届时间_副本.csv"
    return p if p.exists() else base / "时间" / "会议历届时间.csv"


def parse_meeting_dates(path_csv):
    """从 会议历届时间.csv 解析 (会议名称, T日)。"""
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
    """调用 rqdatac 获取交易日列表。"""
    try:
        import rqdatac
        u, p = _get_rqdata_credentials()
        if u and p:
            rqdatac.init(username=u, password=p)
        else:
            rqdatac.init()
        dates = rqdatac.get_trading_dates(start_date=start_date, end_date=end_date)
        if dates is None:
            return None
        return list(dates)
    except Exception:
        return None


def get_price_rq(order_book_ids, start_date, end_date, field="close"):
    """调用 rqdatac.get_price 获取日线。"""
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
        if hasattr(df, "index") and isinstance(df.index, pd.MultiIndex):
            df = df.unstack(level=-1)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(1)
            df = df.T
        if isinstance(df.columns, pd.MultiIndex):
            df = df[field] if field in df.columns.get_level_values(0) else df
        try:
            if isinstance(df.index, pd.MultiIndex):
                df.index = df.index.get_level_values(-1)
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
    path_meetings = _meeting_dates_path(base)
    if not path_meetings.exists():
        print(f"未找到会议历届时间文件，请放置：时间/会议历届时间.csv 或 会议历届时间_副本.csv")
        return

    # 输出到 output 目录，便于统一管理
    path_out = base / "output" / "会议窗口行业数据_中信30.csv"
    path_out.parent.mkdir(parents=True, exist_ok=True)

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
        for d in [t + timedelta(days=k) for k in range(-30, 30)]:
            all_dates.add(d)
    start_date = min(all_dates).isoformat()
    end_date = max(all_dates).isoformat()

    trading = get_trading_dates_rq(start_date, end_date)
    if not trading:
        print("获取交易日历失败，请检查米筐账号与网络。")
        return
    trading = [d.date() if hasattr(d, "date") and callable(getattr(d, "date")) else d for d in trading]

    ids = [MARKET_INDEX, BENCHMARK_INDEX] + [code for _, code in CITIC30_INDICES]
    closes = get_price_rq(ids, start_date, end_date, field="close")
    if closes is None or closes.empty:
        print("获取行情(close)失败")
        return

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
            if d_start not in closes.index or d_end not in closes.index:
                continue
            if ret_start not in closes.index or ret_end not in closes.index:
                continue

            up_cnt, down_cnt = 0, 0
            detail_parts = []
            # 行业涨跌幅明细：用于下游计算历届平均涨跌幅（格式：行业:数值）
            detail_ret_parts = []
            for ind_name, oid in CITIC30_INDICES:
                if oid not in closes.columns:
                    continue
                r = cumret(closes[oid].to_dict(), ret_start, ret_end)
                if r is None:
                    continue
                if r > 0:
                    up_cnt += 1
                    detail_parts.append(f"{ind_name}:涨")
                elif r < 0:
                    down_cnt += 1
                    detail_parts.append(f"{ind_name}:跌")
                else:
                    detail_parts.append(f"{ind_name}:平")
                # 保留 6 位小数，便于下游聚合
                detail_ret_parts.append(f"{ind_name}:{round(r, 6)}")

            row = {
                "会议名称": name,
                "T日": t_str,
                "窗口": tag,
                "窗口开始": str(d_start),
                "窗口结束": str(d_end),
                "行业上涨家数": up_cnt,
                "行业下跌家数": down_cnt,
                "行业涨跌明细": "; ".join(detail_parts),
                "行业涨跌幅明细": "; ".join(detail_ret_parts),
            }
            rows_out.append(row)

    if not rows_out:
        print("未计算出任何会议窗口数据")
        return

    fieldnames = ["会议名称", "T日", "窗口", "窗口开始", "窗口结束", "行业上涨家数", "行业下跌家数", "行业涨跌明细", "行业涨跌幅明细"]
    with open(path_out, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)
    print(f"已写入 {path_out}，共 {len(rows_out)} 行（会议×窗口）。")
    print("说明：行业为中信一级（29 个指数），行业涨跌明细为方向，行业涨跌幅明细为数值供历届平均涨跌幅计算。")


if __name__ == "__main__":
    run()
