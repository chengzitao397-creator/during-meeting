# -*- coding: utf-8 -*-
"""
窗口累计收益（复利口径）：
  cumulative_return = (1+r1)*(1+r2)*...*(1+rn) - 1
- 支持 list / numpy array / pandas Series
- NaN 在连乘时忽略，并返回有效天数 n_valid；n_valid=0 时返回 NaN
- 不做行业打分/回归/排序等任何额外内容
"""
import numpy as np
import pandas as pd


def cumret(r):
    """
    单序列的窗口累计收益（复利）与有效天数。

    参数
    -----
    r : list, numpy array 或 pandas Series
        日收益序列，如 +2% 写 0.02，-1% 写 -0.01

    返回
    -----
    cumulative_return : float
        累计收益；(1+r1)*...*(1+rn) - 1；n_valid=0 时为 np.nan
    n_valid : int
        参与连乘的有效天数（非 NaN 的个数）
    """
    r = np.asarray(r, dtype=float)
    # 去掉 NaN，只对有效日做连乘
    valid = r[~np.isnan(r)]
    n_valid = len(valid)
    if n_valid == 0:
        return np.nan, 0
    # 复利：(1+r1)*(1+r2)*...*(1+rn) - 1
    prod = np.prod(1.0 + valid)
    cumulative_return = float(prod - 1.0)
    return cumulative_return, n_valid


def cumret_df(df):
    """
    对 DataFrame 的每一列计算窗口累计收益与有效天数。

    参数
    -----
    df : pandas DataFrame
        每列为一支标的或一条日收益序列

    返回
    -----
    cumulative_return : Series
        index 为列名，值为该列的累计收益
    n_valid : Series
        index 为列名，值为该列的有效天数
    """
    if df is None or df.empty:
        return pd.Series(dtype=float), pd.Series(dtype=int)
    cr = []
    nv = []
    for col in df.columns:
        c, n = cumret(df[col])
        cr.append(c)
        nv.append(n)
    return pd.Series(cr, index=df.columns), pd.Series(nv, index=df.columns)


# ----- 最小例子验证 -----
if __name__ == "__main__":
    # r = [0.10, -0.10] -> (1.1 * 0.9) - 1 = -0.01
    r = [0.10, -0.10]
    cr, n = cumret(r)
    assert abs(cr - (-0.01)) < 1e-10, f"expected -0.01, got {cr}"
    assert n == 2, f"expected n_valid=2, got {n}"
    print("cumret([0.10, -0.10]) ->", f"cumulative_return={cr}, n_valid={n}")

    # 带 NaN：跳过 NaN
    r_nan = [0.10, np.nan, -0.10]
    cr2, n2 = cumret(r_nan)
    assert abs(cr2 - (-0.01)) < 1e-10
    assert n2 == 2
    print("cumret([0.10, nan, -0.10]) ->", f"cumulative_return={cr2}, n_valid={n2}")

    # 全 NaN
    cr3, n3 = cumret([np.nan, np.nan])
    assert np.isnan(cr3) and n3 == 0
    print("cumret([nan, nan]) ->", f"cumulative_return={cr3}, n_valid={n3}")

    # DataFrame
    df = pd.DataFrame({"A": [0.10, -0.10], "B": [0.02, 0.03]})
    cr_s, n_s = cumret_df(df)
    print("cumret_df:", cr_s.to_dict(), n_s.to_dict())
    print("ok")
