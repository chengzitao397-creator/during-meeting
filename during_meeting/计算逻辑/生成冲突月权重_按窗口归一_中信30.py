# -*- coding: utf-8 -*-
"""
在全局冲突月权重的基础上，按「同一交易窗口」在组内再归一一遍。

- 输入：output/冲突月权重_中信30.csv
  - 来自脚本：生成冲突月权重_中信30.py
  - 其中「权重」列已经是按全局 |平均涨跌幅| 之和归一后的全局权重
- 处理：
  - 按 window 分组（例如 T-5、T-1、T、T+1、T+5）
  - 对每个 window 组内单独计算 Σ|权重|
  - 组内再归一：权重_窗口内 = 原权重 / Σ|权重|
  - 同时给出百分比形式：权重_窗口内_百分比 = 权重_窗口内 × 100
- 输出：
  - 对每个 window 输出一份单独的 CSV，格式与原表完全一致，只是：
    - 仅包含该 window 的行
    - 使用窗口内归一后的权重列
  - 文件命名：output/冲突月权重_<window>_中信30.csv
    - 例如：冲突月权重_T_中信30.csv、冲突月权重_T+1_中信30.csv
"""
from pathlib import Path
import csv


def sanitize_window_for_filename(window: str) -> str:
    """
    将窗口名转成适合用于文件名的形式。
    - 这里主要是保留原样，仅替换可能影响文件系统的字符（目前简单替换空格）。
    """
    window = (window or "").strip()
    return window.replace(" ", "_")


def run():
    base = Path(__file__).resolve().parent.parent
    path_in = base / "output" / "冲突月权重_中信30.csv"

    if not path_in.exists():
        print(f"未找到全局冲突月权重表：{path_in}")
        return

    # 读取全局权重表
    with open(path_in, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames_in = reader.fieldnames or []
        rows = list(reader)

    if not rows:
        print("全局冲突月权重表为空")
        return

    # 按 window 分组
    by_window = {}
    for r in rows:
        window = (r.get("window") or "").strip()
        if not window:
            continue
        by_window.setdefault(window, []).append(r)

    if not by_window:
        print("全局冲突月权重表中未找到任何 window 分组")
        return

    for window, group_rows in by_window.items():
        # 计算该窗口内权重绝对值之和
        abs_sum = 0.0
        for r in group_rows:
            raw = r.get("权重")
            if raw in ("", None):
                continue
            try:
                abs_sum += abs(float(raw))
            except (TypeError, ValueError):
                continue

        if abs_sum <= 0:
            print(f"窗口 {window} 内所有行权重无效或为 0，跳过该窗口。")
            continue

        # 在该窗口内重新归一：权重_窗口内 / 权重_窗口内_百分比
        for r in group_rows:
            raw = r.get("权重")
            if raw in ("", None):
                r["权重_窗口内"] = ""
                r["权重_窗口内_百分比"] = ""
                continue
            try:
                w = float(raw)
                w_local = w / abs_sum
                r["权重_窗口内"] = round(w_local, 8)
                # 百分比形式，方便观感（例如 1.23 表示 1.23%）
                r["权重_窗口内_百分比"] = round(w_local * 100, 4)
            except (TypeError, ValueError):
                r["权重_窗口内"] = ""
                r["权重_窗口内_百分比"] = ""

        # 输出该窗口单独的 CSV，与原表列格式一致 + 窗口内权重两列
        out_fieldnames = list(fieldnames_in)
        # 确保新增列放在末尾且不重复
        for extra in ["权重_窗口内", "权重_窗口内_百分比"]:
            if extra not in out_fieldnames:
                out_fieldnames.append(extra)

        window_tag = sanitize_window_for_filename(window)
        path_out = base / "output" / f"冲突月权重_{window_tag}_中信30.csv"
        path_out.parent.mkdir(parents=True, exist_ok=True)

        with open(path_out, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=out_fieldnames)
            writer.writeheader()
            writer.writerows(group_rows)

        print(
            f"已写入 {path_out}，共 {len(group_rows)} 行；"
            f"该文件内各行使用窗口内归一后的权重（绝对值和=1，正=做多、负=做空）。"
        )


if __name__ == "__main__":
    run()

