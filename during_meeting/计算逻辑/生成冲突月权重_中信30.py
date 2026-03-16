# -*- coding: utf-8 -*-
"""
基于「受影响显著一级行业_历届平均涨跌幅」表，按平均涨跌幅保留正负、绝对值之和归一化得到权重。

- 输入：output/受影响显著一级行业_历届平均涨跌幅_中信30.csv
- 逻辑：权重 = 平均涨跌幅 / sum(|平均涨跌幅|)，故所有权重的绝对值之和 = 1（正=做多、负=做空）
- 输出：output/冲突月权重_中信30.csv（原表所有列 + 权重）
"""
from pathlib import Path
import csv


def run():
    base = Path(__file__).resolve().parent.parent
    path_in = base / "output" / "受影响显著一级行业_历届平均涨跌幅_中信30.csv"
    path_out = base / "output" / "冲突月权重_中信30.csv"

    if not path_in.exists():
        print(f"未找到历届平均涨跌幅表：{path_in}")
        return

    with open(path_in, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames_in = reader.fieldnames or []
        rows = list(reader)

    if not rows:
        print("历届平均涨跌幅表为空")
        return

    # 用「平均涨跌幅」做原始值，求绝对值之和作为归一化分母
    abs_sum = 0.0
    for r in rows:
        raw = r.get("平均涨跌幅")
        if raw == "":
            continue
        try:
            abs_sum += abs(float(raw))
        except (TypeError, ValueError):
            continue

    if abs_sum <= 0:
        print("所有行的平均涨跌幅均为 0 或无效，无法归一化")
        return

    # 每行权重 = 平均涨跌幅 / 分母；保留正负
    for r in rows:
        raw = r.get("平均涨跌幅")
        if raw == "":
            r["权重"] = ""
            r["权重_百分比"] = ""
            continue
        try:
            val = float(raw)
            w = val / abs_sum
            r["权重"] = round(w, 8)
            # 方便观感阅读：再给出百分比形式（例如 1.23 表示 1.23%）
            r["权重_百分比"] = round(w * 100, 4)
        except (TypeError, ValueError):
            r["权重"] = ""
            r["权重_百分比"] = ""

    out_fieldnames = list(fieldnames_in) + ["权重", "权重_百分比"]
    path_out.parent.mkdir(parents=True, exist_ok=True)
    with open(path_out, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"已写入 {path_out}，共 {len(rows)} 行；权重按 |平均涨跌幅| 之和归一化，正=做多、负=做空。")


if __name__ == "__main__":
    run()
