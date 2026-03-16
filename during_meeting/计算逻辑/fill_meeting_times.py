# -*- coding: utf-8 -*-
"""
为会议大全中「第22行及以上」且时间为空的会议，填入查到的历届时间。
数据来源：公开资料（三中全会、两会、进博会、广交会等）。
"""
import csv
from pathlib import Path

def main():
    # 项目根目录；会议大全在「会议名称」文件夹
    base = Path(__file__).resolve().parent.parent
    dir_name = base / "会议名称"

    # 历届时间数据（公开资料整理，分号分隔多届）
    LIJIE_TIMES = {
        "三中全会": "十一届1978-12-18至22；十二届1984-10-20；十三届1988-09-26至30；十四届1993-11-11至14；十五届1998-10-12至14；十六届2003-10-11至14；十七届2008-10-09至12；十八届2013-11-09至12；十九届2018-02-26至28；二十届2024-07-15至18",
        "两会": "通常每年3月召开（人大一般3月5日开幕，政协3月3日或4日开幕）；近年：2020年两会因疫情5月22日人大开幕；2024年3月4日政协开幕、3月5日人大开幕",
        "中国国际进口博览会(CIIE) (开幕)": "第一届2018-11-05至10；第二届2019-11-05至10；第三届2020-11-05至10；第四届2021-11-05至10；第五届2022-11-05至10；第六届2023-11-05至10；第七届2024-11-05至10",
        "中央经济工作会议": "每年12月前后；本表已含2012-2025年各届具体日期，见第20-33行",
        "中央金融工作会议 (开幕日)": "本表已含历届：1997、2002、2007、2012、2017、2023年，见第34-39行",
        "大会": "泛指，无固定历届时间",
        "峰会": "泛指，无固定历届时间",
        "广交会(春季第一期开幕)": "每年4月中旬（通常4月15日左右）；春交会自1957年起每年一届，近年如第135届2024-04-15开幕",
        "广交会(秋季第一期开幕)": "每年10月中旬（通常10月15日左右）；秋交会自1957年起每年一届，近年如第136届2024-10-15开幕",
        "研讨会": "泛指，无固定历届时间",
        "论坛": "泛指，无固定历届时间",
    }

    with open(dir_name / "会议大全.csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        return
    header = rows[0]
    data = rows[1:]
    time_idx = header.index("时间") if "时间" in header else -1
    name_idx = header.index("event_name_cn") if "event_name_cn" in header else 0
    if time_idx < 0:
        print("未找到「时间」列")
        return

    filled = 0
    for i, row in enumerate(data):
        if len(row) <= time_idx:
            continue
        name = row[name_idx].strip() if name_idx < len(row) else ""
        time_val = row[time_idx].strip() if row[time_idx] else ""
        if time_val or name not in LIJIE_TIMES:
            continue
        row[time_idx] = LIJIE_TIMES[name]
        filled += 1

    with open(dir_name / "会议大全.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(data)
    with open(dir_name / "会议大全", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(data)

    # 更新 会议大全.md
    cn_headers = ["中文名称", "英文名称", "频次", "常见月份", "首次年", "届数估计", "政策领域", "A股传导", "备注", "时间"]
    col_count = len(header)
    md_lines = [
        "# 会议大全",
        "",
        "含会议大全原有条目、历届会议统计及第22行以上会议的历届时间（已填入）。",
        "",
        "| " + " | ".join(cn_headers[:col_count]) + " |",
        "| " + " | ".join(["---"] * col_count) + " |",
    ]
    for row in data:
        cells = [str(c).replace("|", "\\|").strip()[:80] for c in row]
        if len(cells) < col_count:
            cells += [""] * (col_count - len(cells))
        md_lines.append("| " + " | ".join(cells[:col_count]) + " |")
    with open(dir_name / "会议大全.md", "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))

    print(f"已为 {filled} 条会议填入历届时间。")
    print("已更新：会议大全、会议大全.csv、会议大全.md")

if __name__ == "__main__":
    main()
