# 月度会议查询网页

选年份+月份，查看该月有哪些会议、影响显著的行业、历届涨跌表现与幅度。

## 本机运行（仅自己可访问）

在**项目根目录**执行：

```bash
streamlit run web/app_月度会议查询.py
```

或在 `web` 目录下执行：

```bash
cd web
streamlit run app_月度会议查询.py
```

浏览器打开终端里提示的地址（一般为 **http://localhost:8501**），即为网页入口。

**说明**：此时只有你这台电脑能访问该地址。把 `http://localhost:8501` 发给别人，别人无法打开。

## 让别人通过链接访问

需要把应用**部署到公网**，例如：

- [Streamlit Community Cloud](https://share.streamlit.io/)：把项目推到 GitHub 后，用该服务部署，会得到一个可分享的 URL。
- 自己的服务器：在服务器上安装 Python + streamlit，运行上述命令并配置域名/反向代理，即可用域名访问。

部署完成后，把得到的**公网 URL（或域名）**发给别人，对方用浏览器打开即可使用。

## 依赖

本目录或项目根目录执行：

```bash
pip install -r requirements.txt
```

`requirements.txt` 需包含：`streamlit`、`pandas`。若根目录已有，可直接用根目录的。
