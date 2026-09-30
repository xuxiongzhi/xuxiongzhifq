# 亚马逊周报 · 飞书插件

店铺 A 后端里的 `/周报` 插件。两个卖家账号(tangliuquan、xupeng)合并出一份汇总周报：
领星周数据 + SP-API → 周宽表与 AI 数据包 → `ai_runner`(默认模型)写周报 → **群里只发 AI 周报文件，最后发一条概述**。

## 目录

```
plugins/
├─ 周报_plugin.py              # 指令、AI 调用、发文件、定时(放进店铺A的 plugins/)
└─ weekly_report/               # 数据引擎(整个目录放进店铺A的 plugins/)
   ├─ weekly_pipeline_spapi.py  # ingest(合并) / pack(AI数据包) / window
   ├─ recognize.py              # 按表头识别领星导出、统一改名、检查齐全
   ├─ config.json               # 参数；SP-API 凭证指向 ../../ziliao/
   ├─ test/inputs/<周结束日>/    # ★ 测试数据：领星导出 + SP-API .bin(文件名随意)
   ├─ test/ raw/ spapi_cache/ weekly.db out/   # 测试工作区(自动生成)
   ├─ data/inbox/ raw/ spapi_cache/ weekly.db  # 正式工作区(自动生成)
   ├─ out/                      # 正式输出：周报_<周>.md、周宽表、数据包
   └─ logs/                     # 子进程日志
tools/local_test.py             # 本地模拟(伪造 config/feishu_gateway/ai_runner)
```

测试与正式两个工作区完全隔离(各自的数据库、SP-API 缓存、输出)。测试只用已下载的 SP-API 缓存，不请求亚马逊；
每次测试都从 `test/inputs/` 复制，原文件不动，可反复跑。

## 现阶段：测试

1. 把领星导出和 SP-API 下载的 `.bin` 放进 `plugins/weekly_report/test/inputs/2026-09-26/`。
2. 群里发 `/周报 测试`(不带日期=测试文件夹里最新的一周)。
3. 收到：进度提示 → `周报_2026-09-26.md` 文件 → 概述(店铺汇总、断货风险、数据质量、执行清单前3条)。

## 正式上线(领星自动化接入后)

1. 领星导出放 `data/inbox/`(以后由自动下载写入)，`/周报 导入` → `/周报 生成`。
2. 在目标群发 `/周报 绑定`，再发 `/周报 定时 开启`：每周五 13:00(北京时间)自动跑上一周(周日~周六)，
   从 inbox 导入 → 拉 SP-API → AI 周报 → 发文件 + 概述；文件不全会在群里提示缺什么。
3. 凭证：`ziliao/亚马逊sp-api.txt`、`ziliao/亚马逊sp-api-xupeng.txt`。依赖：`pandas openpyxl requests python-amazon-sp-api`。

## 其它指令

`/周报` 状态 · `/周报 窗口` 领星下载日期 · `/周报 检查` 文件是否齐全 · `/周报 AI` 只重跑 AI 周报 · `/周报 发送` 重发周报文件

## 本地测试

```bash
python tools/local_test.py "/周报 测试" "/周报 发送 2026-09-26" "/周报"
```

## 后续

- 月报：`--month`，按周结束日归属月份，与上月按日均对比(需库里至少 2 个月)。
- 领星自动下载到日期文件夹，接入后开启定时。
