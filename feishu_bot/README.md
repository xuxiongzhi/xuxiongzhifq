# 亚马逊周报 · 飞书插件

店铺 A 后端里的 `/周报` 插件：领星周数据 + SP-API → 父体周宽表、AI 数据包 → 飞书卡片与附件。
两个卖家账号(tangliuquan、xupeng)合并出一份汇总周报，推送到店铺 A 的群。

## 目录

```
plugins/
├─ 周报_plugin.py                 # 飞书外壳：指令、进度、卡片、推送(本文件放进店铺A的 plugins/)
└─ weekly_report/                  # 数据引擎(整个目录放进店铺A的 plugins/)
   ├─ weekly_pipeline_spapi.py     # 周报脚本：ingest(合并) / pack(AI数据包) / window
   ├─ recognize.py                 # 按表头识别领星导出，统一改名，检查齐全
   ├─ config.json                  # 参数；SP-API 凭证指向 ../../ziliao/
   └─ data/                        # 运行时生成(不入库)：inbox/ raw/<周>/ spapi_cache/ weekly.db logs/
tools/local_test.py                # 本地模拟飞书调用(不需要网关)
docs/gateway_send_file.md          # 网关需新增的发文件接口
```

## 部署(店铺 A)

1. 把 `plugins/周报_plugin.py` 和 `plugins/weekly_report/` 复制到店铺 A 的 `plugins/`。
2. SP-API 凭证放 `ziliao/亚马逊sp-api.txt`、`ziliao/亚马逊sp-api-xupeng.txt`(路径可在 config.json 的 `spapi_accounts` 改)。
3. 依赖：`pip install pandas openpyxl requests python-amazon-sp-api`。
4. 发附件需要网关新增 `/gw_proxy/send_file`(见 docs)。没加之前，除「发送」外其它指令都能用。
5. 重启店铺 A 后端，群里发 `/help` 应能看到「📊 数据报表 → 亚马逊周报」。

## 每周流程(第一期：手动放文件)

1. `/周报 窗口` 查看领星各报表要下载的日期(周日~周六)。
2. 把领星导出(文件名随意)放进 `plugins/weekly_report/data/inbox/`，发 `/周报 导入`。
3. `/周报 检查` 确认必需文件齐全、订单覆盖整周。
4. `/周报 生成`(会拉 SP-API)或 `/周报 测试`(只用已下载的 SP-API 缓存)。
5. `/周报 发送` 获取 xlsx / csv / AI 数据包附件。

## 本地测试

```bash
cp <领星导出与 SP-API .bin> plugins/weekly_report/data/inbox/
python tools/local_test.py "/周报 导入 2026-09-26" "/周报 测试 2026-09-26" "/周报 发送 2026-09-26"
```

## 后续版本

- 第二期：AI 周报(ai_runner，需支持 max_tokens 参数)与存档，卡片展示执行清单。
- 第三期：每周五定时跑上一周(周日~周六)，接真实 SP-API。
- 第四期：月报(`--month`，按周结束日归属月份，与上月按日均对比；环比需库里至少 2 个月)。
- 第五期：领星自动下载到日期文件夹。
