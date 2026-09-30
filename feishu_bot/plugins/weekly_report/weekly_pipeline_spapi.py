#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每周运营数据管线：领星导出(+预留SP-API) -> 父体级周表(入库) -> 多周AI汇总包

  python weekly_pipeline.py init                                     # 生成 config.json 与 SP-API 模板
  python weekly_pipeline.py window                                   # 先看本次各数据源的时间窗口，按同样日期去领星下载
  python weekly_pipeline.py ingest --inputs inputs/2026-09-30 [--week-end 2026-09-30] [--refresh-spapi] [--no-spapi] [--sp-wait 秒]
  python weekly_pipeline.py pack --weeks 4 [--end 2026-09-30]        # 产出给AI的汇总包(md)

设计要点
  * 一周一次 ingest：清洗 -> 拼成 "店铺 x 父体" 一行的周宽表 + 广告明细表 -> 写入 SQLite(同一周重跑会覆盖)
  * pack 读取最近 N 周(2或4)，生成 "趋势 + 窗口内累计明细 + 数据质量说明 + 提示词" 的单个 md 文件，整体贴给 AI
  * 所有金额统一折算为 USD；比率一律用"汇总后再相除"，不对比率求平均
  * SP-API 数据先留空列；把 spapi_parent_weekly*.csv 放进当周 inputs 目录，下次 ingest 自动并入
"""
import argparse, glob, gzip, hashlib, json, os, re, shutil, sqlite3, sys, warnings, time, io
from pathlib import Path
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd

# SP-API：参考「送仓时间自动更新_plugin.py」的凭证解析方式，
# 但凭证文件改为 weekly_pipeline.py 同目录下自动发现的 txt。
try:
    from sp_api.api import Reports
    from sp_api.base import Marketplaces
    SPAPI_IMPORT_OK = True
except Exception:
    Reports = None
    Marketplaces = None
    SPAPI_IMPORT_OK = False

warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.abspath(__file__))

# --------------------------------------------------------------------------- 进度日志(控制台+文件，实时刷新)
_T0 = time.time()
_LOGF = None

def start_clock():
    global _T0
    _T0 = time.time()

def init_log(path):
    global _LOGF
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        _LOGF = open(path, "a", encoding="utf-8")
    except Exception:
        _LOGF = None

def _fmt_sec(sec):
    sec = int(sec)
    return f"{sec // 60}分{sec % 60:02d}秒"

def log(msg):
    line = f"[{datetime.now():%H:%M:%S} 已用{_fmt_sec(time.time() - _T0)}] {msg}"
    try:
        print(line, flush=True)
    except Exception:
        pass
    if _LOGF:
        try:
            _LOGF.write(line + "\n"); _LOGF.flush()
        except Exception:
            pass

DEFAULT_CONFIG = {
    "db_path": "data/weekly.db",
    "out_dir": "out",
    "exclude_countries": ["CA", "MX", "BR"],   # 数据量太小不足以分析，整站点剔除(Listing/广告/SP-API都不处理)；同店铺的其它站点(如 US、UK)保留
    "cny_per_usd": 7.10,                       # 占位汇率，请按实际更新
    "fx_to_usd": {"USD": 1.0, "CAD": 0.72, "MXN": 0.055, "GBP": 1.30, "BRL": 0.18,
                  "EUR": 1.10, "JPY": 0.0067, "AUD": 0.65},   # 占位汇率，请按实际更新
    "freight_usd_per_unit_default": None,      # 头程单件成本(USD)，为空则盈亏ACoS只给"未含头程"上限
    "freight_placeholder_max": 0.05,           # 头程<=此值视为未维护(如0.01)
    "freight_by_style_csv": "",                # 可选：CSV，两列 款,头程_USD
    "attribution_lag_days": 3,                 # 广告归因回补天数(最近几天的订单可能还没补齐)
    "new_product_days": 60,
    "stock_low_days": 21,
    "stock_high_days": 120,
    "sample_orders_low": 5,
    "sample_orders_high": 15,
    "min_clicks_waste": 8,                     # 窗口内累计点击>=此值且零订单 -> 列入数据包观察名单(否定候选由 actions.py 规则生成)
    "top_n": {"waste": 12, "winners": 12, "keywords": 10, "detail_parents": 15, "sqp": 10},
    "prompt_file": "",                         # 可选：自定义提示词文件路径
    # ---- SP-API 自动拉取 ----
    "spapi_enabled": True,
    "spapi_credential_file": "",              # 单账号：凭证txt路径；留空则自动寻找本脚本同目录 *.txt
    "spapi_accounts": {},                     # 多账号(同一站点有多个卖家账号时必填)：{"tangliuquan": "亚马逊sp-api.txt", "xupeng": "sp-api-xupeng.txt"}
                                              # 键 = 店铺名去掉站点后缀(tangliuquan-US -> tangliuquan)
    "spapi_single_account": "",               # 只有一个凭证、但Listing里有多个账号时：填该凭证所属账号名
    "spapi_poll_seconds": 15,
    "spapi_report_timeout_seconds": 600,      # 单份报告最长等待；超时只放弃等待，报告仍在亚马逊生成，下次运行自动续用
    "spapi_heartbeat_seconds": 30,            # 等待期间每隔多少秒打印一次进度
    "spapi_total_timeout_seconds": 2400,      # SP-API 阶段总时限(含所有店铺)；到点后剩余报告留到下次运行
    "spapi_sqp_countries": ["US"],            # SQP 只拉这些站点(SQP最慢，且 createReport 限额约每分钟1次)
    "spapi_throttle_sleep_seconds": 65,       # createReport 限流(约每分钟1次)时的等待
    "spapi_api_retries": 5,
    "spapi_cache_dir": "data/spapi_cache",
    "spapi_refresh": False,                   # True 或命令行 --refresh-spapi：忽略缓存重新拉取
    "spapi_fetch_sales_traffic": True,        # 会话/页面浏览/转化率/Buy Box/订购件数 (需 Brand Analytics 角色)
    "spapi_sales_history_weeks": 1,           # 1=只拉本周(默认；历史销售由领星订单导出补齐，不必回填)。>1 会多请求前几周的 销售与流量报告(只为补过去几周的会话/转化率趋势)，容易触发限流
    "spapi_fetch_returns": True,              # 退货件数与原因 (需 Amazon Fulfillment 角色)
    "spapi_fetch_planning": True,             # 库龄/预估仓储费/精选报价与竞品最低价 (GET_FBA_INVENTORY_PLANNING_DATA，拉取时的快照)
    "spapi_fetch_search_catalog": False,      # 品牌分析-搜索漏斗(Search Catalog Performance)：默认关闭(父体流量用 销售与流量报告的会话 对比广告来评估)；需要搜索曝光/点击时再设 true
    "spapi_search_catalog_countries": ["US", "UK"],
    "spapi_fetch_sqp": False,                 # 逐ASIN的SQP(搜索词级)：默认关闭。API 只支持按ASIN请求，且亚马逊对该报告有全局每日请求上限(约20份/天)
    "spapi_sqp_max_asins": 54,               # SQP 每块<=200字符约18个ASIN；54个=3~4块请求(每账号)
    "spapi_sqp_asins_per_parent": 3,          # 每个父体取销量最高的前N个子体ASIN(实测：前1个只覆盖父体曝光约20%，前3个约50%，前5个约67%，所以只能看趋势/相对，不能当绝对份额)
    "sales_source": "auto",                   # auto：有订单导出且覆盖窗口时，销量7/销售额7改用订单口径(与周窗口、SP对齐)；listing：始终用Listing滚动值
    "week_end_default": "last_saturday",      # 未指定 --week-end 时的周结束日：last_saturday=最近一个数据已发布的周六；listing_date=Listing文件名里的日期
    "spapi_sqp_lag_days": 2,                  # SQP 周六结束后至少隔这么多天才请求(亚马逊约需1-2天发布)，太早则跳过
    "spapi_window_lag_days": 0,               # 流量/退货窗口的结束日往前推N天(亚马逊最近1-2天流量常未出数；想要完整7天可填2，但会与领星窗口错开)
    "receiving_avail_days": 10,               # 货到亚马逊仓(送达/开始入库)后到可售的天数；海运 发货→可售 ≈ 备货+历史中位运输(发货→开始入库)+此值 ≈ 40天
    "replen_cycle_days": 10,                  # 发货周期：每隔多少天发一次海运，建议海运发货件数要覆盖到下一批到仓
    "safety_stock_days": 10,                  # 安全库存天数：只用于建议发货/补货件数，不影响断货判定(卖空才算断货)
    "demand_weights": {"7": 0.4, "14": 0.3, "30": 0.2, "60": 0.1},   # 预测日均=各窗口日均加权(补货建议里截至快照日的7/14/30/60天销量)；上架天数不足的窗口不用，权重按剩余窗口重新归一
    "excess_fba_days": 90,                    # 冗余：FBA可售+在途 超过 日均_预测×此天数 的部分(亚马逊90天以上开始算库龄/冗余)
    "excess_total_days": 180,                 # 冗余：FBA+在途+待交付+本地仓 超过 日均_预测×此天数 的部分；待交付里超出的部分可与工厂协商减少/延后
    "slow_min_age_days": 45,                  # SKU开售(首单/创建)不足此天数不判滞销/冗余，列为'新SKU观察'
    "signed_pending_days": 7,                 # 货件签收明细里最近N天净签收的件数视为"已签收待上架"(亚马逊已签收、还没进可售，Listing可售/在途都不含)，按 签收日+上架天数 计入供给；0=不计
    "local_ship_prep_days": 2,                # 本地仓库存从决定发货到发出的备货天数(装箱/贴标/交货代)，用于断货模拟里的本地仓空运/海运到仓日
    "spapi_skip_countries": [],               # 不请求SP-API的站点，例如授权只覆盖北美时填 ["UK"]，省掉每周的 Unauthorized 警告
    "spapi_allow_duplicate_store_aliases": False,
    "action_rules": {}                        # 候选动作阈值，覆盖 actions.py 的 DEFAULT_RULES(如 {"min_clicks_judge": 30, "bid_up_step": 0.15})；空=用默认
}

PATTERNS = {"listing": "Listing", "campaign": "广告活动报告", "group": "广告组报告",
            "product": "推广商品报告", "placement": "广告位报告", "keyword": "关键词报告",
            "auto": "自动投放报告", "search": "用户搜索报告"}
COST_PATTERN = "导出产品"

CN_COUNTRY = {"US": "美国", "UK": "英国", "GB": "英国", "CA": "加拿大", "MX": "墨西哥", "BR": "巴西", "DE": "德国",
              "FR": "法国", "IT": "意大利", "ES": "西班牙", "JP": "日本", "AU": "澳洲"}
CUR_BY_CODE = {"US": "USD", "CA": "CAD", "MX": "MXN", "UK": "GBP", "GB": "GBP", "BR": "BRL",
               "DE": "EUR", "FR": "EUR", "IT": "EUR", "ES": "EUR", "JP": "JPY", "AU": "AUD"}

AD_METRICS = {"曝光量": "曝光", "点击": "点击", "花费-本币": "花费", "广告销售额-本币": "广告销售",
              "直接销售额-本币": "直接销售", "间接销售额-本币": "间接销售", "广告订单": "广告订单", "广告销量": "广告销量"}
AD_SUM = ["曝光", "点击", "花费", "广告销售", "广告订单", "广告销量"]
AD_MONEY = ["花费", "广告销售", "直接销售", "间接销售", "预算", "竞价", "默认竞价"]

SP_DERIVED_DOC = {
    "销量_SP": "SP订购件数按覆盖天数折算成7天(与广告窗口对齐的销量，订购口径，和广告订单同一口径)",
    "销售额_SP": "SP订购销售额折算成7天",
    "TACoS_SP": "广告花费 ÷ 销售额_SP(与广告窗口对齐，比用领星滚动7日更可靠)",
    "广告销售占比_SP": "广告销售 ÷ 销售额_SP",
    "库存天数_SP": "FBA可售 ÷ (销量_SP/7)，不含在途；含在途见 含在途天数_SP",
    "含在途天数_SP": "(FBA可售+在途) ÷ (销量_SP/7)"}

SP_COLS = ["SP_流量覆盖天数", "SP_流量截止日", "SP_会话", "SP_页面浏览", "SP_订购件数", "SP_订购销售额", "SP_单位会话率", "SP_BuyBox占比",
           "SP_搜索周期", "SP_搜索曝光", "SP_搜索点击", "SP_搜索加购", "SP_搜索购买", "SP_搜索销售额", "SP_搜索点击率", "SP_搜索转化率", "SP_搜索覆盖ASIN数",
           "SP_SQP周期", "SP_SQP曝光", "SP_SQP点击", "SP_SQP加购", "SP_SQP购买", "SP_SQP查询数", "SP_SQP覆盖ASIN数",
           "SP_退货件数", "SP_退货率", "SP_退货原因Top3",
           "SP_库龄90天以上件数", "SP_库龄181天以上件数", "SP_预估仓储费", "SP_预估长期仓储费",
           "SP_我方价格", "SP_我方实际售价", "SP_促销中SKU数", "SP_最低促销价", "SP_精选报价价格", "SP_精选报价为我方", "SP_竞品最低价", "SP_价格高于竞品最低价比例"]
SP_DOC = {
    "SP_会话": "销售与流量报告(PARENT粒度)：sessions", "SP_页面浏览": "销售与流量报告：pageViews",
    "SP_订购件数": "销售与流量报告：unitsOrdered，可与领星'销量7'对账", "SP_订购销售额": "销售与流量报告：orderedProductSales(已折USD)",
    "SP_单位会话率": "销售与流量报告：unitSessionPercentage(亚马逊为0-100刻度，已转小数)",
    "SP_BuyBox占比": "销售与流量报告：buyBoxPercentage(已转小数，按会话加权)；明显低于90%要先查Buy Box",
    "SP_SQP周期": "SQP只能按亚马逊自然周(周日~周六)取，与广告周不一定对齐",
    "SP_SQP曝光": "SQP：我方ASIN在该周所有返回查询中的曝光合计(各子体相加)；每个ASIN只返回Top100查询，是部分口径",
    "SP_SQP点击": "SQP：我方点击合计", "SP_SQP加购": "SQP：我方加购合计", "SP_SQP购买": "SQP：我方购买合计", "SP_SQP查询数": "SQP：父体下出现的不同搜索词个数", "SP_SQP覆盖ASIN数": "SQP：实际拿到数据的子体ASIN数(每父体最多取销量前N个子体，只是样本)",
    "SP_退货件数": "FBA退货报告：窗口内在亚马逊收货处理的退货件数(无退货=0)", "SP_退货率": "退货件数÷订购件数(无订购数则用领星销量7)；退货来自更早订单，单周波动大，宜看4周累计",
    "SP_退货原因Top3": "FBA退货报告：退货原因前三及件数", "SP_库龄90天以上件数": "库存计划报告：库龄>90天件数(91-180天+181天以上)",
    "SP_库龄181天以上件数": "库存计划报告：库龄>=181天件数(2026年起进入长期仓储附加费区间)",
    "SP_预估仓储费": "库存计划报告：下月预估月度仓储费合计(USD)", "SP_预估长期仓储费": "库存计划报告：下次预估长期仓储附加费合计(USD)",
    "SP_我方价格": "库存计划报告：your-price(我方标价，不含促销)在子体间的均值(USD)",
    "SP_我方实际售价": "逐子体：有促销价(sales-price，低于标价)时用促销价，否则用标价，再取均值(USD)；'价格高于竞品最低价比例'以它为分子",
    "SP_促销中SKU数": "sales-price 低于标价的子体数(正在促销/降价)", "SP_最低促销价": "促销中子体的最低促销价(USD)，与 保本价 比较",
    "SP_精选报价价格": "库存计划报告：featuredoffer-price(Buy Box 精选报价)在子体间的均值(USD)；可能是别的卖家或我方的促销价，不能和竞品最低价直接比出'我方贵多少'",
    "SP_精选报价为我方": "0-1，子体中'精选报价价格=我方价格'的比例(近似；Buy Box以SP_BuyBox占比为准)",
    "SP_竞品最低价": "库存计划报告：lowest-price-new-plus-shipping 均值(USD，可能包含我方自己的报价)",
    "SP_价格高于竞品最低价比例": "逐子体 我方实际售价(促销价优先)÷最低价-1 的均值(0.1=高10%)；<=0表示我方即最低。注意：最低价(lowest-price-new-plus-shipping)常常就是我方自己的促销价，用标价去比会误判为'比竞品贵'"}

SP_DOC.update(SP_DERIVED_DOC)
SP_DOC.update({
    "SP_流量覆盖天数": "销售与流量报告里实际有数据的天数(窗口7天)；<7说明亚马逊最近几天还没出数，SP_会话/页面浏览/订购件数/订购销售额只是N天合计",
    "SP_流量截止日": "销售与流量报告里最后一个有数据的日期",
    "SP_搜索周期": "品牌分析Search Catalog Performance的周期，亚马逊自然周(周日~周六)，与领星7天窗口可能错开",
    "SP_搜索曝光": "搜索漏斗：父体下各子体在搜索结果页的曝光合计(全部搜索词合计，含自然+广告，不能拆到具体搜索词)",
    "SP_搜索点击": "搜索漏斗：点击合计", "SP_搜索加购": "搜索漏斗：加购合计", "SP_搜索购买": "搜索漏斗：购买合计",
    "SP_搜索销售额": "搜索漏斗：来自搜索结果页的销售额(searchTrafficSales，已折USD)",
    "SP_搜索点击率": "SP_搜索点击÷SP_搜索曝光(先汇总再相除)", "SP_搜索转化率": "SP_搜索购买÷SP_搜索点击(先汇总再相除)；不等于广告转化率",
    "SP_搜索覆盖ASIN数": "搜索漏斗：报告里归到该父体的子体ASIN数"})

PCT_COLS = {"ACoS", "TACoS", "TACoS_实收", "CVR", "CTR", "广告销售占比", "平台费率", "盈亏ACoS", "盈亏ACoS上限_未含头程",
            "预算使用率", "IS", "全站转化率", "广告点击占会话", "SP_单位会话率", "SP_BuyBox占比", "SP_退货率",
            "窗口ACoS", "本周ACoS", "曝光份额", "点击份额", "加购份额", "购买份额",
            "SP_精选报价为我方", "SP_价格高于竞品最低价比例", "SP_搜索点击率", "SP_搜索转化率", "TACoS_SP", "广告销售占比_SP",
            "订单促销占比", "订单B2B占比", "订单待处理占比"}

# --------------------------------------------------------------------------- 工具
def load_config(path=None):
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    p = path or os.path.join(HERE, "config.json")
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            user = json.load(f)
        for k, v in user.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
    return cfg

def rel(p):
    return p if os.path.isabs(p) else os.path.join(HERE, p)

def num(s):
    """'--'、'有花费无销售额' -> NaN；'12.5%' -> 0.125；'1,234' -> 1234"""
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce")
    t = s.astype(str).str.strip().str.replace(",", "", regex=False)
    pct = t.str.endswith("%")
    v = pd.to_numeric(t.str.rstrip("%"), errors="coerce")
    return v.where(~pct, v / 100)

def find_file(folder, pattern):
    ms = [f for f in glob.glob(os.path.join(folder, "*.xls*"))
          if pattern in os.path.basename(f) and not os.path.basename(f).startswith("~$")]
    return max(ms, key=os.path.getmtime) if ms else None

def style_of(sku):
    m = re.match(r"^([A-Za-z]+\d+)", str(sku))
    return m.group(1).upper() if m else str(sku).split("-")[0].upper()

def code_of_store(store):
    return str(store).rsplit("-", 1)[-1].upper()

def derive(df):
    df["ACoS"] = df["花费"] / df["广告销售"].where(df["广告销售"] > 0)
    df["CVR"] = df["广告订单"] / df["点击"].where(df["点击"] > 0)
    df["CPC"] = df["花费"] / df["点击"].where(df["点击"] > 0)
    df["CTR"] = df["点击"] / df["曝光"].where(df["曝光"] > 0)
    return df

class Quality:
    def __init__(self):
        self.rows = []
    def add(self, level, item, detail):
        self.rows.append({"级别": level, "检查项": item, "结果": detail})
        if str(item).startswith("SP-API"):
            log(f"[{level}] {item}：{str(detail)[:220]}")
    def df(self):
        return pd.DataFrame(self.rows)

# --------------------------------------------------------------------------- 读取与清洗
def read_listing(path, cfg, week_end, q):
    df = pd.read_excel(path)
    n0 = len(df)
    df = df.drop_duplicates(subset=["店铺", "MSKU"], keep="first")
    q.add("WARN" if len(df) < n0 else "OK", "Listing重复行", f"店铺+MSKU重复 {n0 - len(df)} 行(已去重)")
    numc = ["价格", "FBA可售", "FBA预留", "FBA计划入库", "FBA标发在途", "FBA入库中", "FBA不可售", "FBM库存",
            "7日销量", "14日销量", "30日销量", "7日销售额", "14日销售额", "30日销售额",
            "7日广告费", "14日广告费", "30日广告费", "预估FBA费（API）", "平台费率", "评分", "Rating总数", "大类排名"]
    for c in numc:
        if c in df.columns:
            df[c] = num(df[c])
    ex = {str(c).upper() for c in (cfg.get("exclude_countries") or [])}
    if ex:
        hit = df["店铺"].map(code_of_store).isin(ex)
        if hit.any():
            tot7 = float(df["7日销量"].sum())
            ex7 = float(df.loc[hit, "7日销量"].sum())
            by = df[hit].groupby(df.loc[hit, "店铺"].map(code_of_store))["7日销量"].sum().round(0).astype(int).to_dict()
            q.add("INFO", "已排除站点", f"按 config.exclude_countries={sorted(ex)} 剔除 {int(hit.sum())} 行Listing(其中7日销量{ex7:.0f}件，占全部{(ex7 / tot7 if tot7 else 0):.1%}；按站点{by})。"
                  "若北美库存是共享池，US的库存天数没有扣除这些站点的消耗，占比小时可忽略")
            df = df[~hit].copy()
    fx = cfg["fx_to_usd"]
    rate = df["币种"].map(fx)
    bad = df.loc[rate.isna(), "币种"].dropna().unique().tolist()
    if bad:
        q.add("ERROR", "币种汇率缺失", f"config.fx_to_usd 缺少 {bad}，相关金额为空")
    for c in ["价格", "7日销售额", "14日销售额", "30日销售额", "7日广告费", "14日广告费", "30日广告费", "预估FBA费（API）"]:
        df[c] = df[c] * rate
    df["款"] = df["SKU"].map(style_of) if "SKU" in df.columns else df["MSKU"].map(style_of)
    df["父ASIN"] = df["父ASIN"].fillna(df["ASIN"])
    df["在途"] = df[["FBA计划入库", "FBA标发在途", "FBA入库中"]].fillna(0).sum(axis=1)
    # 领星"开售时间"常为空：用首单时间，缺失再退到创建时间(取日期部分)
    df["开售时间"] = pd.to_datetime(df["首单时间"], errors="coerce").fillna(
        pd.to_datetime(df["创建时间"].astype(str).str[:10], errors="coerce"))
    return df

def read_ad(path, cfg, q, name):
    df = pd.read_excel(path)
    df = df[df["店铺名称"].notna()].copy()
    df = df.rename(columns=AD_METRICS).rename(columns={"店铺名称": "店铺", "竞价-本币": "竞价", "当前竞价-本币": "竞价",
                                                     "默认竞价-本币": "默认竞价"})
    df = df.loc[:, ~df.columns.duplicated()]
    _ex = {str(c).upper() for c in (cfg.get("exclude_countries") or [])}
    if _ex:
        df = df[~df["店铺"].map(code_of_store).isin(_ex)].copy()
    for c in AD_SUM + ["直接销售", "间接销售", "IS", "预算", "竞价", "默认竞价"]:
        if c in df.columns:
            df[c] = num(df[c])
    rate = df["店铺"].map(lambda s: cfg["fx_to_usd"].get(CUR_BY_CODE.get(code_of_store(s), ""), np.nan))
    if rate.isna().any():
        q.add("ERROR", f"{name}币种汇率缺失", "存在无法识别站点/汇率的店铺，金额为空")
    for c in AD_MONEY:
        if c in df.columns:
            df[c] = df[c] * rate
    return df

def read_cost(path, cfg, q):
    if not path:
        q.add("WARN", "成本文件", "未找到 导出产品(按SKU) 文件，成本/盈亏ACoS为空")
        return None
    c = pd.read_excel(path, sheet_name=0)
    c = c.rename(columns={"*SKU": "SKU"})
    c["采购成本_CNY"] = num(c["采购成本(CNY)"])
    c.loc[c["采购成本_CNY"] <= 1, "采购成本_CNY"] = np.nan          # 0/1 视为占位，不当作真实成本
    c["采购交期_天"] = num(c["采购交期"]) if "采购交期" in c.columns else np.nan
    c.loc[c["采购交期_天"] <= 0, "采购交期_天"] = np.nan                 # 0 视为没维护
    out = c[["SKU", "采购成本_CNY", "采购交期_天"]].drop_duplicates("SKU")
    # 头程：工作表"按国家维护头程清关"的"默认头程费用(含税)"(SKU×国家)，接近0的值(如0.01)是没维护的占位，不当真实头程
    try:
        xl = pd.ExcelFile(path)
        sheet = next((x for x in xl.sheet_names if "头程" in x and "国家" in x), None)
        if sheet:
            f = xl.parse(sheet).rename(columns={"*SKU": "SKU"})
            col = next((x for x in f.columns if "默认头程费用" in str(x) and "币种" not in str(x)), None)
            ccol = next((x for x in f.columns if "默认头程费用币种" in str(x)), None)
            if col and "国家名称" in f.columns:
                f["头程原值"] = num(f[col])
                raw_n = len(f)
                sku_in_sheet = set(f["SKU"].astype(str))                 # 头程表里出现过的SKU(含只有占位值的)
                f = f[f["头程原值"] > float(cfg.get("freight_placeholder_max", 0.05))].copy()
                cur = f[ccol].fillna("CNY").astype(str).str.upper() if ccol else pd.Series("CNY", index=f.index)
                rate = cur.map(lambda x: 1.0 / cfg["cny_per_usd"] if x == "CNY" else cfg["fx_to_usd"].get(x, np.nan))
                f["头程_USD"] = f["头程原值"] * rate
                fv = f[["SKU", "国家名称", "头程_USD"]].drop_duplicates(["SKU", "国家名称"]).copy()
                fv["估算"] = False
                fv["估算方式"] = "实际"
                n_all = c["SKU"].astype(str).nunique()
                ph_only = len(sku_in_sheet - set(fv["SKU"].astype(str)))   # 只有0.01等占位值、没有有效头程的SKU
                if cfg.get("freight_impute_sibling", True):
                    # 同款同尺码不同颜色的头程实测完全一致：新建颜色(还没在头程表里维护)用兄弟SKU补，各颜色不一致的组不补
                    sz = lambda x: str(x).rsplit("-", 1)[-1]
                    fv["款"] = fv["SKU"].map(style_of); fv["尺码"] = fv["SKU"].map(sz)
                    ref = fv.groupby(["款", "尺码", "国家名称"])["头程_USD"].agg(["median", "nunique"]).reset_index()
                    ref = ref[ref["nunique"] == 1]
                    allsku = pd.DataFrame({"SKU": c["SKU"].astype(str).unique()})
                    allsku["款"] = allsku["SKU"].map(style_of); allsku["尺码"] = allsku["SKU"].map(sz)
                    need = allsku.merge(ref, on=["款", "尺码"], how="inner").merge(fv[["SKU", "国家名称"]], on=["SKU", "国家名称"], how="left", indicator=True)
                    need = need[need["_merge"] == "left_only"]
                    if len(need):
                        fv = pd.concat([fv[["SKU", "国家名称", "头程_USD", "估算", "估算方式"]],
                                        pd.DataFrame({"SKU": need["SKU"].values, "国家名称": need["国家名称"].values, "头程_USD": need["median"].values,
                                                      "估算": True, "估算方式": "同款同尺码"})], ignore_index=True)
                    n_imp, n_imp_sku = int(len(need)), int(need["SKU"].nunique())
                else:
                    n_imp, n_imp_sku = 0, 0
                # ---- 按克重反推：头程≈单品毛重×每公斤头程(同系列同国家的中位数，系列无参考时用该国家中位数)。
                #      实测同系列每公斤头程基本固定(如 ZJ 各款均为 9 元/kg)，所以缺头程的新款可按自己的毛重估
                n_w, n_w_sku, w_note = 0, 0, ""
                if cfg.get("freight_impute_weight", True) and "单品毛重" in c.columns:
                    wu = c["单品毛重单位"].astype(str).str.strip().str.lower() if "单品毛重单位" in c.columns else pd.Series("g", index=c.index)
                    c["_kg"] = num(c["单品毛重"]) * np.where(wu == "kg", 1.0, np.where(wu.isin(["lb", "lbs"]), 0.4536, 0.001))
                    kg = c.loc[c["_kg"] > 0].drop_duplicates("SKU").set_index(c.loc[c["_kg"] > 0].drop_duplicates("SKU")["SKU"].astype(str))["_kg"]
                    fam = lambda x: (re.match(r"^([A-Za-z]+)", str(x)) or re.match(r"(.*)", str(x))).group(1).upper()
                    real = fv[fv["估算方式"] == "实际"].copy()
                    real["kg"] = real["SKU"].astype(str).map(kg)
                    real = real[real["kg"] > 0]
                    real["系列"] = real["SKU"].map(fam)
                    real["每kg"] = real["头程_USD"] / real["kg"]
                    r_fam = real.groupby(["系列", "国家名称"])["每kg"].median()
                    r_cty = real.groupby("国家名称")["每kg"].median()
                    have = set(zip(fv["SKU"].astype(str), fv["国家名称"]))
                    rows_w = []
                    for sku_, kg_ in kg.items():
                        for cty in r_cty.index:
                            if (sku_, cty) in have:
                                continue
                            rt = r_fam.get((fam(sku_), cty), np.nan)
                            how = f"按克重(同系列{fam(sku_)})"
                            if pd.isna(rt):
                                rt, how = r_cty[cty], "按克重(国家中位)"
                            rows_w.append({"SKU": sku_, "国家名称": cty, "头程_USD": kg_ * rt, "估算": True, "估算方式": how})
                    if rows_w:
                        fv = pd.concat([fv, pd.DataFrame(rows_w)], ignore_index=True)
                        n_w, n_w_sku = len(rows_w), len({r_["SKU"] for r_ in rows_w})
                    us_ = r_fam[r_fam.index.get_level_values(1) == "美国"] * cfg["cny_per_usd"]
                    w_note = "；美国每公斤头程(元)：" + "、".join(f"{k[0]} {v:.1f}" for k, v in us_.items())
                out.attrs["freight"] = fv[["SKU", "国家名称", "头程_USD", "估算", "估算方式"]]
                _absent = sorted(set(c["SKU"].astype(str)) - sku_in_sheet)
                q.add("WARN" if (_absent or ph_only) else "OK", "头程表缺SKU",
                      f"产品表{n_all}个SKU；头程表里{len(_absent)}个SKU整行缺失(不是空值，是这些SKU在该表里根本没有行，多为新建颜色/新款没同步头程)，另有{ph_only}个只有0.01等占位值。"
                      f"已用同款同尺码其它颜色补了 {n_imp_sku} 个SKU(共{n_imp}个SKU×国家；各颜色头程一致的组才补)，"
                      f"再按单品毛重×同系列每公斤头程估算了 {n_w_sku} 个SKU(共{n_w}个SKU×国家){w_note}；估算值见 out/头程缺失清单_*.csv，请在领星补维护后重新导出")
                q.add("INFO", "头程来源", f"取自'{sheet}'工作表的'{col}'，按SKU×国家匹配；{len(f)}/{raw_n} 行为有效值(其余为0.01等占位或未维护)。"
                      "不含关税/清关费，利润仍可能偏高")
    except Exception as e:
        q.add("WARN", "头程读取失败", f"读取'按国家维护头程清关'工作表失败：{e}")
    return out

_G_F = re.compile(r"\bwom[ae]n'?s?\b|\bladies\b|\blady\b|\bfemale\b|\bgirls?\b|feminin|femenin|mujer|mulher|damen|femme", re.I)
_G_M = re.compile(r"\bm[ae]n'?s?\b|\bmale\b|\bboys?\b|\bgentlem[ae]n\b|masculin|hombre|homem|herren|homme", re.I)
_CATS = [(r"dress|vestido|robe dress", "连衣裙"), (r"\bpolo\b", "Polo衫"), (r"t-?shirt|\btee\b|camiseta", "T恤"),
         (r"blouse|blusa", "女式衬衫"), (r"shirt|camisa", "衬衫"), (r"\brobe\b|túnica|tunica", "长袍")]
_SLEEVE = [(r"sleeveless|sin mangas|sem mangas", "无袖"), (r"3/4|three.quarter|manga 3/4", "七分袖"), (r"half sleeve|meia manga|media manga", "中袖"),
           (r"short sleeve|manga curta|manga corta", "短袖"), (r"long sleeve|manga longa|manga larga", "长袖")]


def text_gender(t):
    """文本(标题/搜索词)里的性别：女/男/男女/''(未提及)"""
    t = str(t or "")
    f, m = bool(_G_F.search(t)), bool(_G_M.search(t))
    return "男女" if f and m else ("女" if f else ("男" if m else ""))


def product_profile(titles):
    """父体下各子体标题 → (性别, 品类, 袖长)。性别取子体多数；标题没写性别=未知(不猜)"""
    gs = [text_gender(t) for t in titles if isinstance(t, str) and t.strip()]
    gs = [g for g in gs if g]
    gender = max(set(gs), key=gs.count) if gs else "未知"
    joined = " ".join(str(t) for t in titles if isinstance(t, str)).lower()
    cat = next((c for pat, c in _CATS if re.search(pat, joined, re.I)), "未知")
    sleeves = sorted({sl for pat, sl in _SLEEVE if re.search(pat, joined, re.I)})
    return gender, cat, "/".join(sleeves) or "未写"


def dominant(prod_m, keys):
    """广告活动/广告组 -> 花费最多的父体；同时返回对应多个父体的组"""
    t = prod_m.copy()
    t["父ASIN"] = t["父ASIN"].fillna("未匹配")
    t["款"] = t["款"].fillna("未匹配")
    g = t.groupby(keys + ["父ASIN", "款"]).agg(s=("花费", "sum"), n=("MSKU", "count")).reset_index()
    g = g.sort_values(["s", "n"], ascending=False)
    first = g.drop_duplicates(keys)[keys + ["父ASIN", "款"]]
    multi = g.groupby(keys)["父ASIN"].nunique()
    return first, multi[multi > 1]

# --------------------------------------------------------------------------- 父体周表
def build_parent(L, prod_m, cost, spapi, week_end, cfg, q, orders=None, ship=None, replen=None):
    pos = lambda s: s[s > 0].mean()
    sale = L[L["状态"] == "在售"].copy()
    P = sale.groupby(["店铺", "父ASIN"]).agg(
        款=("款", lambda s: s.mode().iat[0]), 在售子体数=("MSKU", "count"),
        FBA可售=("FBA可售", "sum"), FBA预留=("FBA预留", "sum"), 在途=("在途", "sum"), FBA不可售=("FBA不可售", "sum"),
        销量7=("7日销量", "sum"), 销量14=("14日销量", "sum"), 销量30=("30日销量", "sum"),
        销售额7=("7日销售额", "sum"), 销售额14=("14日销售额", "sum"), 销售额30=("30日销售额", "sum"),
        领星广告费7=("7日广告费", "sum"), 标价=("价格", pos), FBA费=("预估FBA费（API）", pos),
        平台费率=("平台费率", pos), 评分=("评分", pos), 评论数=("Rating总数", "max"),
        大类排名=("大类排名", lambda s: s[s > 0].median()), 首次开售=("开售时间", "min")).reset_index()
    # ---- 没有在售子体、但仍有库存/在途的父体(例如新站点已发货、Listing 还是停售)：保留一行，避免库存和在途被悄悄丢掉
    ns = L[L["状态"] != "在售"].copy()
    ns["_inv"] = ns[["FBA可售", "FBA预留", "在途"]].fillna(0).sum(axis=1)
    have = set(zip(P["店铺"], P["父ASIN"]))
    new_parent = pd.Series([(a_, b_) not in have for a_, b_ in zip(ns["店铺"], ns["父ASIN"])], index=ns.index)
    ns = ns[(ns["_inv"] > 0) & new_parent]
    if len(ns):
        N = ns.groupby(["店铺", "父ASIN"]).agg(
            款=("款", lambda s: s.mode().iat[0]), 在售子体数=("MSKU", lambda s: 0),
            FBA可售=("FBA可售", "sum"), FBA预留=("FBA预留", "sum"), 在途=("在途", "sum"), FBA不可售=("FBA不可售", "sum"),
            销量7=("7日销量", "sum"), 销量14=("14日销量", "sum"), 销量30=("30日销量", "sum"),
            销售额7=("7日销售额", "sum"), 销售额14=("14日销售额", "sum"), 销售额30=("30日销售额", "sum"),
            领星广告费7=("7日广告费", "sum"), 标价=("价格", pos), FBA费=("预估FBA费（API）", pos),
            平台费率=("平台费率", pos), 评分=("评分", pos), 评论数=("Rating总数", "max"),
            大类排名=("大类排名", lambda s: s[s > 0].median()), 首次开售=("开售时间", "min")).reset_index()
        P = pd.concat([P, N], ignore_index=True)
        by = N.groupby("店铺").agg(父体数=("父ASIN", "count"), 在途件数=("在途", "sum"), 可售件数=("FBA可售", "sum"))
        q.add("INFO", "未在售但有库存/在途的父体",
              f"{len(N)} 个父体没有在售子体(Listing停售/未上架)，但有库存或在途，已保留为'未在售'行：" + "；".join(
                  f"{k}：{int(r_['父体数'])}个父体，在途{int(r_['在途件数'])}件，FBA可售{int(r_['可售件数'])}件" for k, r_ in by.iterrows())
              + "。货到仓前要确认Listing已激活")
    P["币种"] = "USD"   # 所有金额已折算为USD
    # ---- 商品属性(来自 Listing 标题；config.product_gender_override 可按款强制指定性别)
    if "标题" in L.columns:
        _src = L.sort_values("状态", key=lambda s_: (s_ != "在售").astype(int))
        _tt = _src.groupby(["店铺", "父ASIN"])["标题"].apply(list).to_dict()
        prof = [product_profile(_tt.get((a_, b_), [])) for a_, b_ in zip(P["店铺"], P["父ASIN"])]
        P["商品性别"] = [x[0] for x in prof]; P["品类"] = [x[1] for x in prof]; P["袖长"] = [x[2] for x in prof]
        P["标题摘要"] = [str((_tt.get((a_, b_)) or [""])[0])[:90] for a_, b_ in zip(P["店铺"], P["父ASIN"])]
    else:
        P["商品性别"], P["品类"], P["袖长"], P["标题摘要"] = "未知", "未知", "未写", ""
    ov = {str(k).upper(): v for k, v in (cfg.get("product_gender_override") or {}).items()}
    if ov:
        P["商品性别"] = [ov.get(str(k_).upper(), g_) for k_, g_ in zip(P["款"], P["商品性别"])]
    unk = P.loc[P["商品性别"] == "未知", "款"].unique().tolist()
    q.add("WARN" if unk else "OK", "商品性别识别", (f"{len(unk)} 个款标题里没写性别，报告不得推断：{unk[:10]}；可在 config.product_gender_override 指定" if unk
                                                  else f"全部父体已从标题识别性别：" + "、".join(f"{k_}={g_}" for k_, g_ in P.drop_duplicates("款")[["款", "商品性别"]].values)))
    P["评论数"] = P["评论数"].fillna(0)
    P["上架天数"] = (pd.Timestamp(week_end) - P["首次开售"]).dt.days
    P = P.drop(columns=["首次开售"])
    stop = L[L["状态"] != "在售"]
    n_stop = int((stop["FBA可售"].fillna(0) > 0).sum())
    _sk = stop[stop["FBA可售"].fillna(0) > 0]
    _detail = "；".join(f"{a_} {b_} {int(c_)}件" for a_, b_, c_ in zip(_sk["店铺"].head(10), _sk["MSKU"].head(10), _sk["FBA可售"].head(10)))
    q.add("WARN" if n_stop else "OK", "停售子体仍有FBA库存",
          f"{n_stop} 个SKU共 {int(stop['FBA可售'].fillna(0).sum())} 件(未计入父体库存)：{_detail}。这些库存卖不出去，需要重新激活或移除")
    if P["上架天数"].isna().any():
        q.add("INFO", "开售时间缺失", f"{int(P['上架天数'].isna().sum())} 个父体无开售时间，无法判断新品期")

    P["销量来源"] = "Listing滚动7日"
    if orders and orders.get("use"):
        oi = orders["items"].merge(L[["店铺", "MSKU", "父ASIN"]].drop_duplicates(["店铺", "MSKU"]), on=["店铺", "MSKU"], how="left")
        bad = oi[oi["父ASIN"].isna()]
        if len(bad):
            q.add("WARN", "订单导出->Listing匹配", f"{len(bad)} 个MSKU在Listing里找不到(件数{bad['件数'].sum():.0f})：{bad['MSKU'].head(8).tolist()}")
        og = oi.dropna(subset=["父ASIN"]).groupby(["店铺", "父ASIN"])[["件数", "销售额", "促销件数", "促销销售额", "B2B件数", "待处理件数", "退款件数", "换货件数"]].sum().reset_index()
        og = og.rename(columns={c: "订单" + c for c in og.columns if c not in ("店铺", "父ASIN")})
        P = P.merge(og, on=["店铺", "父ASIN"], how="left")
        cov_o = P["店铺"].isin(orders["stores"])
        for c in [c for c in og.columns if c.startswith("订单")]:
            P.loc[cov_o, c] = P.loc[cov_o, c].fillna(0)
        P["Listing销量7"], P["Listing销售额7"] = P["销量7"], P["销售额7"]
        a_, b_ = float(P.loc[cov_o, "订单件数"].sum()), float(P.loc[cov_o, "Listing销量7"].sum())
        q.add("INFO", "对账:订单导出 vs Listing销量7", f"{a_:.0f} vs {b_:.0f} (差 {abs(a_ - b_) / max(b_, 1e-9):.1%})；Listing 是滚动窗口，与周窗口不重合，差异属正常")
        if str(cfg.get("sales_source", "auto")) in ("auto", "orders"):
            P.loc[cov_o, "销量7"] = P.loc[cov_o, "订单件数"]
            P.loc[cov_o, "销售额7"] = P.loc[cov_o, "订单销售额"]
            P.loc[cov_o, "销量来源"] = "订单导出"
        net = (P["订单件数"] - P["订单换货件数"]).where(lambda v: v > 0)
        P["订单均价"] = P["订单销售额"] / net
        P["订单实收销售额"] = P["订单销售额"] - P["订单促销销售额"]          # 扣除促销(如Vine免费样品)的名义销售额
        net2 = (P["订单件数"] - P["订单换货件数"] - P["订单促销件数"]).where(lambda v: v > 0)
        P["订单实收均价"] = P["订单实收销售额"] / net2
        for a2, b2 in (("订单促销件数", "订单促销占比"), ("订单B2B件数", "订单B2B占比"), ("订单待处理件数", "订单待处理占比")):
            P[b2] = P[a2] / P["订单件数"].where(P["订单件数"] > 0)
    P["日均7"] = P["销量7"] / 7
    P["日均30"] = P["销量30"] / 30
    P = add_forecast(P, replen, cfg, q)
    P["含在途天数_预测"] = (P["FBA可售"] + P["在途"]) / P["日均_预测"].where(P["日均_预测"] > 0)
    P["库存天数_7"] = P["FBA可售"] / P["日均7"].where(P["日均7"] > 0)
    P["库存天数_30"] = P["FBA可售"] / P["日均30"].where(P["日均30"] > 0)
    P["含在途天数_7"] = (P["FBA可售"] + P["在途"]) / P["日均7"].where(P["日均7"] > 0)
    LEAD_DAYS.clear()
    SKU_AGE.clear()
    if "开售时间" in L.columns:                      # SKU级开售天数(首单时间，没有则创建时间)：新SKU不算滞销/冗余
        _age = (pd.Timestamp(week_end) - pd.to_datetime(L["开售时间"], errors="coerce")).dt.days
        SKU_AGE.update({(a_, str(b_)): c_ for a_, b_, c_ in zip(L["店铺"], L["MSKU"], _age) if pd.notna(c_)})
    if ship and ship.get("use"):
        P, _det = ship_summary(P, ship, cfg, q, L)
        SHIP_DETAIL["df"] = _det
    if replen and replen.get("use"):
        if ship and ship.get("use"):
            P = supply_summary(P, replen, ship, cfg, q)
        else:
            q.add("WARN", "补货建议", "本次没有FBA货件文件：本地仓库存/待交付无法和在途货件一起做断货模拟，只把补货建议的字段并入(不含断货模拟)")
    P["均价"] = (P["销售额7"] / P["销量7"].where(P["销量7"] > 0)).fillna(P["标价"])
    if "订单实收均价" in P.columns:      # 用实收均价(扣除换货0元单与Vine名义价)做盈亏线；全是促销/换货单时退回标价
        _use = P["销量来源"] == "订单导出"
        P.loc[_use, "均价"] = P.loc[_use, "订单实收均价"].fillna(P.loc[_use, "标价"])

    # ---- 成本与盈亏线
    if cost is not None:
        cs = sale.merge(cost, on="SKU", how="left")
        cg = cs.groupby(["店铺", "父ASIN"]).agg(采购成本_CNY=("采购成本_CNY", "mean"),
                                              采购交期_天=("采购交期_天", "max"),
                                              成本缺失子体数=("采购成本_CNY", lambda s: int(s.isna().sum()))).reset_index()
        P = P.merge(cg, on=["店铺", "父ASIN"], how="left")
        P["采购成本_USD"] = P["采购成本_CNY"] / cfg["cny_per_usd"]
        miss = P[P["成本缺失子体数"] > 0]
        q.add("WARN" if len(miss) else "OK", "成本覆盖",
              f"{len(miss)} 个父体存在成本缺失/占位(0或1)的子体：{', '.join(sorted(miss['款'].unique())[:20])}")
    else:
        P["采购成本_CNY"] = np.nan; P["成本缺失子体数"] = np.nan; P["采购成本_USD"] = np.nan; P["采购交期_天"] = np.nan
    fsheet = cost.attrs.get("freight") if cost is not None else None
    if fsheet is not None and len(fsheet):
        cf = sale[["店铺", "父ASIN", "SKU", "MSKU"]].copy()
        cf["国家名称"] = cf["店铺"].map(lambda x: CN_COUNTRY.get(code_of_store(x)))
        cf = cf.merge(fsheet, on=["SKU", "国家名称"], how="left")
        FREIGHT_GAP["df"] = cf[cf["头程_USD"].isna() | (cf["估算"].fillna(False).astype(bool))].assign(
            款=lambda d_: d_["SKU"].map(style_of), 状态=lambda d_: np.where(d_["头程_USD"].isna(), "缺失(补不上)", "已估算：" + d_["估算方式"].fillna("").astype(str)),
            建议头程_CNY=lambda d_: (d_["头程_USD"] * cfg["cny_per_usd"]).round(2))[["店铺", "款", "SKU", "国家名称", "状态", "建议头程_CNY"]]
        fg = cf.groupby(["店铺", "父ASIN"]).agg(头程_USD=("头程_USD", "mean"),
                                              头程估算子体数=("估算", lambda v: int(v.fillna(False).astype(bool).sum())),
                                              头程缺失子体数=("头程_USD", lambda v: int(v.isna().sum())),
                                              头程估算方式=("估算方式", lambda v: "、".join(sorted({x for x in v.dropna() if x != "实际"})))).reset_index()
        P = P.merge(fg, on=["店铺", "父ASIN"], how="left")
        miss_f = P[P["头程缺失子体数"] > 0]
        q.add("WARN" if len(miss_f) else "OK", "头程覆盖",
              f"{len(miss_f)} 个父体仍有头程补不上的子体(这些子体不计入头程均值，盈亏ACoS用其余子体的均值)：{', '.join(sorted(miss_f['款'].unique())[:20])}；"
              f"另有 {int((P['头程估算子体数'].fillna(0) > 0).sum())} 个父体含估算头程(同款同尺码或按克重反推，见'头程估算方式')："
              + "；".join(f"{a_}({b_})" for a_, b_ in zip(P.loc[P['头程估算子体数'].fillna(0) > 0, '款'], P.loc[P['头程估算子体数'].fillna(0) > 0, '头程估算方式']))[:300])
    else:
        P["头程_USD"] = np.nan; P["头程缺失子体数"] = np.nan; P["头程估算子体数"] = np.nan
    fr = {}
    if cfg.get("freight_by_style_csv") and os.path.exists(rel(cfg["freight_by_style_csv"])):
        f = pd.read_csv(rel(cfg["freight_by_style_csv"]), encoding="utf-8-sig")
        fr = dict(zip(f["款"].astype(str).str.upper(), f["头程_USD"]))
    P["头程_USD"] = P["头程_USD"].fillna(P["款"].map(fr))     # 工作表优先，其次按款CSV，最后默认值
    if cfg.get("freight_usd_per_unit_default") is not None:
        P["头程_USD"] = P["头程_USD"].fillna(cfg["freight_usd_per_unit_default"])
    if P["头程_USD"].isna().all():
        q.add("WARN", "头程成本缺失", "没有取到头程单件成本(成本表里无'按国家维护头程清关'工作表，也未配置CSV/默认值)：盈亏ACoS只给'未含头程'上限，实际盈亏线更低")
    if "首次断货_全供给_海运_天后" in P.columns:
        sea_d_, air_d_, prep_, rv_ = LEAD_DAYS.get("full", (30, 12, 2, 10))
        # 只有全部供给在数量上都不够(缺口>0)才需要新采购；缺口=0 时全供给断货只是到货时间问题(海运太慢)，新采购更慢也解决不了，留空
        need_ = P["未来60日缺口件数_含全部供给"] > 0
        P["新采购最晚下单_海运_天后"] = (P["首次断货_全供给_海运_天后"] - (P["采购交期_天"] + prep_ + sea_d_ + rv_)).where(need_)
        P["新采购最晚下单_空运_天后"] = (P["首次断货_全供给_空运_天后"] - (P["采购交期_天"] + prep_ + air_d_ + rv_)).where(need_)
    elif "海运最晚发货_天后" in P.columns:
        P["海运最晚下单_天后"] = P["海运最晚发货_天后"] - P["采购交期_天"]
        P["空运最晚下单_天后"] = P["空运最晚发货_天后"] - P["采购交期_天"]
    P["平台费"] = P["均价"] * P["平台费率"]
    base = P["均价"] - P["平台费"] - P["FBA费"] - P["采购成本_USD"]
    P["单件毛利_未含头程"] = base
    P["单件毛利"] = base - P["头程_USD"]
    # 保本价：售价扣平台费后刚好覆盖 采购+头程+FBA费(不含广告/仓储)；清货促销价不应低于它
    P["保本价"] = (P["采购成本_USD"] + P["头程_USD"] + P["FBA费"]) / (1 - P["平台费率"]).where(P["平台费率"] < 1)
    P["盈亏ACoS上限_未含头程"] = base / P["均价"]
    P["盈亏ACoS"] = P["单件毛利"] / P["均价"]

    # ---- 广告
    covered = set(prod_m["店铺"].unique()) if prod_m is not None else set()
    P["广告已覆盖"] = P["店铺"].isin(covered)
    if prod_m is not None and len(prod_m):
        t = prod_m.copy(); t["父ASIN"] = t["父ASIN"].fillna("未匹配")
        A = t.groupby(["店铺", "父ASIN"]).agg(曝光=("曝光", "sum"), 点击=("点击", "sum"), 广告花费=("花费", "sum"),
                                            广告销售=("广告销售", "sum"), 广告订单=("广告订单", "sum"),
                                            广告销量=("广告销量", "sum"), 广告活动数=("广告活动", "nunique"),
                                            广告组数=("广告组", "nunique")).reset_index()
        P = P.merge(A, on=["店铺", "父ASIN"], how="outer")
        for c in ["曝光", "点击", "广告花费", "广告销售", "广告订单", "广告销量", "广告活动数", "广告组数"]:
            P.loc[P["广告已覆盖"].fillna(P["店铺"].isin(covered)), c] = P.loc[
                P["广告已覆盖"].fillna(P["店铺"].isin(covered)), c].fillna(0)
        P["广告已覆盖"] = P["店铺"].isin(covered)
    else:
        for c in ["曝光", "点击", "广告花费", "广告销售", "广告订单", "广告销量", "广告活动数", "广告组数"]:
            P[c] = np.nan
    P["ACoS"] = P["广告花费"] / P["广告销售"].where(P["广告销售"] > 0)
    P["CVR"] = P["广告订单"] / P["点击"].where(P["点击"] > 0)
    P["CPC"] = P["广告花费"] / P["点击"].where(P["点击"] > 0)
    P["CTR"] = P["点击"] / P["曝光"].where(P["曝光"] > 0)
    P["TACoS"] = P["广告花费"] / P["销售额7"].where(P["销售额7"] > 0)
    # 实收口径(扣除促销/Vine名义销售额)，与数据包'窗口汇总'的 TACoS(窗口) 同口径；非订单口径的行退回 TACoS
    P["TACoS_实收"] = P["TACoS"]
    if "订单实收销售额" in P.columns:
        _use = P["销量来源"] == "订单导出"
        P.loc[_use, "TACoS_实收"] = P.loc[_use, "广告花费"] / P.loc[_use, "订单实收销售额"].where(P.loc[_use, "订单实收销售额"] > 0)
    P["广告销售占比"] = P["广告销售"] / P["销售额7"].where(P["销售额7"] > 0)
    _old_profit = P["销量7"] * P["单件毛利"] - P["广告花费"]
    if "订单件数" in P.columns:
        # 促销(Vine)与换货单位没有收入，但仍要付 采购+头程+FBA费；其余按实收均价算单件毛利
        _free = P["订单促销件数"].fillna(0) + P["订单换货件数"].fillna(0)
        _paid = (P["订单件数"].fillna(0) - _free).clip(lower=0)
        _new_profit = _paid * P["单件毛利"] - _free * (P["FBA费"] + P["采购成本_USD"] + P["头程_USD"]) - P["广告花费"]
        P["估算7日利润_含头程"] = np.where(P["销量来源"] == "订单导出", _new_profit, _old_profit)
    else:
        P["估算7日利润_含头程"] = _old_profit

    # ---- SP-API(自动拉取或手工CSV)
    for c in SP_COLS:
        P[c] = np.nan
    for _c in ["会话7", "自然会话估算", "自然转化率估算", "广告订购占比", "每会话广告成本", "每会话销售额", "会话相对店内中位", "转化率相对店内中位", "全站转化率", "广告点击占会话", "销量_SP", "销售额_SP", "TACoS_SP", "广告销售占比_SP", "库存天数_SP", "含在途天数_SP"]:
        P[_c] = np.nan
    if spapi is not None and len(spapi):
        keep = ["店铺", "父ASIN"] + [c for c in SP_COLS if c in spapi.columns]
        sdf = spapi[keep].drop_duplicates(["店铺", "父ASIN"])
        P = P.drop(columns=[c for c in keep if c in SP_COLS]).merge(sdf, on=["店铺", "父ASIN"], how="left")
        for c in SP_COLS:
            if c not in P.columns:
                P[c] = np.nan
        cdays = P["SP_流量覆盖天数"].where(P["SP_流量覆盖天数"] > 0).fillna(7)      # 无覆盖信息(如手工CSV)按7天
        scale = 7 / cdays                                                        # N天合计折算成7天
        units7 = P["SP_订购件数"] * scale
        den = units7.where(units7 > 0).fillna(P["销量7"].where(P["销量7"] > 0))
        P["SP_退货率"] = P["SP_退货率"].fillna(P["SP_退货件数"] / den)
        P["SP_搜索点击率"] = P["SP_搜索点击率"].fillna(P["SP_搜索点击"] / P["SP_搜索曝光"].where(P["SP_搜索曝光"] > 0))
        P["SP_搜索转化率"] = P["SP_搜索转化率"].fillna(P["SP_搜索购买"] / P["SP_搜索点击"].where(P["SP_搜索点击"] > 0))
        units = P["SP_订购件数"].where(P["SP_订购件数"].notna()).fillna(P["销量7"])      # 与会话同源同窗口优先
        P["全站转化率"] = units / P["SP_会话"].where(P["SP_会话"] > 0)
        P["销量_SP"] = P["SP_订购件数"] * scale
        P["销售额_SP"] = P["SP_订购销售额"] * scale
        P["TACoS_SP"] = P["广告花费"] / P["销售额_SP"].where(P["销售额_SP"] > 0)
        P["广告销售占比_SP"] = P["广告销售"] / P["销售额_SP"].where(P["销售额_SP"] > 0)
        _d = (P["销量_SP"] / 7).where(P["销量_SP"] > 0)
        P["库存天数_SP"] = P["FBA可售"] / _d
        P["含在途天数_SP"] = (P["FBA可售"] + P["在途"]) / _d
        sess7 = P["SP_会话"] * scale
        P["广告点击占会话"] = P["点击"] / sess7.where(sess7 > 0)
        # ---- 父体流量 vs 广告(都折算到7天口径)：会话=自然+广告 的总流量；点击不等于会话，只作相对判断
        P["会话7"] = sess7
        P["自然会话估算"] = (sess7 - P["点击"]).clip(lower=0).where(sess7 > 0)
        P["广告订购占比"] = P["广告销量"] / units7.where(units7 > 0)            # >100% 说明广告口径(7天归因、含光环、跨周)与订购口径不同，不是真的占比
        _org_u = (units7 - P["广告销量"]).where(P["广告销量"] < units7)           # 广告销量>=订购件数时无法拆分，留空而不是硬算
        P["自然转化率估算"] = _org_u / P["自然会话估算"].where(P["自然会话估算"] >= 30)
        P["每会话广告成本"] = P["广告花费"] / sess7.where(sess7 > 0)
        P["每会话销售额"] = P["销售额_SP"] / sess7.where(sess7 > 0)
        _el = (P["在售子体数"] > 0) & (sess7 >= 100)
        _ms, _mc = P[_el].groupby("店铺")["会话7"].median(), P[_el].groupby("店铺")["全站转化率"].median()
        P["会话相对店内中位"] = P["会话7"] / P["店铺"].map(_ms)
        P["转化率相对店内中位"] = P["全站转化率"] / P["店铺"].map(_mc)
        cov = {k: int(P[c].notna().sum()) for k, c in [("流量", "SP_会话"), ("退货", "SP_退货件数"), ("库龄/仓储", "SP_库龄90天以上件数"),
                                                      ("竞品价", "SP_竞品最低价"), ("搜索漏斗", "SP_搜索曝光"), ("SQP", "SP_SQP曝光")]}
        q.add("OK" if cov["流量"] else "WARN", "SP-API覆盖(有数据的父体数)", f"共{len(P)}个父体：{cov}")
        sub = P[P["SP_订购件数"].notna()]
        if len(sub):
            sc_ = 7 / sub["SP_流量覆盖天数"].where(sub["SP_流量覆盖天数"] > 0).fillna(7)
            a_, b_ = float((sub["SP_订购件数"] * sc_).sum()), float(sub["销量7"].sum())
            d_ = abs(a_ - b_) / max(b_, 1e-9)
            cd_ = sorted(sub["SP_流量覆盖天数"].dropna().unique().tolist())
            q.add("WARN" if d_ > 0.10 else "OK", f"对账:SP订购件数(折算7天) vs 销量7({'/'.join(sorted(sub['销量来源'].dropna().unique()))})", f"{a_:.0f} vs {b_:.0f} (差 {d_:.1%})；SP流量覆盖天数{cd_ or '未知'}；窗口起止日/时区不同会造成差异")
    else:
        q.add("INFO", "SP-API数据", "本周没有SP-API数据(未配置凭证/拉取失败/无手工CSV)：SP_开头列为空，AI不得对流量/转化/退货/库龄/竞品价下结论")

    # ---- 分级标签(仅作提示，不是决策)
    n = P["广告订单"].fillna(0)
    P["样本量"] = np.select([P["广告已覆盖"] == False, n == 0, n < cfg["sample_orders_low"], n < cfg["sample_orders_high"]],
                          ["无广告数据", "无订单", "低", "中"], "高")
    _dm = P["日均_预测"] if "日均_预测" in P.columns else P["日均7"]
    d = P["FBA可售"] / _dm.where(_dm > 0)                     # 与断货模拟同一日均
    P["库存状态"] = np.select([d.isna(), d < cfg["stock_low_days"], d > cfg["stock_high_days"]],
                            ["无销量", "紧张", "偏多"], "正常")
    _small = ((P["上架天数"] <= cfg["new_product_days"]) | (P["评论数"] == 0)) & (P["销量30"].fillna(0) < 10)   # 与'阶段=新品'同口径
    P.loc[_small & (P["库存状态"] != "紧张"), "库存状态"] = "新品样本不足"      # 新品卖得少，库存天数没有意义
    P["阶段"] = np.where((P["上架天数"] <= cfg["new_product_days"]) | (P["评论数"] == 0), "新品", "成熟")
    P.loc[P["在售子体数"] == 0, "阶段"] = "未在售"
    P.loc[P["在售子体数"] == 0, "库存状态"] = "未在售"
    P.insert(0, "周结束", str(pd.Timestamp(week_end).date()))
    front = ["周结束", "店铺", "款", "父ASIN", "阶段", "样本量", "库存状态", "销量来源"]
    return P[front + [c for c in P.columns if c not in front]].sort_values("广告花费", ascending=False, na_position="last")

# --------------------------------------------------------------------------- 存储
def upsert_windows(con, table, df, key_col):
    """按窗口覆盖写入：删除该表里与本次相同窗口的行再追加(用于跨周回填的 SP 销量历史)。"""
    df = df.copy()
    ex = con.execute("select name from sqlite_master where type='table' and name=?", (table,)).fetchone()
    if ex:
        cols = {r[1] for r in con.execute(f'pragma table_info("{table}")')}
        for c in df.columns:
            if c not in cols:
                con.execute(f'alter table "{table}" add column "{c}"')
        keys = sorted(df[key_col].astype(str).unique())
        con.execute(f'delete from "{table}" where "{key_col}" in ({",".join("?" * len(keys))})', keys)
    df.to_sql(table, con, if_exists="append", index=False)
    con.commit()

def upsert(con, table, df, week_end):
    df = df.copy()
    for c in df.columns:
        if str(df[c].dtype).startswith("datetime"):
            df[c] = df[c].astype(str)
    if "周结束" not in df.columns:
        df.insert(0, "周结束", week_end)
    ex = con.execute("select name from sqlite_master where type='table' and name=?", (table,)).fetchone()
    if ex:
        cols = {r[1] for r in con.execute(f'pragma table_info("{table}")')}
        for c in df.columns:
            if c not in cols:
                con.execute(f'alter table "{table}" add column "{c}"')
        con.execute(f'delete from "{table}" where 周结束=?', (week_end,))
    df.to_sql(table, con, if_exists="append", index=False)
    con.commit()

def save_xlsx(path, sheets):
    from openpyxl import load_workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        for name, df in sheets.items():
            df.to_excel(xw, sheet_name=name[:31], index=False)
    wb = load_workbook(path)
    for ws in wb:
        hdr = [c.value for c in ws[1]]
        for row in ws.iter_rows():
            for c in row:
                c.font = Font(name="Arial", size=10)
        for c in ws[1]:
            c.font = Font(name="Arial", size=10, bold=True, color="FFFFFF")
            c.fill = PatternFill("solid", fgColor="305496")
            c.alignment = Alignment(wrap_text=True, vertical="center")
        for i, h in enumerate(hdr, start=1):
            if h in PCT_COLS:
                for r in range(2, ws.max_row + 1):
                    ws.cell(r, i).number_format = "0.0%"
            letter = ws.cell(1, i).column_letter
            w = max([len(str(h))] + [len(str(ws.cell(r, i).value)) for r in range(2, min(ws.max_row, 60) + 1)
                                       if ws.cell(r, i).value is not None])
            ws.column_dimensions[letter].width = min(max(9, w * 1.15), 55)
        ws.freeze_panes = "A2"
    wb.save(path)

FIELD_DOC = pd.DataFrame([
    ("库存天数_7 / _30", "FBA可售 ÷ 日均7 / 日均30；日均7=销量7÷7(销量来源=订单导出 时为订单口径，否则为领星Listing滚动7日)，日均30=领星30日销量÷30；不含在途"),
    ("含在途天数_7", "(FBA可售+计划入库+标发在途+入库中) ÷ 7日日均"),
    ("评分 / 评论数", "评分=同父体下评分>0的子体均值(变体家族共用评分)；新子体评分0已剔除；评论数取最大值"),
    ("大类排名", "子体排名>0的中位数，仅供参考，不可用于判断"),
    ("均价", "销量来源=订单导出 时=订单实收均价(扣除换货与促销名义价)；否则=销售额7÷销量7；都没有时用标价"),
    ("盈亏ACoS", "(均价-平台费-FBA费-采购成本-头程) ÷ 均价；头程缺失时为空"),
    ("盈亏ACoS上限_未含头程", "同上但不扣头程，实际盈亏线一定低于它"),
    ("估算7日利润_含头程", "订单口径：(订单件数−促销−换货)×单件毛利 − (促销+换货)件数×(FBA费+采购+头程) − 广告花费；Listing口径：销量7×单件毛利−广告花费。未含退货、仓储"),
    ("TACoS / 广告销售占比", "广告花费÷销售额7 / 广告销售÷销售额7；销售额7含促销(Vine)名义销售额。销量来源=Listing滚动7日 时与广告窗口不重合，只看趋势"),
    ("TACoS_实收", "广告花费÷订单实收销售额(扣除促销名义销售额，与数据包'窗口汇总'的TACoS(窗口)同口径)；非订单口径的行=TACoS；实收为0时为空"),
    ("模拟起算日", "断货模拟与'首次断货_天后'等天数的起算日=FBA货件/补货建议快照日，不是周结束日"),
    ("预算使用率", "日均花费(窗口花费÷7)÷预算；预算是导出时的当前值，周内改过预算时可能>100%"),
    ("样本量", "按广告订单数分级：无订单/低/中/高，阈值见 config"),
    ("上架天数", "距父体下最早子体首单日期(无首单则用创建日期)的天数"),
    ("阶段", "上架天数<=new_product_days 或评论数=0 视为新品；没有在售子体但有库存/在途的父体为'未在售'"),
    ("广告已覆盖", "False表示该店铺-站点本周没有广告报表，广告列为空(不是0)"),
    ("销量来源", "订单导出=销量7/销售额7来自领星订单导出(与周窗口、SP口径对齐，逐日对账结果见'数据质量'的'对账:订单导出 vs SP每日订购')；Listing滚动7日=领星Listing的滚动窗口(与周窗口不重合)；原值保留在 Listing销量7/Listing销售额7"),
    ("订单件数/订单销售额", "订单导出窗口内：件数=数量合计(Canceled为0，含Pending/B2B/换货)，销售额=Item Price合计(不含运费税费)"),
    ("订单均价", "订单销售额÷(订单件数-换货件数)；换货订单销售额为0，不应拉低均价"),
    ("订单促销销售额/订单实收销售额/订单实收均价", "促销(如Vine免费样品)单位在订单里有名义Item Price(与SP销售额口径一致)，但销售收益为0、没有真实收入；实收销售额=订单销售额−促销名义销售额；实收均价=实收销售额÷(件数−换货−促销)"),
    ("货件_已发货在途件数/货件_入库中待收件数", "领星FBA货件：SHIPPED/IN_TRANSIT 的已发货件数 / RECEIVING 的(已发货-签收量，逐SKU取>=0)；已与Listing在途逐父体核对(差异来自发往停售子体的件数)"),
    ("货件_已签收待上架件数", "亚马逊最近 signed_pending_days(默认7)天已签收、还没进可售的件数(按货件签收明细的净签收；Listing可售/在途里都没有这部分)；断货模拟按 签收日+上架天数 可售计入"),
    ("最早到仓日/最晚到仓日/最晚可售日", "到仓=送达时段起/止(卖家中心登记值，未获货代确认的只是预估)，入库中=开始入库日；可售日=到仓日+receiving_avail_days(默认10天，亚马逊收货上架)，断货模拟按可售日入库"),
    ("到仓_保守_7日内/8-14日/15-30日/30日后", "按'送达时段止'(保守)分桶的到仓件数"),
    ("断货天数_乐观/保守", "用日均7逐日模拟未来shipment_horizon_days(默认60)天：乐观=送达时段起+上架天数可售，保守=送达时段止+上架天数可售；库存不够卖满一天日均的天数。不含采购在途/本地仓库存"),
    ("首次断货_天后", "保守到仓情形下，第几天首次断货(空=60天内不断货)"),
    ("本地可用/待交付/待检待上架量/本地仓在途/采购计划", "领星补货建议：本地可用等取自父ASIN汇总行；待交付改用SKU明细去重后合计(父汇总常含无Listing SKU而虚高)。同账号各站点共享池只计入主站(US优先)，其它站点置0"),
    ("待交付_PO数/待交付_最早预计到货日/待交付_最早/最晚预计可售日", "来自待交付详情的采购单明细：预计到货=到本地仓，预计可售=到FBA可卖；已过预计到货日的按晚到天数顺延预计可售日"),
    ("待交付_已过预计到货件数/待交付_无PO明细件数", "预计到货日已过仍未到的PO件数；父体'待交付'汇总比能对上单据号的PO件数多出来的部分(按该父体最晚预计可售日保守处理)"),
    ("断货天数_含待交付/断货天数_全供给_空运/海运", "在断货模拟里逐层加供给：S1=S0+工厂待交付(按预计可售日)；S2=S1+本地仓库存今天发出(空运/海运，到仓=备货local_ship_prep_days+历史中位运输天数+上架天数)"),
    ("本地仓最少空运/海运件数_避免断货", "本地仓最少发出多少件才能让60天内不断货；0=不发也不断货(FBA+在途+待交付已足够)；空=全部发出也避免不了(件数不够，或断货发生在这批货可售之前，看断货天数_全供给)，或没有本地库存/没有销量。父体级合计，未按尺码拆分"),
    ("新采购最晚下单_海运/空运_天后", "全供给情形首次断货_天后 − 采购交期 − 备货天数 − 运输天数 − 上架天数；<0=即使现在向工厂下单也来不及；空=60天内不需要新采购(含：未来60日缺口件数_含全部供给=0，全供给仍断货只是到货时间问题，应走空运/控速，新采购解决不了)"),
    ("未来60日缺口件数_含全部供给", "日均7×60 − FBA可售 − 表内在途 − 待交付 − 本地可用，>0 表示60天内还需新采购这么多(父体级，未按尺码拆)"),
    ("领星_*", "领星补货建议里的7天日均、断货时间、建议采购日/量、本地发FBA量、建议本地发货日、备货时长：领星自己的口径，仅作对照"),
    ("采购交期_天", "成本表'采购交期'(工厂交期，父体取各子体最大值)；0视为没维护"),
    ("海运/空运最晚下单_天后", "最晚发货_天后 − 采购交期_天：现在(快照日)起最晚第几天必须向工厂下单才赶得上；负数=现在下单也来不及。只含采购交期+历史中位运输天数，不含国内质检/装箱/订舱时间，也不含已下单未交付的采购单(缺待交付数据，已下单时偏保守)"),
    ("头程估算子体数/头程估算方式", "头程表里没有有效头程、用估算值补上的子体数与方式：①同款同尺码其它颜色(各颜色一致才补)；②按克重反推=单品毛重×同系列同国家每公斤头程中位数(系列无参考时用国家中位)。估算头程计入盈亏ACoS/利润，结论需注明'头程为估算'"),
    ("海运/空运最晚发货_天后", "首次断货_天后 − (历史海运/空运发货→开始入库中位天数 + 上架天数receiving_avail_days)；负数=现在发也来不及(海运)，只能空运或控速"),
    ("建议海运发货件数", "今天从本地仓海运发出、在(备货+海运中位+上架)天后可售的一批货，最少多少件能让它可售起到'海运发货_覆盖至第N天'(=到可售天数+发货周期replen_cycle_days+安全库存safety_stock_days)都不断货；可售日之前的断货海运补不上，不计(看断货天数/空运)；已计入FBA可售、在途、工厂待交付；0=不用发；空=无销量。父体级合计，需按尺码拆分"),
    ("海运发货_本地仓不足件数", "建议海运发货件数 − (本地可用 − 建议空运件数)；本地仓先保空运，>0 表示本地仓不够发，差额要新采购或控速"),
    ("建议空运件数", "今天从本地仓空运发出、在 空运可售_天后(备货+空运中位+上架)可售的一批货，最少多少件能撑到 海运可售_天后(之后由今天发的海运接上)；0=海运前不会断货，不用空运；空=无销量。空运可售之前的断货补不上，见 空运可售前无法避免断货天数(只能控速)。父体级合计，需按尺码拆分"),
    ("建议空运件数_按近7天日均", "敏感性：同 建议空运件数，但日均改用 日均7(最近一周)；只在日均7明显高于日均_预测(近期放量)时计算。两者差距大=空运量对'放量是否持续'很敏感，需人工判断"),
    ("日均_预测", "断货模拟与发货/空运件数用的日均：补货建议(截至快照日)的7/14/30/60天日均按 demand_weights(默认0.4/0.3/0.2/0.1)加权；上架天数不足的窗口不参与；没有补货建议的父体用 日均7、领星Listing 14/30日销量。日均7(订单周窗口)只用于描述本周"),
    ("日均_7天/14天/30天/60天", "补货建议里截至快照日的各窗口销量÷天数(父体=各子体合计)，用于算 日均_预测；无补货建议时 7天=日均7、14/30天=领星Listing"),
    ("尺码_*", "尺码级(子体SKU)断货模拟：亚马逊按子体判断缺货——某尺码可售为0就是断货(已到仓但未上架的不算可售)。每个SKU用自己的日均_预测、FBA可售、在途货件、已签收待上架、待交付PO、本地可用(同账号共享池)逐日模拟60天；父体行是各SKU合计"),
    ("尺码_缺货件数_含待交付", "各尺码在60天内卖不出去的件数合计(FBA+在途+待交付都算上仍缺)；父体级模拟会用别的尺码库存抵消，低估缺货"),
    ("建议空运件数_尺码合计/建议海运发货件数_尺码合计", "逐SKU算 空运(撑到海运可售日)/海运(覆盖到可售+发货周期+安全库存)最少件数再求和，明细见 空运尺码明细/海运尺码明细；比父体级更准(尺码之间不能互相顶替)"),
    ("尺码_当前断码数/尺码_当前断码_主力数/尺码_当前断码", "快照日可售为0且日均>=0.1的SKU数；主力=占父体需求>=5%。明细里注明'已到仓N件待上架'(入库中/已签收，按上架天数后可售)还是'无货在途'。按亚马逊规则，可售为0就是断货，到仓未上架也买不到"),
    ("保本价", "(采购成本+头程+FBA费)÷(1−平台费率)：清货促销的价格下限(不含广告与仓储费)；亚马逊建议促销价常低于它"),
    ("冗余_FBA件数/冗余_总件数", "逐SKU：FBA冗余=FBA可售+在途−日均_预测×excess_fba_days(默认90天)；总冗余=FBA+在途+待交付+本地仓−日均_预测×excess_total_days(默认180天)，零销量SKU全部算冗余；父体为合计，明细见 *_明细"),
    ("冗余_可削减PO件数", "逐SKU：待交付中超出总冗余线的部分(=min(待交付, 总冗余))，可与工厂协商减少或延后"),
    ("滞销_SKU数/滞销_FBA件数/滞销_本地件数", "开售且首次到FBA都已>=slow_min_age_days(默认45天)、近30天零销量、有FBA或本地仓库存的SKU；本地仓共享池只在主站行计一次。开售或首次到FBA不足45天、或从没到过FBA(本地未发)的计入 新SKU观察数，不算滞销/冗余"),
    ("亚马逊冗余件数/亚马逊建议促销明细", "库存计划报告：estimated-excess-quantity 合计；recommended-action=Create sale/Lower price 的SKU及亚马逊建议价(常低于保本价，只作参考)"),
    ("尺码_本地仓不足件数", "逐SKU：建议空运+海运件数 − 本地可用(共享池) 的缺口合计；>0 表示本地仓对应尺码不够发，需要新采购/催PO/控速"),
    ("空运_本地可发件数/海运_本地可发件数", "逐SKU 建议件数中本地仓(共享池，先保空运)实际能发的部分合计，明细见 空运尺码明细/海运尺码明细；本地仓发不出的见 空运本地不足明细/尺码_本地仓不足明细"),
    ("空运可售前无法避免断货天数", "只算FBA+在途+待交付时，在空运可售日之前就断货的天数：任何发货都来不及，只能降竞价/降预算/提价控速"),
    ("空运发货_本地仓不足件数", "建议空运件数 − 本地可用；>0 表示本地仓不够空运"),
    ("未来60日缺口件数", "日均7×60 − FBA可售 − 表内在途(不含采购在途/本地仓库存)；>0 表示60天内需要再补这么多"),
    ("在途_发往非在售子体件数", "在途里发往停售/未激活子体的件数，到仓后也不能卖，需先激活Listing"),
    ("订单促销/B2B/待处理占比", "占订单件数的比例；促销占比可提示销量是否被促销拉动"),
    ("全站转化率", "SP_订购件数÷SP_会话(无订购数时用领星销量7)；与亚马逊口径SP_单位会话率应接近"),
    ("广告点击占会话", "广告点击÷会话7，近似值(点击不等于会话，一个会话可含多次点击)，只看相对大小与趋势"),
    ("会话7/自然会话估算", "会话7=SP销售与流量报告的会话折算到7天(自然+广告的总流量)；自然会话估算=会话7−广告点击(不小于0)，粗略"),
    ("广告订购占比", "广告销量÷SP订购件数(折算7天)；>100% 表示广告归因口径(7天归因、含光环、跨周)与订购口径不同，此时自然转化率估算留空"),
    ("自然转化率估算", "(SP订购件数−广告销量)÷自然会话估算；广告销量>=订购件数或自然会话<30 时为空"),
    ("每会话销售额/每会话广告成本", "销售额_SP÷会话7 / 广告花费÷会话7：每个会话带来多少收入、花了多少广告费"),
    ("会话相对店内中位/转化率相对店内中位", "该父体的会话7(或全站转化率)÷同店铺中位数(只统计会话>=100的在售父体)；1=中位水平，不足100会话的店铺为空"),
] + [(k, v) for k, v in SP_DOC.items()], columns=["字段", "口径说明"])


# --------------------------------------------------------------------------- SP-API 自动数据
SP_DAILY = {}     # 店铺 -> DataFrame(date, units, sales)：销售与流量报告的每日订购件数/销售额，用来和订单导出逐日对账

SP_MARKETPLACE = {
    "US": ("ATVPDKIKX0DER", "US"), "CA": ("A2EUQ1WTGCTBG2", "CA"), "MX": ("A1AM78C64UM0Y8", "MX"),
    "BR": ("A2Q3Y263D00KWC", "BR"), "UK": ("A1F83G8C2ARO7P", "UK"), "DE": ("A1PA6795UKMFR9", "DE"),
    "FR": ("A13V1IB3VIYZZH", "FR"), "IT": ("APJ6JRA9NG5V4", "IT"), "ES": ("A1RKKUPIHCS9HS", "ES"),
    "JP": ("A1VC38T7YXB528", "JP"), "AU": ("A39IBJ37TRP1C6", "AU"),
}
_PERM_HINT = ("若报 Unauthorized/403：销售流量与SQP需要应用勾选 Brand Analytics 角色(SQP还要品牌备案)，"
              "退货与库存计划报告需要 Amazon Fulfillment 角色；改角色后要重新授权拿新的 Refresh Token")

def _account_of(store):
    return str(store).rsplit("-", 1)[0]

def _find_spapi_credential_file(cfg):
    p = str(cfg.get("spapi_credential_file") or "").strip()
    if p:
        p = rel(p)
        if os.path.isfile(p):
            return p
        raise FileNotFoundError(f"config.spapi_credential_file 文件不存在：{p}")
    for name in ("亚马逊sp-api.txt", "amazon_sp_api.txt", "sp-api.txt", "spapi.txt"):
        p = os.path.join(HERE, name)
        if os.path.isfile(p):
            return p
    txts = [p for p in glob.glob(os.path.join(HERE, "*.txt"))
            if os.path.isfile(p) and "weekly" not in os.path.basename(p).lower()]
    if len(txts) == 1:
        return txts[0]
    if not txts:
        raise FileNotFoundError("脚本同目录没有找到 SP-API 凭证 txt")
    raise FileNotFoundError("脚本同目录发现多个 txt，无法安全猜测凭证文件：" + ", ".join(os.path.basename(x) for x in txts)
                            + "；请在 config.json 设置 spapi_credential_file 或 spapi_accounts")

def _parse_spapi_credentials(path):
    raw = {}
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or ":" not in line:
                continue
            k, v = line.split(":", 1)
            raw[k.strip()] = v.strip()
    creds = {"lwa_app_id": raw.get("LWA Client ID"), "lwa_client_secret": raw.get("LWA Client Secret")}
    aws = {"aws_access_key": raw.get("Access key ID"), "aws_secret_key": raw.get("Secret access key"), "role_arn": raw.get("role_arn")}
    if all(aws.values()):          # AWS 签名字段已非必需：三项齐全才带上，兼容旧版库
        creds.update(aws)
    refresh = raw.get("Refresh Token")
    missing = [k for k in ("lwa_app_id", "lwa_client_secret") if not creds.get(k)]
    if not refresh:
        missing.append("Refresh Token")
    if missing:
        raise ValueError(f"SP-API 凭证缺少字段：{missing}")
    return creds, refresh

def _build_reports_client(creds, refresh_token, code):
    if not SPAPI_IMPORT_OK:
        raise RuntimeError("未安装 python-amazon-sp-api (pip install python-amazon-sp-api)")
    if code not in SP_MARKETPLACE:
        raise ValueError(f"暂不支持站点：{code}")
    return Reports(credentials=creds, marketplace=getattr(Marketplaces, code), refresh_token=refresh_token)

def _resolve_accounts(cfg, stores, q):
    """店铺名 -> (凭证, refresh_token, 凭证文件名)。同一站点的多个卖家账号必须各配各的凭证。"""
    out = {}
    acc_cfg = cfg.get("spapi_accounts") or {}
    if acc_cfg:
        for s in stores:
            f = acc_cfg.get(_account_of(s))
            if not f:
                continue
            try:
                p = rel(f)
                if not os.path.isfile(p):
                    raise FileNotFoundError(f"凭证文件不存在：{p}")
                c, r = _parse_spapi_credentials(p)
                out[s] = (c, r, os.path.basename(p))
            except Exception as e:
                q.add("WARN", f"SP-API凭证:{s}", str(e))
        miss = [s for s in stores if s not in out]
        if miss:
            q.add("INFO", "SP-API未覆盖店铺", f"spapi_accounts 未提供这些店铺的凭证，跳过：{miss}")
        return out
    path = _find_spapi_credential_file(cfg)
    creds, refresh = _parse_spapi_credentials(path)
    prefixes = sorted({_account_of(s) for s in stores})
    single = str(cfg.get("spapi_single_account") or "").strip()
    if single:
        targets = [s for s in stores if _account_of(s) == single]
    elif len(prefixes) == 1 or cfg.get("spapi_allow_duplicate_store_aliases"):
        targets = list(stores)
    else:
        q.add("WARN", "SP-API账号不明确",
              f"Listing 里有多个卖家账号{prefixes}，但只找到一个凭证({os.path.basename(path)})，无法确定它属于谁。"
              "请在 config.json 设置 spapi_accounts={'账号名':'凭证文件'}(每个账号一个文件)，或 spapi_single_account='该凭证所属账号名'")
        return {}
    q.add("INFO", "SP-API凭证", f"使用 {os.path.basename(path)} 拉取：{targets}")
    return {s: (creds, refresh, os.path.basename(path)) for s in targets}

def _spapi_range(week_end, lag_days=0):
    end = datetime.strptime(str(week_end), "%Y-%m-%d") - timedelta(days=int(lag_days))
    start = end - timedelta(days=6)
    return start.strftime("%Y-%m-%dT00:00:00Z"), end.strftime("%Y-%m-%dT23:59:59Z")

def _sqp_week(week_end):
    """SQP 只能按亚马逊自然周(周日~周六)取：取 <= week_end 的最近一个周六作为周末。
    周结束日本身是周六时，SQP周期与它完全对齐(原始数据日后按亚马逊周对齐时即如此)。返回 (起ISO, 止ISO, 周六date)"""
    d = datetime.strptime(str(week_end), "%Y-%m-%d")
    sat = d - timedelta(days=(d.weekday() - 5) % 7)
    sun = sat - timedelta(days=6)
    return sun.strftime("%Y-%m-%dT00:00:00Z"), sat.strftime("%Y-%m-%dT23:59:59Z"), sat.date()

def _cache_path(cfg, store, tag, week_end):
    d = rel(cfg.get("spapi_cache_dir", "data/spapi_cache"))
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, re.sub(r"[^A-Za-z0-9_.-]+", "_", f"{store}_{tag}_{week_end}") + ".bin")

def _is_throttle(e):
    s = f"{type(e).__name__} {e}"
    return any(k in s for k in ("Throttl", "QuotaExceeded", "429"))

def _create_with_retry(client, spec, cfg, tag=""):
    tries = 1 if spec.get("soft") else int(cfg.get("spapi_api_retries", 5))      # 低优先级(历史窗口)限流就跳过，不干等
    sleep_s = int(cfg.get("spapi_throttle_sleep_seconds", 65))
    kw = {"reportType": spec["type"], "marketplaceIds": [spec["mp"]], "reportOptions": spec.get("options") or {}}
    if spec.get("start"):
        kw["dataStartTime"] = spec["start"]
        kw["dataEndTime"] = spec["end"]
    for i in range(tries):
        try:
            res = client.create_report(**kw)
            rid = (getattr(res, "payload", None) or {}).get("reportId")
            if not rid:
                raise RuntimeError(f"创建报告失败：{getattr(res, 'payload', None)}")
            return rid
        except Exception as e:
            if _is_throttle(e) and i < tries - 1:
                log(f"{tag} 创建报告被亚马逊限流，{sleep_s}秒后重试({i + 1}/{tries - 1})；createReport 限额约每分钟1次，报告多时慢是正常的")
                time.sleep(sleep_s)
                continue
            raise

def _get_status(client, rid):
    pl = getattr(client.get_report(rid), "payload", None) or {}
    return str(pl.get("processingStatus", "")).upper(), pl

def _doc_bytes(client, doc_id):
    import requests
    try:
        res = client.get_report_document(doc_id, decrypt=False)
    except TypeError:
        res = client.get_report_document(doc_id)
    pl = getattr(res, "payload", None) or {}
    if pl.get("url"):
        r = requests.get(pl["url"], timeout=180)
        r.raise_for_status()
        data = r.content
        if str(pl.get("compressionAlgorithm") or "").upper() == "GZIP" or data[:2] == b"\x1f\x8b":
            data = gzip.decompress(data)
        return data
    doc = pl.get("document")
    if doc is not None:
        return doc.encode("utf-8") if isinstance(doc, str) else bytes(doc)
    raise RuntimeError(f"报告文档没有下载地址：{pl}")

def _run_reports(client, specs, cfg, label="", deadline=None):
    """先批量创建(并行排队)，再同时轮询所有报告，谁先好先处理；等待期间定时打印进度。
    返回 {name: (状态, bytes或错误文字)}；状态 DONE/CACHE/CANCELLED/ERROR。
    未取回的 reportId 记在缓存目录 *.rid，下次运行自动续用，不重复创建。"""
    deadline = deadline or (time.time() + int(cfg.get("spapi_total_timeout_seconds", 2400)))
    out, pending = {}, {}
    refresh = bool(cfg.get("spapi_refresh"))
    per_to = int(cfg.get("spapi_report_timeout_seconds", 600))
    poll = max(1, int(cfg.get("spapi_poll_seconds", 15)))
    hb = max(1, int(cfg.get("spapi_heartbeat_seconds", 30)))
    n = len(specs)
    for k, sp in enumerate(specs, 1):
        tag = f"{label} [{k}/{n}] {sp['name']}"
        cp = sp["cache"]; ridf = cp + ".rid"
        if not refresh and os.path.exists(cp):
            out[sp["name"]] = ("CACHE", Path(cp).read_bytes())
            log(f"{tag} 命中缓存，跳过请求")
            continue
        if client is None:                   # 仅缓存模式(--spapi-cache-only)：不调用亚马逊
            out[sp["name"]] = ("ERROR", "仅缓存模式：本地没有该报告的缓存，未请求亚马逊")
            log(f"{tag} 仅缓存模式且无缓存，跳过")
            continue
        rid = None
        if not refresh and os.path.exists(ridf):
            try:
                old = Path(ridf).read_text(encoding="utf-8").strip()
                st, _ = _get_status(client, old)
                if st in ("IN_QUEUE", "IN_PROGRESS", "DONE"):
                    rid = old
                    log(f"{tag} 续用上次未取回的报告 {old} ({st})")
            except Exception:
                rid = None
        if rid is None:
            if time.time() > deadline:
                out[sp["name"]] = ("ERROR", "已超过 spapi_total_timeout_seconds，本次未创建(下次运行会继续)")
                log(f"{tag} 总时限已到，跳过创建")
                continue
            try:
                rid = _create_with_retry(client, sp, cfg, tag)
                Path(ridf).write_text(rid, encoding="utf-8")
                log(f"{tag} 已创建 reportId={rid}")
            except Exception as e:
                out[sp["name"]] = ("ERROR", str(e))
                log(f"{tag} 创建失败：{str(e)[:200]}")
                continue
        pending[rid] = {"sp": sp, "tag": tag, "t0": time.time(), "st": "CREATED"}
    total, t_start, last_hb = len(pending), time.time(), time.time()
    if total:
        log(f"{label} {total} 份报告已提交，开始等待亚马逊生成(每{poll}秒查询一次，每{hb}秒汇报一次进度)")
    while pending:
        for rid, it in list(pending.items()):
            sp, tag = it["sp"], it["tag"]
            try:
                st, meta = _get_status(client, rid)
            except Exception as e:
                if _is_throttle(e):
                    continue
                out[sp["name"]] = ("ERROR", str(e))
                log(f"{tag} 查询状态失败：{str(e)[:200]}")
                del pending[rid]
                continue
            it["st"] = st or it["st"]
            waited = time.time() - it["t0"]
            if st == "DONE":
                try:
                    data = _doc_bytes(client, meta["reportDocumentId"])
                    tmp = sp["cache"] + ".tmp"
                    Path(tmp).write_bytes(data)
                    os.replace(tmp, sp["cache"])
                    if os.path.exists(sp["cache"] + ".rid"):
                        os.remove(sp["cache"] + ".rid")
                    out[sp["name"]] = ("DONE", data)
                    log(f"{tag} 完成，下载{len(data) / 1024:.0f}KB，本份等待{_fmt_sec(waited)}")
                except Exception as e:
                    out[sp["name"]] = ("ERROR", str(e))
                    log(f"{tag} 下载失败：{str(e)[:200]}")
                del pending[rid]
            elif st in ("CANCELLED", "FATAL"):
                if os.path.exists(sp["cache"] + ".rid"):
                    os.remove(sp["cache"] + ".rid")
                if st == "CANCELLED":
                    out[sp["name"]] = ("CANCELLED", "亚马逊取消了报告(可能该时段无数据，也可能被取消)")
                else:
                    out[sp["name"]] = ("ERROR", f"FATAL: {meta}")
                log(f"{tag} 状态 {st}，不再等待")
                del pending[rid]
            elif waited > per_to or time.time() > deadline:
                why = ("按 --sp-wait 0 不等待" if per_to == 0 else f"单份等待超过{per_to}秒") if waited > per_to else "总时限已到"
                out[sp["name"]] = ("ERROR", f"{why}仍未生成(状态{it['st']})；报告仍在亚马逊生成，下次运行会自动续用")
                log(f"{tag} {why}，放弃等待(状态{it['st']})，下次运行会续用 {rid}")
                del pending[rid]
        if not pending:
            break
        time.sleep(poll)
        if time.time() - last_hb >= hb:
            last_hb = time.time()
            log(f"{label} 进度：已完成 {total - len(pending)}/{total}，仍在等待 {len(pending)} 份，本批已等{_fmt_sec(time.time() - t_start)}；"
                + ", ".join(f"{v['sp']['name']}={v['st']}" for v in pending.values()))
    return out

def _text(b):
    return b.decode("utf-8-sig", errors="replace") if isinstance(b, (bytes, bytearray)) else str(b)

def _num(v):
    try:
        if v is None or (isinstance(v, str) and not v.strip()):
            return np.nan
        return float(v)
    except Exception:
        return np.nan

def _wavg(v, w):
    m = v.notna() & w.notna() & (w > 0)
    if m.any():
        return float((v[m] * w[m]).sum() / w[m].sum())
    return float(v.mean()) if v.notna().any() else np.nan

def _read_tsv(text):
    if not text.strip():
        return pd.DataFrame()
    x = pd.read_csv(io.StringIO(text), sep="\t", dtype=str, keep_default_na=False)
    x.columns = [str(c).strip().lower() for c in x.columns]
    return x

def _attach_parent(x, L_store):
    """先按 SKU 再按 ASIN 找父ASIN(两种都对不上的行会被统计为未匹配)"""
    x = x.copy()
    x["_p"] = np.nan
    if "sku" in x.columns:
        m = dict(zip(L_store["MSKU"].astype(str).str.strip(), L_store["父ASIN"]))
        x["_p"] = x["sku"].astype(str).str.strip().map(m)
    if "asin" in x.columns:
        m2 = dict(zip(L_store["ASIN"].astype(str).str.strip(), L_store["父ASIN"]))
        x["_p"] = x["_p"].fillna(x["asin"].astype(str).str.strip().map(m2))
    return x

# ---- 各报告解析
def _parse_sales_traffic(obj, fx):
    """salesAndTrafficByAsin(PARENT粒度)。注意：字段在 trafficByAsin/salesByAsin 里；两个百分比是0-100刻度，这里转为小数，
    重复父体时按会话数加权，不对百分比直接求和。"""
    rows = []
    for r in (obj or {}).get("salesAndTrafficByAsin") or []:
        parent = str(r.get("parentAsin") or "").strip()
        if not parent:
            continue
        tr, sa = r.get("trafficByAsin") or {}, r.get("salesByAsin") or {}
        rows.append({"父ASIN": parent, "s": _num(tr.get("sessions")), "pv": _num(tr.get("pageViews")),
                     "bb": _num(tr.get("buyBoxPercentage")), "usp": _num(tr.get("unitSessionPercentage")),
                     "u": _num(sa.get("unitsOrdered")), "amt": _num((sa.get("orderedProductSales") or {}).get("amount"))})
    cols = ["父ASIN", "SP_会话", "SP_页面浏览", "SP_订购件数", "SP_订购销售额", "SP_单位会话率", "SP_BuyBox占比"]
    bd = [x for x in (obj or {}).get("salesAndTrafficByDate") or [] if x.get("date")]
    dates = sorted({str(x.get("date")) for x in bd})
    daily = pd.DataFrame([{"date": str(x["date"]), "units": _num((x.get("salesByDate") or {}).get("unitsOrdered")),
                           "sales": _num(((x.get("salesByDate") or {}).get("orderedProductSales") or {}).get("amount")) * fx} for x in bd],
                         columns=["date", "units", "sales"])
    if not rows:
        return pd.DataFrame(columns=cols), dates, daily
    d = pd.DataFrame(rows)
    out = []
    for p, g in d.groupby("父ASIN"):
        out.append({"父ASIN": p, "SP_会话": g["s"].sum(min_count=1), "SP_页面浏览": g["pv"].sum(min_count=1),
                    "SP_订购件数": g["u"].sum(min_count=1), "SP_订购销售额": g["amt"].sum(min_count=1) * fx,
                    "SP_单位会话率": _wavg(g["usp"], g["s"]) / 100, "SP_BuyBox占比": _wavg(g["bb"], g["s"]) / 100})
    return pd.DataFrame(out)[cols], dates, daily

def _returns_fingerprint(text):
    """退货内容指纹(订单+SKU+退货时间，忽略列差异)：同一账号不同北美站点返回的是同一批退货，用它识别重复"""
    x = _read_tsv(text)
    if x.empty or not {"order-id", "sku"} <= set(x.columns):
        return None
    keys = sorted(f"{a}~{b}~{c}" for a, b, c in zip(x["order-id"], x["sku"], x.get("return-date", [""] * len(x))))
    return hashlib.md5("|".join(keys).encode()).hexdigest()

def _parse_returns(text, L_store):
    x = _read_tsv(text)
    empty = pd.DataFrame(columns=["父ASIN", "SP_退货件数", "SP_退货原因Top3"])
    if x.empty:
        return empty, 0, 0
    x = _attach_parent(x, L_store)
    x["_qty"] = pd.to_numeric(x["quantity"], errors="coerce").fillna(1) if "quantity" in x.columns else 1.0
    bad = int(x["_p"].isna().sum())
    m = x.dropna(subset=["_p"])
    if m.empty:
        return empty, 0, bad
    tot = m.groupby("_p")["_qty"].sum()
    top = {}
    if "reason" in m.columns:
        rs = m.groupby(["_p", "reason"])["_qty"].sum().reset_index().sort_values(["_p", "_qty"], ascending=[True, False])
        for p, g in rs.groupby("_p"):
            top[p] = "; ".join(f"{r}:{int(q_)}" for r, q_ in zip(g["reason"].head(3), g["_qty"].head(3)))
    df = pd.DataFrame({"父ASIN": tot.index, "SP_退货件数": tot.values})
    df["SP_退货原因Top3"] = df["父ASIN"].map(top)
    return df, int(tot.sum()), bad

PLANNING_SKU = {}    # 店铺 -> 库存计划报告的SKU级字段(库龄/亚马逊冗余/建议/建议价/仓储费)，尺码级冗余分析用

def _parse_planning(text, L_store, cfg, fx_default, store=None):
    """GET_FBA_INVENTORY_PLANNING_DATA：库龄、预估仓储费、竞品价。各列名随站点/账号可能不同，缺列则该项留空。"""
    x = _read_tsv(text)
    cols = ["父ASIN", "SP_库龄90天以上件数", "SP_库龄181天以上件数", "SP_预估仓储费", "SP_预估长期仓储费",
            "SP_我方价格", "SP_我方实际售价", "SP_促销中SKU数", "SP_最低促销价", "SP_精选报价价格", "SP_精选报价为我方", "SP_竞品最低价", "SP_价格高于竞品最低价比例"]
    if x.empty:
        return pd.DataFrame(columns=cols), 0
    x = _attach_parent(x, L_store)
    bad = int(x["_p"].isna().sum())
    x = x.dropna(subset=["_p"]).copy()
    if x.empty:
        return pd.DataFrame(columns=cols), bad
    n = lambda c: pd.to_numeric(x[c].astype(str).str.replace(",", ""), errors="coerce") if c in x.columns else pd.Series(np.nan, index=x.index)
    has = lambda c: c in x.columns
    if has("currency"):
        rate = x["currency"].astype(str).str.upper().map(cfg["fx_to_usd"]).fillna(fx_default)
    else:
        rate = pd.Series(fx_default, index=x.index)
    ssum = lambda *s: pd.concat(s, axis=1).sum(axis=1, min_count=1)
    a91 = n("inv-age-91-to-180-days")
    tail = ssum(n("inv-age-366-to-455-days"), n("inv-age-456-plus-days"), n("inv-age-365-plus-days"))   # 365天以上的各种分桶写法
    if has("inv-age-181-to-330-days"):                 # 新口径
        a181 = ssum(n("inv-age-181-to-330-days"), n("inv-age-331-to-365-days"), tail)
    else:                                              # 旧口径
        a181 = ssum(n("inv-age-181-to-270-days"), n("inv-age-271-to-365-days"), tail)
    x["_a181"] = a181
    x["_a91"] = ssum(a91, a181)
    x["_sto"] = n("estimated-storage-cost-next-month") * rate
    x["_lt"] = n("estimated-ltsf-next-charge") * rate
    your, feat, low = n("your-price") * rate, n("featuredoffer-price") * rate, n("lowest-price-new-plus-shipping") * rate
    sale = n("sales-price") * rate
    onsale = (sale > 0) & (your > 0) & (sale < your - 0.005)
    eff = sale.where(onsale, your)                      # 实际售价：有促销价用促销价
    x["_your"] = your.where(your > 0)
    x["_eff"] = eff.where(eff > 0)
    x["_onsale"] = onsale.astype(float)
    x["_sale"] = sale.where(onsale)
    x["_feat"] = feat.where(feat > 0)
    x["_low"] = low.where(low > 0)
    x["_own"] = (((feat - eff).abs() <= 0.01) | ((feat - your).abs() <= 0.01)).astype(float).where(feat > 0)   # 精选报价=我方促销价或标价 都算我方
    x["_gap"] = (eff / low - 1).where((eff > 0) & (low > 0))
    x.loc[x["_gap"].abs() < 0.005, "_gap"] = 0.0
    if store and "sku" in x.columns:
        PLANNING_SKU[store] = pd.DataFrame({
            "MSKU": x["sku"].astype(str), "库龄91天以上": x["_a91"], "库龄181天以上": x["_a181"],
            "亚马逊冗余件数": n("estimated-excess-quantity"), "亚马逊建议": x["recommended-action"] if has("recommended-action") else None,
            "亚马逊建议价": (n("recommended-sales-price") * rate).where(lambda v: v > 0), "预估月仓储费": x["_sto"],
            "标价": x["_your"], "促销价": x["_sale"],
            "库存健康": x["fba-inventory-level-health-status"] if has("fba-inventory-level-health-status") else None})
    s = lambda c: (lambda v: v.sum(min_count=1))
    g = x.groupby("_p").agg(SP_库龄90天以上件数=("_a91", s("")), SP_库龄181天以上件数=("_a181", s("")),
                            SP_预估仓储费=("_sto", s("")), SP_预估长期仓储费=("_lt", s("")),
                            SP_我方价格=("_your", "mean"), SP_我方实际售价=("_eff", "mean"), SP_促销中SKU数=("_onsale", "sum"),
                            SP_最低促销价=("_sale", "min"), SP_精选报价价格=("_feat", "mean"), SP_精选报价为我方=("_own", "mean"),
                            SP_竞品最低价=("_low", "mean"), SP_价格高于竞品最低价比例=("_gap", "mean")).reset_index()
    return g.rename(columns={"_p": "父ASIN"})[cols], bad

def _pick(d, *names):
    if isinstance(d, dict):
        for n in names:
            if d.get(n) is not None:
                return d.get(n)
    return None

def _parse_search_catalog(obj, L_store, fx):
    """品牌分析 Search Catalog Performance：一次请求返回整个品牌目录每个ASIN的搜索漏斗(不需要ASIN清单)。
    按 ASIN->父体 相加；点击率/转化率在 build_parent 里用汇总值相除。"""
    cols = ["父ASIN", "SP_搜索曝光", "SP_搜索点击", "SP_搜索加购", "SP_搜索购买", "SP_搜索销售额", "SP_搜索覆盖ASIN数"]
    rows = []
    for r in (obj or {}).get("dataByAsin") or []:
        im, ck, ca, pu = (r.get(k) or {} for k in ("impressionData", "clickData", "cartAddData", "purchaseData"))
        sales = _pick(pu, "searchTrafficSales") or r.get("searchTrafficSales") or {}
        amt = _num(sales.get("amount")) if isinstance(sales, dict) else _num(sales)
        rows.append({"asin": str(r.get("asin") or "").strip(), "imp": _num(_pick(im, "impressionCount")),
                     "clk": _num(_pick(ck, "clickCount")), "cart": _num(_pick(ca, "cartAddCount", "cartAddsCount")),
                     "buy": _num(_pick(pu, "purchaseCount")), "amt": amt})
    if not rows:
        return pd.DataFrame(columns=cols), 0, 0
    d = pd.DataFrame(rows)
    m = dict(zip(L_store["ASIN"].astype(str).str.strip(), L_store["父ASIN"]))
    d["父ASIN"] = d["asin"].map(m)
    bad = int(d["父ASIN"].isna().sum())
    d = d.dropna(subset=["父ASIN"])
    if d.empty:
        return pd.DataFrame(columns=cols), bad, len(rows)
    sm = lambda v: v.sum(min_count=1)
    g = d.groupby("父ASIN").agg(SP_搜索曝光=("imp", sm), SP_搜索点击=("clk", sm), SP_搜索加购=("cart", sm),
                               SP_搜索购买=("buy", sm), SP_搜索销售额=("amt", sm), SP_搜索覆盖ASIN数=("asin", "nunique")).reset_index()
    g["SP_搜索销售额"] = g["SP_搜索销售额"] * fx
    return g[cols], bad, len(rows)

def _sqp_rows(objs):
    rows = []
    for obj in objs:
        for r in (obj or {}).get("dataByAsin") or []:
            qd, im, ck = r.get("searchQueryData") or {}, r.get("impressionData") or {}, r.get("clickData") or {}
            ca, pu = r.get("cartAddData") or {}, r.get("purchaseData") or {}
            rows.append({"asin": r.get("asin"), "搜索词": qd.get("searchQuery"), "搜索量": _num(qd.get("searchQueryVolume")),
                         "总曝光": _num(im.get("totalQueryImpressionCount")), "我方曝光": _num(im.get("asinImpressionCount")),
                         "总点击": _num(ck.get("totalClickCount")), "我方点击": _num(ck.get("asinClickCount")),
                         "总加购": _num(ca.get("totalCartAddCount")), "我方加购": _num(ca.get("asinCartAddCount")),
                         "总购买": _num(pu.get("totalPurchaseCount")), "我方购买": _num(pu.get("asinPurchaseCount")),
                         "周期起": r.get("startDate"), "周期止": r.get("endDate")})
    return pd.DataFrame(rows)

def _sqp_to_parent(raw, L_store, store):
    """子体ASIN -> 父体：同一搜索词的市场总量对各子体相同(取max)，我方数量跨子体相加，份额=我方/总量。"""
    cols = ["店铺", "父ASIN", "搜索词", "搜索量", "总曝光", "我方曝光", "曝光份额", "总点击", "我方点击", "点击份额",
            "总加购", "我方加购", "加购份额", "总购买", "我方购买", "购买份额", "周期起", "周期止"]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=cols)
    m = dict(zip(L_store["ASIN"].astype(str).str.strip(), L_store["父ASIN"]))
    raw = raw.copy()
    raw["父ASIN"] = raw["asin"].astype(str).str.strip().map(m)
    raw = raw.dropna(subset=["父ASIN", "搜索词"])
    if raw.empty:
        return pd.DataFrame(columns=cols)
    mx, sm = (lambda v: v.max()), (lambda v: v.sum(min_count=1))
    g = raw.groupby(["父ASIN", "搜索词"]).agg(搜索量=("搜索量", "max"), 总曝光=("总曝光", "max"), 我方曝光=("我方曝光", sm),
                                             总点击=("总点击", "max"), 我方点击=("我方点击", sm), 总加购=("总加购", "max"),
                                             我方加购=("我方加购", sm), 总购买=("总购买", "max"), 我方购买=("我方购买", sm),
                                             周期起=("周期起", "first"), 周期止=("周期止", "first")).reset_index()
    for a, b, c in (("我方曝光", "总曝光", "曝光份额"), ("我方点击", "总点击", "点击份额"),
                    ("我方加购", "总加购", "加购份额"), ("我方购买", "总购买", "购买份额")):
        g[c] = g[a] / g[b].where(g[b] > 0)
    g.insert(0, "店铺", store)
    return g[cols]

def _sqp_chunks(L_store, cap, per_parent):
    """每个有销量的父体先取销量最高的 per_parent 个在售子体ASIN(保证每个父体都有样本)，父体按7日销售额排序，总数不超过 cap；
    拼成每块<=200字符(空格分隔)。返回 (块列表, 入选ASIN数, 有销量子体ASIN总数, 覆盖父体数, 有销量父体总数)"""
    s = L_store[(L_store["状态"] == "在售") & (L_store["30日销量"].fillna(0) > 0)].copy()
    if s.empty:
        return [], 0, 0, 0, 0
    s["ASIN"] = s["ASIN"].astype(str).str.strip()
    ps = s.groupby("父ASIN")["7日销售额"].sum().rename("_ps")
    s = s.join(ps, on="父ASIN").sort_values(["_ps", "30日销量"], ascending=False)
    total, ptotal = s["ASIN"].nunique(), s["父ASIN"].nunique()
    s["_rk"] = s.groupby("父ASIN").cumcount()
    s = s[s["_rk"] < int(per_parent)].sort_values(["_rk", "_ps"], ascending=[True, False])   # 先给每个父体的第1名，再第2名...，预算不足时砍的是各父体的尾部
    asins = list(dict.fromkeys(s["ASIN"]))[: int(cap)]
    pcov = s[s["ASIN"].isin(asins)]["父ASIN"].nunique()
    chunks, cur = [], ""
    for a in asins:
        if cur and len(cur) + 1 + len(a) > 200:
            chunks.append(cur); cur = a
        else:
            cur = f"{cur} {a}".strip()
    if cur:
        chunks.append(cur)
    return chunks, len(asins), total, pcov, ptotal

def _hist_have(cfg):
    """库里已有完整(7/7天)数据的 (店铺, 窗口结束)，这些历史窗口不用再请求"""
    try:
        con = sqlite3.connect(rel(cfg["db_path"]))
        d = pd.read_sql('select 店铺, 窗口结束, 覆盖天数 from "weekly_sp_sales"', con)
        con.close()
        d = d[d["覆盖天数"] >= 7]
        return set(zip(d["店铺"], d["窗口结束"]))
    except Exception:
        return set()

def _status_msg(st, payload):
    return payload if isinstance(payload, str) else ""

def _fetch_store(store, code, client, L_store, week_end, cfg, q, deadline=None, label="", acct="", seen_returns=None):
    mp = SP_MARKETPLACE[code][0]
    fx = cfg["fx_to_usd"].get(CUR_BY_CODE.get(code, ""), np.nan)
    start, end = _spapi_range(week_end, cfg.get("spapi_window_lag_days", 0))
    uni = L_store[["店铺", "父ASIN"]].drop_duplicates().copy()
    lag_tag = f"_lag{int(cfg.get('spapi_window_lag_days', 0))}" if cfg.get("spapi_window_lag_days", 0) else ""
    specs = []
    hist_n = max(1, int(cfg.get("spapi_sales_history_weeks", 1)))
    hist_win = {}          # 名称 -> (窗口起, 窗口止)
    if cfg.get("spapi_fetch_sales_traffic", True):
        specs.append({"name": "st", "type": "GET_SALES_AND_TRAFFIC_REPORT", "mp": mp, "start": start, "end": end,
                      "options": {"dateGranularity": "DAY", "asinGranularity": "PARENT"},
                      "cache": _cache_path(cfg, store, "SALES_TRAFFIC" + lag_tag, week_end)})
        hist_win["st"] = (start[:10], end[:10])
        have_h = _hist_have(cfg)
        for k_ in range(1, hist_n):        # 往前再拉 k 个整7天窗口(与当前窗口首尾相接，同为周日~周六)
            s_k = (datetime.strptime(start[:10], "%Y-%m-%d") - timedelta(days=7 * k_)).strftime("%Y-%m-%d")
            e_k = (datetime.strptime(end[:10], "%Y-%m-%d") - timedelta(days=7 * k_)).strftime("%Y-%m-%d")
            if (store, e_k) in have_h:      # 库里已经有这个窗口的完整数据(以前每周都跑过)，不再重复请求
                continue
            nm = f"sth{k_}"
            specs.append({"name": nm, "soft": True, "type": "GET_SALES_AND_TRAFFIC_REPORT", "mp": mp, "start": s_k + "T00:00:00Z", "end": e_k + "T23:59:59Z",
                          "options": {"dateGranularity": "DAY", "asinGranularity": "PARENT"},
                          "cache": _cache_path(cfg, store, f"SALES_TRAFFIC_W{e_k}", week_end)})
            hist_win[nm] = (s_k, e_k)
    if cfg.get("spapi_fetch_returns", True):
        specs.append({"name": "ret", "type": "GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA", "mp": mp, "start": start, "end": end,
                      "options": {}, "cache": _cache_path(cfg, store, "RETURNS", week_end)})
    plan_on = cfg.get("spapi_fetch_planning", cfg.get("spapi_fetch_aged_inventory", True))
    if plan_on:
        specs.append({"name": "plan", "type": "GET_FBA_INVENTORY_PLANNING_DATA", "mp": mp, "options": {},
                      "cache": _cache_path(cfg, store, "INV_PLANNING", week_end)})
    sc_start = sc_end = None
    if cfg.get("spapi_fetch_search_catalog", True) and code in set(cfg.get("spapi_search_catalog_countries") or ["US", "UK"]):
        sc_start, sc_end, sat0 = _sqp_week(week_end)
        gap0 = (datetime.now(timezone.utc).date() - sat0).days
        if gap0 < int(cfg.get("spapi_sqp_lag_days", 2)):
            q.add("INFO", f"SP-API搜索漏斗:{store}", f"周期{sc_start[:10]}~{sc_end[:10]}刚结束{gap0}天，亚马逊通常还没发布，本次跳过；过两天再跑")
            sc_start = None
        else:
            specs.append({"name": "sc", "type": "GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT", "mp": mp,
                          "start": sc_start, "end": sc_end, "options": {"reportPeriod": "WEEK"},
                          "cache": _cache_path(cfg, store, "SEARCH_CATALOG_" + sc_start[:10], week_end)})
    sqp_chunks, n_asin, n_total, p_cov, p_tot, sqp_start, sqp_end = [], 0, 0, 0, 0, None, None
    if cfg.get("spapi_fetch_sqp", False) and code in set(cfg.get("spapi_sqp_countries") or ["US"]):
        sqp_start, sqp_end, sat = _sqp_week(week_end)
        gap = (datetime.now(timezone.utc).date() - sat).days
        if gap < int(cfg.get("spapi_sqp_lag_days", 2)):
            q.add("INFO", f"SP-API SQP:{store}", f"SQP周期{sqp_start[:10]}~{sqp_end[:10]}刚结束{gap}天，亚马逊通常还没发布，本次跳过；过两天再跑(缓存不会记录跳过)")
        else:
            sqp_chunks, n_asin, n_total, p_cov, p_tot = _sqp_chunks(L_store, cfg.get("spapi_sqp_max_asins", 54),
                                                                    cfg.get("spapi_sqp_asins_per_parent", 3))
        for i, ch in enumerate(sqp_chunks):
            tag = "SQP_" + hashlib.md5((ch + sqp_start).encode()).hexdigest()[:10]
            specs.append({"name": f"sqp{i}", "type": "GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT", "mp": mp,
                          "start": sqp_start, "end": sqp_end, "options": {"reportPeriod": "WEEK", "asin": ch},
                          "cache": _cache_path(cfg, store, tag, week_end)})
    specs.sort(key=lambda z: 1 if z.get("soft") else 0)      # 主报告先创建，历史窗口(低优先级)排后
    log(f"{label} 需要 {len(specs)} 份报告：" + ", ".join(sp["name"] for sp in specs))
    res = _run_reports(client, specs, cfg, label, deadline)
    out = uni.copy()

    def ok(name):
        return res.get(name, ("ERROR", "未请求"))[0] in ("DONE", "CACHE")

    def fail(label, name):
        st, msg = res.get(name, ("ERROR", "未请求"))
        text = _status_msg(st, msg)[:300]
        hint = f" | {_PERM_HINT}" if any(k in text for k in ("Unauthorized", "403", "Forbidden", "AccessDenied", "denied")) else ""
        soft = "历史" in label or st == "CANCELLED"          # 历史窗口限流/退货报告被取消(多为无数据)：提示即可，不算告警
        if "历史" in label and ("Quota" in text or "Throttl" in text):
            text += "(限流，本次跳过，下次运行会自动补拉)"
        q.add("INFO" if soft else "WARN", f"SP-API{label}:{store}", f"{st}: {text}{hint}")

    hist_rows = []
    for nm_, (s_k, e_k) in hist_win.items():
        if nm_ == "st" or nm_ not in res:
            continue
        if ok(nm_):
            try:
                dfh, dts, _ = _parse_sales_traffic(json.loads(_text(res[nm_][1])), fx)
                got_h = len([d_ for d_ in dts if s_k <= d_ <= e_k])
                dfh = dfh.rename(columns={"SP_会话": "会话", "SP_页面浏览": "页面浏览", "SP_订购件数": "订购件数", "SP_订购销售额": "订购销售额",
                                          "SP_单位会话率": "单位会话率", "SP_BuyBox占比": "BuyBox占比"})
                dfh["店铺"], dfh["窗口起"], dfh["窗口结束"], dfh["覆盖天数"] = store, s_k, e_k, got_h
                hist_rows.append(dfh)
                if got_h < 7:
                    q.add("WARN", f"SP-API销售流量(历史):{store}", f"窗口{s_k}~{e_k}只有 {got_h}/7 天有数据，已按实际天数记录")
                    try:
                        os.remove([sp_ for sp_ in specs if sp_["name"] == nm_][0]["cache"])
                    except OSError:
                        pass
            except Exception as e:
                q.add("WARN", f"SP-API销售流量(历史):{store}", f"{s_k}~{e_k} 解析失败：{e}")
        else:
            fail("销售流量(历史)", nm_)
    if "st" in res:
        if ok("st"):
            try:
                df, dates, daily = _parse_sales_traffic(json.loads(_text(res["st"][1])), fx)
                SP_DAILY[store] = daily
                _cur = df.rename(columns={"SP_会话": "会话", "SP_页面浏览": "页面浏览", "SP_订购件数": "订购件数", "SP_订购销售额": "订购销售额",
                                          "SP_单位会话率": "单位会话率", "SP_BuyBox占比": "BuyBox占比"}).copy()
                _cur["店铺"], _cur["窗口起"], _cur["窗口结束"] = store, start[:10], end[:10]
                _cur["覆盖天数"] = len([d_ for d_ in dates if start[:10] <= d_ <= end[:10]])
                hist_rows.append(_cur)
                exp_days = (datetime.strptime(end[:10], "%Y-%m-%d") - datetime.strptime(start[:10], "%Y-%m-%d")).days + 1
                in_win = [d_ for d_ in dates if start[:10] <= d_ <= end[:10]]
                got = len(in_win)
                df["SP_流量覆盖天数"] = got
                df["SP_流量截止日"] = in_win[-1] if in_win else None
                out = out.merge(df, on="父ASIN", how="left")
                msg = f"{len(df)} 个父体有流量数据；窗口{start[:10]}~{end[:10]}内有数据的天数 {got}/{exp_days}(截止{in_win[-1] if in_win else '无'})"
                if got < exp_days:
                    msg += (f"。最近{exp_days - got}天亚马逊尚未出数：SP_会话/页面浏览/订购件数/订购销售额只是{got}天合计，与7日数据比较前要折算日均"
                            "(SP_退货率、广告点击占会话已自动折算)；该报告不完整，不写缓存，下次运行会重新拉取。想要完整7天：过2~3天再跑，或设 spapi_window_lag_days=2")
                    for sp_ in specs:
                        if sp_["name"] == "st":
                            try:
                                os.remove(sp_["cache"])
                            except OSError:
                                pass
                q.add("OK" if (len(df) and got >= exp_days) else "WARN", f"SP-API销售流量:{store}", msg)
            except Exception as e:
                q.add("WARN", f"SP-API销售流量:{store}", f"解析失败：{e}")
        else:
            fail("销售流量", "st")
    if "ret" in res:
        if ok("ret"):
            try:
                rh = _returns_fingerprint(_text(res["ret"][1]))
                prev = (seen_returns or {}).get((acct, rh)) if rh else None
                if prev:
                    q.add("WARN", f"SP-API退货:{store}", f"与 {prev} 的退货报告内容完全相同(亚马逊按账号返回北美各站点合并的退货)，为避免重复计入，本站点退货列留空")
                    raise StopIteration
                if seen_returns is not None and rh:
                    seen_returns[(acct, rh)] = store
                df, units, bad = _parse_returns(_text(res["ret"][1]), L_store)
                out = out.merge(df, on="父ASIN", how="left")
                out["SP_退货件数"] = out["SP_退货件数"].fillna(0)        # 报告成功=覆盖全店，没出现的父体退货为0
                q.add("OK", f"SP-API退货:{store}", f"{len(df)} 个父体有退货，共{int(units)}件；{bad}行无法匹配到父体。"
                      "注意：退货在亚马逊收货处理后才入报告，单周波动大，宜看4周累计")
            except StopIteration:
                pass
            except Exception as e:
                q.add("WARN", f"SP-API退货:{store}", f"解析失败：{e}")
        else:
            fail("退货", "ret")
    if "plan" in res:
        if ok("plan"):
            try:
                df, bad = _parse_planning(_text(res["plan"][1]), L_store, cfg, fx, store=store)
                out = out.merge(df, on="父ASIN", how="left")
                q.add("OK", f"SP-API库存计划:{store}",
                      f"{len(df)} 个父体有库龄/仓储/价格数据；{bad}行无法匹配到父体。此报告是拉取当时的快照，补跑历史周得到的不是当时的值")
            except Exception as e:
                q.add("WARN", f"SP-API库存计划:{store}", f"解析失败：{e}")
        else:
            fail("库存计划", "plan")
    if "sc" in res:
        if ok("sc"):
            try:
                g, bad, n_rows = _parse_search_catalog(json.loads(_text(res["sc"][1])), L_store, fx)
                g["SP_搜索周期"] = f"{sc_start[:10]}~{sc_end[:10]}"
                out = out.merge(g, on="父ASIN", how="left")
                warn_fields = len(g) and (g["SP_搜索加购"].isna().all() or g["SP_搜索购买"].isna().all())
                q.add("WARN" if (not len(g) or warn_fields) else "OK", f"SP-API搜索漏斗:{store}",
                      f"周期{sc_start[:10]}~{sc_end[:10]}(周日~周六)；报告{n_rows}个ASIN，{len(g)}个父体有数据，{bad}个ASIN归不到父体。"
                      + ("加购/购买字段没有识别到，请把该报告样例发我核对字段名。" if warn_fields else "")
                      + "口径：父体在搜索结果页的整体漏斗，不能拆到具体搜索词")
            except Exception as e:
                q.add("WARN", f"SP-API搜索漏斗:{store}", f"解析失败：{e}")
        else:
            fail("搜索漏斗", "sc")
    sqp_df = pd.DataFrame()
    if sqp_chunks:
        objs, bad_chunks = [], 0
        for i in range(len(sqp_chunks)):
            if ok(f"sqp{i}"):
                try:
                    objs.append(json.loads(_text(res[f"sqp{i}"][1])))
                except Exception as e:
                    bad_chunks += 1
                    q.add("WARN", f"SP-API SQP:{store}", f"第{i + 1}块解析失败：{e}")
            else:
                bad_chunks += 1
                if bad_chunks == 1:
                    fail(" SQP", f"sqp{i}")
        raw = _sqp_rows(objs)
        sqp_df = _sqp_to_parent(raw, L_store, store)
        if len(sqp_df):
            roll = sqp_df.groupby("父ASIN").agg(SP_SQP曝光=("我方曝光", lambda v: v.sum(min_count=1)),
                                                SP_SQP点击=("我方点击", lambda v: v.sum(min_count=1)),
                                                SP_SQP加购=("我方加购", lambda v: v.sum(min_count=1)),
                                                SP_SQP购买=("我方购买", lambda v: v.sum(min_count=1)),
                                                SP_SQP查询数=("搜索词", "nunique")).reset_index()
            amap = dict(zip(L_store["ASIN"].astype(str).str.strip(), L_store["父ASIN"]))
            ra = raw.assign(父ASIN=raw["asin"].astype(str).str.strip().map(amap)).dropna(subset=["父ASIN"])
            roll = roll.merge(ra.groupby("父ASIN")["asin"].nunique().rename("SP_SQP覆盖ASIN数").reset_index(), on="父ASIN", how="left")
            roll["SP_SQP周期"] = f"{sqp_start[:10]}~{sqp_end[:10]}"
            out = out.merge(roll, on="父ASIN", how="left")
        q.add("OK" if len(sqp_df) and not bad_chunks else "WARN", f"SP-API SQP:{store}",
              f"周期{sqp_start[:10]}~{sqp_end[:10]}(周日~周六，与广告周不一定对齐)；请求{len(sqp_chunks)}块，覆盖{p_cov}/{p_tot}个有销量父体、{n_asin}/{n_total}个有销量子体ASIN(每父体最多取销量前{cfg.get('spapi_sqp_asins_per_parent', 6)}个子体)，"
              f"失败{bad_chunks}块；得到{len(sqp_df)}条父体×搜索词。每个ASIN最多返回Top100查询，汇总值只是部分口径")
    hist_df = pd.concat(hist_rows, ignore_index=True) if hist_rows else pd.DataFrame()
    return out, sqp_df, hist_df

def fetch_spapi_parent_weekly(L, week_end, cfg, q):   # 返回 (父体SP数据, SQP, 周度销量历史)
    """返回 (父体级SP数据 DataFrame|None, SQP父体×搜索词 DataFrame|None)。每个店铺(卖家账号×站点)独立拉取。"""
    cache_only = bool(cfg.get("spapi_cache_only"))
    if not SPAPI_IMPORT_OK and not cache_only:
        q.add("WARN", "SP-API库", "未安装 python-amazon-sp-api(pip install python-amazon-sp-api)：本次跳过在线拉取")
        return None, None, None
    SP_DAILY.clear()
    stores = sorted(set(str(x).strip() for x in L["店铺"].dropna().unique()))
    if cache_only:
        q.add("INFO", "SP-API仅缓存模式", f"只读取 {cfg.get('spapi_cache_dir')} 里已有的报告，不请求亚马逊；没有缓存的报告按缺失处理")
        accounts = {s_: (None, None, "cache") for s_ in stores}
    else:
        accounts = _resolve_accounts(cfg, stores, q)
    if not accounts:
        return None, None, None
    frames, sqps, hists, clients = [], [], [], {}
    todo = sorted([x for x in stores if x in accounts], key=lambda x: (0 if code_of_store(x) == "US" else 1, x))   # 美国优先，退货报告归美国
    seen_returns = {}
    deadline = time.time() + int(cfg.get("spapi_total_timeout_seconds", 2400))
    log(f"SP-API 开始：{len(todo)} 个店铺-站点 {todo}；总时限{_fmt_sec(cfg.get('spapi_total_timeout_seconds', 2400))}，"
        f"单份报告最长等待{_fmt_sec(cfg.get('spapi_report_timeout_seconds', 600))}")
    for i, store in enumerate(todo, 1):
        label = f"店铺{i}/{len(todo)} {store}"
        code = code_of_store(store)
        if code not in SP_MARKETPLACE:
            q.add("INFO", f"SP-API站点:{store}", f"暂不支持站点 {code}")
            continue
        if code in {str(c).upper() for c in (cfg.get("spapi_skip_countries") or [])}:
            q.add("INFO", f"SP-API站点:{store}", "按 config.spapi_skip_countries 跳过(不请求SP-API)")
            continue
        if time.time() > deadline:
            q.add("WARN", "SP-API总时限", f"已超过 spapi_total_timeout_seconds，跳过剩余店铺：{todo[i - 1:]}；下次运行会续用已提交的报告")
            break
        creds, refresh, fname = accounts[store]
        key = (fname, code)
        t0 = time.time()
        log(f"{label} 开始")
        try:
            if key not in clients:
                clients[key] = None if cache_only else _build_reports_client(creds, refresh, code)
            base = L[L["店铺"] == store].copy()
            if base.empty:
                continue
            f, s_, h_ = _fetch_store(store, code, clients[key], base, week_end, cfg, q, deadline, label, fname, seen_returns)
            frames.append(f)
            if s_ is not None and len(s_):
                sqps.append(s_)
            if h_ is not None and len(h_):
                hists.append(h_)
        except Exception as e:
            q.add("WARN", f"SP-API店铺:{store}", f"整店拉取失败：{e}")
        log(f"{label} 结束，用时{_fmt_sec(time.time() - t0)}")
    log("SP-API 阶段结束")
    if not frames:
        return None, None, None
    return (pd.concat(frames, ignore_index=True), (pd.concat(sqps, ignore_index=True) if sqps else None),
            (pd.concat(hists, ignore_index=True) if hists else None))

def merge_spapi_manual_over_auto(auto_df, manual_df):
    keys = ["店铺", "父ASIN"]
    out = pd.concat([auto_df[keys], manual_df[keys]], ignore_index=True).drop_duplicates()
    out = out.merge(auto_df, on=keys, how="left")
    m = manual_df.drop_duplicates(keys).set_index(keys)
    idx = pd.MultiIndex.from_frame(out[keys])
    for c in SP_COLS:
        if c in manual_df.columns:
            vals = pd.Series(m[c]).reindex(idx).to_numpy()
            mask = pd.notna(vals)
            if c not in out:
                out[c] = np.nan
            out[c] = out[c].astype(object)
            out.loc[mask, c] = vals[mask]
            try:
                out[c] = pd.to_numeric(out[c])
            except Exception:
                pass
    return out

# --------------------------------------------------------------------------- ingest
SHIP_COLS = ["货件单号", "店铺", "国家", "货件状态", "运输方式", "SHIPPED状态变更时间", "RECEIVING状态变更时间", "送达时段(开始时间)", "送达时段(结束时间)",
             "物流中心编码", "发货单", "MSKU", "父ASIN", "已发货", "签收量", "申报量"]

FREIGHT_GAP = {}    # 头程缺失/估算的SKU清单，ingest 时导出 csv
SHIP_DETAIL = {}     # ship_summary 产出的在途货件明细(店铺×父体×货件)，供落库/数据包使用
LEAD_DAYS = {}       # supply_summary 模拟用的 (海运, 空运, 备货, 上架) 天数，供 新采购最晚下单 使用(与模拟同一套天数)

def read_shipments(path, cfg, q, week_end):
    """领星"FBA货件"导出(货件详情)：货件表头字段只在每个货件的第一行，其余行为空，需要向下填充。
    口径(已与Listing 在途 逐父体核对)：Listing的 计划入库+标发在途+入库中 = SHIPPED/IN_TRANSIT 的已发货 + RECEIVING 的(已发货-签收量，逐SKU取>=0)。"""
    try:
        d = pd.read_excel(path, sheet_name="货件详情")
    except Exception as e:
        q.add("WARN", "FBA货件", f"读取'货件详情'失败：{e}")
        return None
    miss = [c for c in SHIP_COLS if c not in d.columns]
    if miss:
        q.add("WARN", "FBA货件", f"缺少必要列 {miss}，本次不使用货件文件")
        return None
    d = d[SHIP_COLS + (["签收明细"] if "签收明细" in d.columns else [])].copy()
    hdr = [c for c in SHIP_COLS if c not in ("MSKU", "父ASIN", "已发货", "签收量", "申报量")]
    d[hdr] = d[hdr].ffill()
    for c in ("已发货", "签收量", "申报量"):
        d[c] = num(d[c]).fillna(0)
    ex = {str(c).upper() for c in (cfg.get("exclude_countries") or [])}
    if ex:
        d = d[~d["店铺"].map(code_of_store).isin(ex)].copy()
    m = re.search(r"_(20\d{6})_", os.path.basename(path))
    snap = datetime.strptime(m.group(1), "%Y%m%d").date() if m else datetime.now().date()
    dt = lambda c: pd.to_datetime(d[c].replace("-", np.nan), errors="coerce")
    d["发货日"] = dt("SHIPPED状态变更时间").dt.date
    d["入库开始日"] = dt("RECEIVING状态变更时间").dt.date
    d["窗口起"] = pd.to_datetime(d["送达时段(开始时间)"], errors="coerce").dt.date
    d["窗口止"] = pd.to_datetime(d["送达时段(结束时间)"], errors="coerce").dt.date
    st = d["货件状态"].astype(str).str.upper()
    d["待收"] = 0.0
    live_ship = st.isin(["SHIPPED", "IN_TRANSIT"])
    d.loc[live_ship, "待收"] = d.loc[live_ship, "已发货"]
    rcv = st == "RECEIVING"
    d.loc[rcv, "待收"] = (d.loc[rcv, "已发货"] - d.loc[rcv, "签收量"]).clip(lower=0)
    wk = st == "WORKING"
    d["计划"] = np.where(wk, d["申报量"], 0.0)
    d["状态"] = st
    # ---- 已签收待上架：亚马逊已签收(签收量已扣出'在途')但还没进可售——Listing/库存报告里既不在可售也不在在途，会"消失"几天。
    # 按签收明细取最近 signed_pending_days 天的净签收件数(含更正的负数)，视为 签收日+上架天数 可售
    pend_days = int(cfg.get("signed_pending_days", 7))
    d["待上架"], d["签收日"] = 0.0, None
    if pend_days > 0 and "签收明细" in d.columns:
        lo = snap - timedelta(days=pend_days)
        pq, pdt = [], []
        for txt, stt, sq in zip(d["签收明细"], st, d["签收量"]):
            ent = [(datetime.strptime(a_, "%Y-%m-%d").date(), float(b_)) for a_, b_ in re.findall(r"(\d{4}-\d{2}-\d{2})\s*\|\s*(-?\d+(?:\.\d+)?)", str(txt))] \
                if stt in ("RECEIVING", "CLOSED") else []
            rec = [(a_, b_) for a_, b_ in ent if lo < a_ <= snap]
            net = min(max(sum(b_ for _, b_ in rec), 0.0), max(sq, 0.0))
            pq.append(net)
            pdt.append(max((a_ for a_, b_ in rec if b_ > 0), default=None) if net > 0 else None)
        d["待上架"], d["签收日"] = pq, pdt
        if d["待上架"].sum() > 0:
            g_ = d[d["待上架"] > 0].groupby(["店铺", "货件单号"]).agg(件=("待上架", "sum"), 日=("签收日", "max")).reset_index()
            q.add("INFO", "FBA已签收待上架", f"最近{pend_days}天亚马逊已签收、但通常还没进可售的共 {int(g_['件'].sum())} 件：" +
                  "；".join(f"{a_}/{b_} {int(c_)}件(签收{e_})" for a_, b_, c_, e_ in zip(g_["店铺"].head(6), g_["货件单号"].head(6), g_["件"].head(6), g_["日"].head(6))) +
                  f"。这部分不在Listing可售/在途里，断货模拟按 签收日+上架天数 计入供给(列 货件_已签收待上架件数)；若其中一部分已进可售，会重复计算，偏乐观")
    n_sh = d.loc[d["状态"] != "CANCELLED", "货件单号"].nunique()
    # ---- 诊断：超期未到 / 入库滞留 / 少收 / 历史运输时长
    late = d[live_ship & d["窗口止"].notna() & (d["窗口止"] < snap) & (d["待收"] > 0)]
    if len(late):
        q.add("WARN", "FBA货件超期未到", f"{late['货件单号'].nunique()} 个货件送达时段已过仍未开始入库：{late['货件单号'].unique()[:6].tolist()}(共{int(late['待收'].sum())}件)；可能是送达时段没更新或货代延误")
    stuck_days = int(cfg.get("shipment_stuck_days", 14))
    stuck = d[rcv & (d["待收"] > 0) & d["入库开始日"].notna()]
    stuck = stuck[[(snap - x).days >= stuck_days for x in stuck["入库开始日"]]]
    if len(stuck):
        g_ = stuck.groupby("货件单号").agg(店铺=("店铺", "first"), 待收=("待收", "sum"), 入库开始=("入库开始日", "first"))
        q.add("WARN", "FBA入库滞留", f"{len(g_)} 个货件已开始入库超过{stuck_days}天仍有未签收：" +
              "；".join(f"{k}({r_['店铺']}，待收{int(r_['待收'])}件，自{r_['入库开始']})" for k, r_ in g_.head(5).iterrows()) +
              "。超过亚马逊常规入库时长时，可核对货件差异/开Case")
    closed = d[(d["状态"] == "CLOSED") & ((d["已发货"] - d["签收量"]) > 0)]
    if len(closed):
        cl = closed.assign(少收=closed["已发货"] - closed["签收量"]).groupby("货件单号")["少收"].sum()
        q.add("INFO", "FBA少收(已关闭货件)", f"{len(cl)} 个已关闭货件共少收 {int(cl.sum())} 件(已发货>签收)：{cl.sort_values(ascending=False).head(4).to_dict()}；可评估是否向亚马逊申请赔偿")
    done = d[d["入库开始日"].notna() & d["发货日"].notna() & (d["状态"].isin(["RECEIVING", "CLOSED"]))].drop_duplicates("货件单号")
    if len(done):
        lt = pd.Series([(a - b).days for a, b in zip(done["入库开始日"], done["发货日"])], index=done.index)
        by = lt.groupby(done["运输方式"].fillna("未知")).agg(["median", "min", "max", "count"])
        lt_days = {k: float(r_["median"]) for k, r_ in by.iterrows()}
        q.add("INFO", "FBA历史运输时长(发货→开始入库)", "；".join(f"{k}：中位{int(r_['median'])}天(最短{int(r_['min'])}/最长{int(r_['max'])}，{int(r_['count'])}票)" for k, r_ in by.iterrows()) +
              "。可用来判断'送达时段'是否合理")
        mxd = lt.groupby(done["运输方式"].fillna("未知")).max().to_dict()
        liv = d[live_ship & (d["待收"] > 0) & d["发货日"].notna()].drop_duplicates("货件单号")
        over = [(r_["货件单号"], r_["店铺"], r_["运输方式"], (snap - r_["发货日"]).days, mxd[r_["运输方式"]], int(d.loc[d["货件单号"] == r_["货件单号"], "待收"].sum()))
                for _, r_ in liv.iterrows() if mxd.get(r_["运输方式"]) is not None and (snap - r_["发货日"]).days > mxd[r_["运输方式"]]]
        if over:
            q.add("WARN", "FBA货件在途超过历史最长", "；".join(f"{a_}({b_}，{c_}，已发货{d_}天>历史最长{int(e_)}天，待收{f_}件)" for a_, b_, c_, d_, e_, f_ in over[:5]) +
                  "。可能货代延误/状态没更新/货件异常，这部分件数的到仓日不可信，建议先向货代确认；模拟断货时这批货的到仓日只是登记值")
    lt_days = locals().get("lt_days", {})
    q.add("INFO", "FBA货件", f"{n_sh} 个有效货件(不含取消)，快照日{snap}；状态行数{d['状态'].value_counts().to_dict()}。送达时段是卖家中心登记值：未获货代确认的可能只是系统预估，不等于真实到仓日")
    # 每个SKU第一次在FBA签收(或开始入库)的日期：判断'刚到货的新SKU'不算滞销
    first = {}
    for st_, m_, txt, rs in zip(d["店铺"], d["MSKU"], d["签收明细"] if "签收明细" in d.columns else [None] * len(d), d["入库开始日"]):
        ds = [datetime.strptime(a_, "%Y-%m-%d").date() for a_, b_ in re.findall(r"(\d{4}-\d{2}-\d{2})\s*\|\s*(-?\d+)", str(txt)) if float(b_) > 0]
        if rs is not None and not pd.isna(rs):
            ds.append(rs)
        if ds:
            k_ = (st_, str(m_))
            first[k_] = min([first[k_]] + ds) if k_ in first else min(ds)
    return {"use": True, "rows": d, "snap": snap, "lt": lt_days, "first_recv_sku": first}


REPLEN_PO = {}       # read_replenish 解析出的待交付采购单(PO)明细，供落库/数据包使用
REPLEN_COLS = ["父ASIN汇总行", "欧洲/北美汇总行", "父ASIN", "MSKU", "店铺", "可售", "入库中", "FBA在途", "本地可用", "待检待上架量", "待交付", "待交付详情",
               "本地仓在途", "采购计划", "海外仓可用", "海外仓在途", "7天日均", "30天日均", "7天销量", "14天销量", "30天销量", "60天销量", "断货时间", "建议采购日", "建议本地发货日",
               "建议采购量", "建议采购量-海派", "建议采购量-空派", "本地发FBA量", "本地发FBA量-海派", "本地发FBA量-空派", "采购交期", "备货时长"]
POOL_COLS = ["本地可用", "待检待上架量", "待交付", "本地仓在途", "采购计划", "海外仓可用", "海外仓在途"]

def _parse_po(t):
    out = []
    if isinstance(t, str):
        for ln in t.split("\n")[1:]:
            p_ = [x.strip() for x in ln.split("|")]
            if len(p_) >= 7:
                out.append(p_[:7])
    return out

def read_replenish(path, cfg, q, week_end):
    """领星"补货建议(父ASIN视图)"。三类行：父ASIN汇总行(店铺×父体，用来取本地可用/领星建议等)、欧洲/北美汇总行(忽略)、
    明细行(店铺×MSKU，用来取待交付件数与PO明细)。
    注意：(1)明细行里同一(店铺,MSKU)会重复3~4次，完全相同，必须去重；
    (2)本地可用/待交付是同一账号各站点(US/UK/CA…)共享的同一批货，在每个站点行里都会显示一遍，只计入主站(US优先)，其它站点置0；
    (3)「待交付」不能用父ASIN汇总行：该行会把无Listing的SKU采购也加进去，虚高。正确口径=SKU明细去重后「待交付」合计(与可解析PO件数一致)。"""
    try:
        head = list(pd.read_excel(path, nrows=0).columns)
    except Exception as e:
        q.add("WARN", "补货建议", f"读取失败：{e}")
        return None
    need = ["父ASIN汇总行", "欧洲/北美汇总行", "父ASIN", "MSKU", "店铺", "本地可用", "待交付"]
    miss = [c for c in need if c not in head]
    if miss:
        q.add("WARN", "补货建议", f"缺少必要列 {miss}，本次不使用补货建议")
        return None
    d = pd.read_excel(path, usecols=[c for c in REPLEN_COLS if c in head])
    m_ = re.search(r"_(20\d{6})_", os.path.basename(path))
    snap = datetime.strptime(m_.group(1), "%Y%m%d").date() if m_ else datetime.now().date()
    d["店铺"] = d["店铺"].astype(str)
    single = ~d["店铺"].str.contains("、")
    ex = {str(c).upper() for c in (cfg.get("exclude_countries") or [])}
    if ex:
        single &= ~d["店铺"].map(code_of_store).isin(ex)
    is_par, is_reg = d["父ASIN汇总行"] == "是", d["欧洲/北美汇总行"] == "是"
    for c in POOL_COLS + ["可售", "入库中", "FBA在途", "7天日均", "30天日均", "7天销量", "14天销量", "30天销量", "60天销量", "建议采购量", "建议采购量-海派", "建议采购量-空派", "本地发FBA量", "本地发FBA量-海派", "本地发FBA量-空派", "采购交期", "备货时长"]:
        if c in d.columns:
            d[c] = num(d[c])
    summ = d[is_par & single].copy()
    det_all = d[~is_par & ~is_reg & single].copy()
    det = det_all.drop_duplicates(["店铺", "MSKU"], keep="first").copy()
    n_dup = len(det_all) - len(det)
    prio = lambda s_: {"US": 0, "UK": 1}.get(code_of_store(s_), 2)
    acct = lambda s_: _account_of(s_)
    # ---- 共享池归属：同账号同款，只计入主站(US优先)
    summ["acct"] = summ["店铺"].map(acct)
    summ["款"] = summ["MSKU"].astype(str).str.split("、").str[0].map(style_of)
    summ["_p"] = summ["店铺"].map(prio)
    prim = summ.sort_values("_p").drop_duplicates(["acct", "款"])[["acct", "款", "店铺"]].rename(columns={"店铺": "主站"})
    summ = summ.merge(prim, on=["acct", "款"], how="left")
    summ["补货_共享池已计入主站"] = summ["店铺"] != summ["主站"]
    n_shared = int(summ["补货_共享池已计入主站"].sum())
    # ---- 待交付：用SKU明细去重后按(店铺,父ASIN)合计，覆盖父体汇总行(父汇总常含无Listing SKU而虚高)
    det_for_td = det_all.drop_duplicates(["店铺", "MSKU"], keep="first")
    if "待交付" in det_for_td.columns and len(det_for_td):
        td = det_for_td.groupby(["店铺", "父ASIN"], as_index=False)["待交付"].sum()
        td = td.rename(columns={"待交付": "待交付_明细合计"})
        summ = summ.merge(td, on=["店铺", "父ASIN"], how="left")
        if "待交付" in summ.columns:
            before = summ["待交付"].fillna(0)
            after = summ["待交付_明细合计"].fillna(0)
            n_fix = int((before.round(0) != after.round(0)).sum())
            if n_fix:
                samples = summ.loc[before.round(0) != after.round(0), ["店铺", "款", "待交付", "待交付_明细合计"]].head(5)
                q.add("INFO", "待交付改用明细合计",
                      f"{n_fix}个父体的父ASIN汇总行待交付与SKU明细去重合计不一致，已改用明细合计(正确口径)。样例：" +
                      "；".join(f"{a}/{b} 汇总{int(c)}→明细{int(d)}"
                               for a, b, c, d in zip(samples["店铺"], samples["款"], samples["待交付"].fillna(0), samples["待交付_明细合计"].fillna(0))))
            summ["待交付"] = summ["待交付_明细合计"].fillna(0)
        summ = summ.drop(columns=["待交付_明细合计"], errors="ignore")
    for c in POOL_COLS:
        if c in summ.columns:
            summ.loc[summ["补货_共享池已计入主站"], c] = 0.0
    # ---- SKU 明细(尺码级模拟用)：各站点自己的可售/销量；本地可用是同账号共享池，按(账号,MSKU)记一份
    sk = det.copy()
    sk["acct"] = sk["店铺"].map(acct)
    for c in ("可售", "本地可用", "7天销量", "14天销量", "30天销量", "60天销量"):
        if c not in sk.columns:
            sk[c] = np.nan
    sk["本地可用_池"] = sk.groupby(["acct", "MSKU"])["本地可用"].transform("max").fillna(0)
    sk["_pr"] = sk["店铺"].map(prio)
    sk["池主站行"] = ~sk.sort_values("_pr").duplicated(["acct", "MSKU"]).reindex(sk.index)   # 共享池只在主站行计一次(冗余/滞销用)
    skus = sk[["店铺", "父ASIN", "MSKU", "acct", "池主站行", "可售", "本地可用_池", "7天销量", "14天销量", "30天销量", "60天销量"]].copy()
    # ---- PO 明细：只取主站的明细行
    det["acct"] = det["店铺"].map(acct)
    det["款"] = det["MSKU"].astype(str).map(style_of)
    det["_p"] = det["店铺"].map(prio)
    prim_d = det.sort_values("_p").drop_duplicates(["acct", "款"])[["acct", "款", "店铺"]].rename(columns={"店铺": "主站"})
    det = det.merge(prim_d, on=["acct", "款"], how="left")
    det = det[(det["店铺"] == det["主站"]) & (det["待交付"].fillna(0) > 0)]
    rows = []
    for _, r_ in det.iterrows():
        for p_ in _parse_po(r_.get("待交付详情")):
            rows.append({"店铺": r_["店铺"], "父ASIN": r_["父ASIN"], "MSKU": r_["MSKU"], "单据号": p_[0], "数量": float(num(pd.Series([p_[1]])).iloc[0] or 0),
                         "仓库": p_[2], "状态": p_[3], "下单": p_[4], "预计到货": p_[5], "预计可售": p_[6]})
    po = pd.DataFrame(rows, columns=["店铺", "父ASIN", "MSKU", "单据号", "数量", "仓库", "状态", "下单", "预计到货", "预计可售"])
    for c in ("下单", "预计到货", "预计可售"):
        po[c] = pd.to_datetime(po[c].replace("-", np.nan), errors="coerce").dt.date
    REPLEN_PO["df"] = po
    # ---- 质量提示
    q.add("INFO", "补货建议", f"{len(summ)}个店铺×父体汇总行，快照日{snap}；明细行有{n_dup}行是完全重复的(同店铺同MSKU出现3~4次)，已去重；共享池(本地可用/待交付)在{n_shared}个非主站父体行里重复显示，已只计入主站(US优先)，避免重复计算")
    if len(po):
        late = po[po["预计到货"].notna() & (po["预计到货"] < snap)]
        if len(late):
            g_ = late.groupby("单据号").agg(件数=("数量", "sum"), 预计到货=("预计到货", "first"))
            q.add("WARN", "待交付已过预计到货日", f"{len(g_)}张采购单的预计到货日已过(快照日{snap})但仍是待到货，共{int(g_['件数'].sum())}件：" +
                  "；".join(f"{k}({int(r_['件数'])}件，预计{r_['预计到货']}，已晚{(snap - r_['预计到货']).days}天)" for k, r_ in g_.sort_values('预计到货').head(5).iterrows()) +
                  "。断货模拟里这些PO的预计可售日已按晚到天数顺延；请先向工厂确认交期")
        nod = po[po["预计可售"].isna()]
        if len(nod):
            q.add("WARN", "待交付缺预计可售日", f"{nod['单据号'].nunique()}张采购单没有预计可售时间({int(nod['数量'].sum())}件)，无法放进断货模拟")
    return {"use": True, "snap": snap, "parents": summ, "po": po, "skus": skus}

def _sim_stockout(S0, dmd, arr, snap, H):
    """逐日模拟：每天先入库当天到仓的货，再按日均销量扣减；库存不够卖满一天的日均销量就记为断货日。到仓日<=快照日的按第1天入库；超出H天的忽略。返回(断货天数, 首次断货第几天)"""
    if not (dmd and dmd > 0) or S0 is None or (isinstance(S0, float) and np.isnan(S0)):
        return np.nan, np.nan
    inc = {}
    for dt_, qv in arr:
        if dt_ is None or qv is None or (isinstance(qv, float) and np.isnan(qv)):
            continue
        k = max(1, (dt_ - snap).days)
        inc[k] = inc.get(k, 0) + qv
    stock, out_days, first = float(S0), 0, None
    for t in range(1, H + 1):
        stock += inc.get(t, 0)
        if stock < dmd:
            out_days += 1
            first = first or t
            stock = 0.0
        else:
            stock -= dmd
    return out_days, first

def _cover_qty(S0, dmd, base_arr, when, snap, end_day):
    """现在发出、在 when 可售的一批货，最少多少件才能让 when ~ 第end_day天 都不断货(when 之前的断货已无法补救，不计)。
    end_day = 发货→可售天数 + 发货周期 + 安全库存天数。无销量=NaN；不发也够=0。"""
    if not (dmd and dmd > 0) or S0 is None or (isinstance(S0, float) and np.isnan(S0)):
        return np.nan
    start = max(1, (when - snap).days)
    def outs(qv):
        inc = {}
        for dt_, x in base_arr + ([(when, qv)] if qv else []):
            if dt_ is None or x is None or (isinstance(x, float) and np.isnan(x)):
                continue
            k = max(1, (dt_ - snap).days)
            inc[k] = inc.get(k, 0) + x
        stock, n_ = float(S0), 0
        for t in range(1, end_day + 1):
            stock += inc.get(t, 0)
            if stock < dmd:
                n_ += t >= start
                stock = 0.0
            else:
                stock -= dmd
        return n_
    if outs(0) == 0:
        return 0.0
    lo, hi = 0.0, float(dmd) * end_day + 1
    for _ in range(30):
        mid = (lo + hi) / 2
        if outs(mid) == 0:
            hi = mid
        else:
            lo = mid
    return float(np.ceil(hi))

def _min_dispatch(S0, dmd, base_arr, loc, when, snap, H):
    """本地仓最少发出多少件(到仓日=when)才能让未来H天不断货。已经不断货=0；全部发出仍断货=NaN。"""
    if not loc or not dmd or dmd <= 0:
        return np.nan
    full, _ = _sim_stockout(S0, dmd, base_arr + [(when, loc)], snap, H)
    if full > 0:
        return np.nan
    base_out, _ = _sim_stockout(S0, dmd, base_arr, snap, H)
    if base_out == 0:
        return 0.0
    lo, hi = 0.0, float(loc)
    for _ in range(24):
        mid = (lo + hi) / 2
        out, _ = _sim_stockout(S0, dmd, base_arr + [(when, mid)], snap, H)
        if out == 0:
            hi = mid
        else:
            lo = mid
    return float(np.ceil(hi))

def _wavg_windows(vals, age, W):
    """各窗口日均按权重加权；上架天数不足的长窗口不参与(新品的30/60天日均会被没开卖的日子拉低)，权重重新归一"""
    num_, den = 0.0, 0.0
    for n_, v in vals.items():
        if v is None or pd.isna(v):
            continue
        if n_ > 7 and age is not None and not pd.isna(age) and age < n_:
            continue
        w = float(W.get(str(n_), 0))
        num_ += w * float(v); den += w
    return num_ / den if den > 0 else np.nan


def add_forecast(P, replen, cfg, q):
    """日均_预测：7/14/30/60天日均加权，断货模拟与发货件数都用它；日均7(订单周窗口)只描述本周"""
    W = cfg.get("demand_weights") or DEFAULT_CONFIG["demand_weights"]
    agg = None
    if replen and replen.get("use") and replen.get("skus") is not None and len(replen["skus"]):
        agg = replen["skus"].groupby(["店铺", "父ASIN"])[["7天销量", "14天销量", "30天销量", "60天销量"]].sum(min_count=1)
    out = []
    for _, r in P.iterrows():
        k = (r["店铺"], r["父ASIN"])
        if agg is not None and k in agg.index:
            a = agg.loc[k]
            vals = {7: a["7天销量"] / 7, 14: a["14天销量"] / 14, 30: a["30天销量"] / 30, 60: a["60天销量"] / 60}
            src = "补货建议(截至快照日)"
        else:
            s14 = r.get("销量14")
            vals = {7: r.get("日均7"), 14: (s14 / 14 if s14 is not None and pd.notna(s14) else np.nan), 30: r.get("日均30"), 60: np.nan}
            src = "订单周+Listing"
        out.append((vals[7], vals[14], vals[30], vals[60], _wavg_windows(vals, r.get("上架天数"), W), src))
    for i_, c in enumerate(["日均_7天", "日均_14天", "日均_30天", "日均_60天", "日均_预测", "日均口径"]):
        P[c] = [x[i_] for x in out]
    wtxt = "/".join(f"{k}天{v}" for k, v in W.items())
    hot = P[(P["日均_7天"] > P["日均_预测"] * 1.2) & (P["日均_预测"] >= 3)]
    q.add("INFO", "预测日均", f"日均_预测=各窗口日均加权({wtxt})，断货模拟与建议件数都用它；上架天数不足的窗口不参与。" +
          (f"近7天明显放量(>预测20%)的父体：" + "；".join(f"{a_}/{b_} 7天{c_:.1f} vs 预测{d_:.1f}" for a_, b_, c_, d_ in zip(hot["店铺"].head(5), hot["款"].head(5), hot["日均_7天"].head(5), hot["日均_预测"].head(5))) +
           "，若放量持续，空运/发货量偏少(见 建议空运件数_按近7天日均)" if len(hot) else ""))
    return P


def _sim_short(S0, dmd, arr, snap, H):
    """逐日模拟，返回每天没卖出去的件数(缺口)列表；到仓日按可售日入库"""
    inc = {}
    for dt_, qv in arr:
        if dt_ is None or qv is None or (isinstance(qv, float) and np.isnan(qv)):
            continue
        k = max(1, (dt_ - snap).days)
        inc[k] = inc.get(k, 0) + qv
    stock, short = float(S0 or 0), []
    for t in range(1, H + 1):
        stock += inc.get(t, 0)
        if stock < dmd:
            short.append(dmd - stock); stock = 0.0
        else:
            short.append(0.0); stock -= dmd
    return short


SKU_SUPPLY = {}      # 尺码级模拟明细，供落库/数据包使用
SKU_AGE = {}         # (店铺,MSKU) -> 开售天数(Listing 首单时间/创建时间)

def sku_supply(P, replen, ship, po, snap, cfg, q):
    """尺码(子体SKU)级断货模拟：亚马逊按子体判断缺货，父体合计会用别的尺码库存抵消，低估缺货、也会建议从本地仓发并不缺的颜色。
    每个SKU：S0=可售+在途货件+已签收待上架(按可售日)，S1=+待交付PO；空运/海运件数逐SKU算再合计。"""
    sk = replen.get("skus")
    if sk is None or not len(sk):
        return P
    H = int(cfg.get("shipment_horizon_days", 60))
    sea_d, air_d, prep, rv = LEAD_DAYS["full"]
    cyc, ss = int(cfg.get("replen_cycle_days", 10)), int(cfg.get("safety_stock_days", 10))
    W = cfg.get("demand_weights") or DEFAULT_CONFIG["demand_weights"]
    d_air, d_sea = prep + air_d + rv, prep + sea_d + rv
    w_air, w_sea = snap + timedelta(days=d_air), snap + timedelta(days=d_sea)
    cover_end = d_sea + cyc + ss
    arr_sku = (ship or {}).get("arrivals_sku") or {}
    po_sku = {}
    for (st_, m_), g_ in po.groupby(["店铺", "MSKU"]):
        po_sku[(st_, m_)] = [(a_, b_) for a_, b_ in zip(g_["预计可售_顺延"], g_["数量"]) if a_ is not None]
    age = dict(zip(zip(P["店铺"], P["父ASIN"]), P["上架天数"]))
    ex_fba, ex_tot = int(cfg.get("excess_fba_days", 90)), int(cfg.get("excess_total_days", 180))
    new_days = int(cfg.get("slow_min_age_days", 45))
    plan = {}
    for st_, pdf_ in PLANNING_SKU.items():
        for rec in pdf_.to_dict("records"):
            plan[(st_, rec["MSKU"])] = rec
    keep = set(zip(P["店铺"], P["父ASIN"]))
    rows = []
    for _, r in sk.iterrows():
        k = (r["店铺"], r["父ASIN"])
        if k not in keep:
            continue
        vals = {7: r["7天销量"] / 7, 14: r["14天销量"] / 14, 30: r["30天销量"] / 30, 60: r["60天销量"] / 60}
        d = _wavg_windows(vals, age.get(k), W)
        d = float(d) if (d is not None and pd.notna(d) and d > 0) else 0.0
        S0 = float(r["可售"]) if pd.notna(r["可售"]) else 0.0
        a0 = list(arr_sku.get((r["店铺"], r["MSKU"]), []))
        a1 = a0 + po_sku.get((r["店铺"], r["MSKU"]), [])
        # ---- 冗余/滞销(所有有库存的SKU，含零销量)
        inb, pos = float(sum(b_ for _, b_ in a0)), float(sum(b_ for _, b_ in a1)) - float(sum(b_ for _, b_ in a0))
        pool_own = float(r["本地可用_池"] or 0) if bool(r.get("池主站行", True)) else 0.0
        pl = plan.get((r["店铺"], str(r["MSKU"])), {})
        sage = SKU_AGE.get((r["店铺"], str(r["MSKU"])))
        fr_ = ((ship or {}).get("first_recv_sku") or {}).get((r["店铺"], str(r["MSKU"])))
        fba_age = (snap - fr_).days if fr_ is not None else None
        # 新SKU：开售不足N天，或第一次到FBA不足N天(FBA刚有货，30天零销量不代表滞销)；从没到过FBA且没有可售=本地未发，不算滞销
        young = (sage is not None and sage < new_days) or (fba_age is not None and fba_age < new_days) or (fr_ is None and S0 <= 0)
        ex = {"首次到FBA天数": fba_age, "FBA冗余件数": 0.0 if young else max(0.0, S0 + inb - d * ex_fba), "总冗余件数": 0.0 if young else max(0.0, S0 + inb + pos + pool_own - d * ex_tot),
              "本地仓计入": pool_own, "开售天数": sage, "新SKU观察": bool(young and (S0 + inb + pool_own) > 0),
              "滞销": bool(not young and (r["30天销量"] or 0) == 0 and (S0 + pool_own) > 0),
              "60天零销量": bool((r["60天销量"] or 0) == 0 and (S0 + pool_own) > 0),
              "FBA库存天数": (S0 + inb) / d if d > 0 else np.nan, "总库存天数": (S0 + inb + pos + pool_own) / d if d > 0 else np.nan,
              "库龄91天以上": pl.get("库龄91天以上"), "库龄181天以上": pl.get("库龄181天以上"), "亚马逊冗余件数": pl.get("亚马逊冗余件数"),
              "亚马逊建议": pl.get("亚马逊建议"), "亚马逊建议价": pl.get("亚马逊建议价"), "预估月仓储费": pl.get("预估月仓储费"),
              "标价": pl.get("标价"), "促销价": pl.get("促销价")}
        ex["可削减PO件数"] = min(pos, ex["总冗余件数"])
        if not (d > 0):
            if S0 + inb + pos + pool_own > 0:
                rows.append({"店铺": r["店铺"], "父ASIN": r["父ASIN"], "MSKU": r["MSKU"], "日均_预测": 0.0, "日均_7天": 0.0, "FBA可售": S0,
                             "已到仓待上架": 0.0, "在途与待上架": inb, "待交付": pos, "本地可用_池": float(r["本地可用_池"] or 0),
                             "缺货件数_保守": 0.0, "缺货件数_含待交付": 0.0, "空运前无法避免缺货件数": 0.0, "空运本地可发": 0.0, "空运本地不足": 0.0,
                             "海运本地可发": 0.0, "海运本地不足": 0.0, **ex})
            continue
        sh0, sh1 = _sim_short(S0, d, a0, snap, H), _sim_short(S0, d, a1, snap, H)
        first = next((t + 1 for t, x in enumerate(sh1) if x > 0), None)
        air = _cover_qty(S0, d, a1, w_air, snap, d_sea) if d_air < d_sea else np.nan
        d7s = vals[7] if (vals[7] is not None and pd.notna(vals[7])) else d
        air7 = _cover_qty(S0, d7s, a1, w_air, snap, d_sea) if (d_air < d_sea and d7s > d * 1.05) else air
        sea = _cover_qty(S0, d, a1, w_sea, snap, cover_end)
        pool = float(r["本地可用_池"] or 0)
        air_ = air if pd.notna(air) else 0.0
        rows.append({"店铺": r["店铺"], "父ASIN": r["父ASIN"], "MSKU": r["MSKU"], "日均_预测": d, "日均_7天": vals[7], "FBA可售": S0,
                     "已到仓待上架": float(((ship or {}).get("arrived_sku") or {}).get((r["店铺"], r["MSKU"]), 0)),
                     "在途与待上架": float(sum(b_ for _, b_ in a0)), "待交付": float(sum(b_ for _, b_ in a1)) - float(sum(b_ for _, b_ in a0)),
                     "本地可用_池": pool, "首次缺货_天后": first, "缺货件数_保守": sum(sh0), "缺货件数_含待交付": sum(sh1),
                     "空运前无法避免缺货件数": sum(sh1[: max(0, d_air - 1)]), "建议空运件数": air, "建议空运件数_按近7天": air7, "建议海运件数": sea,
                     "空运本地可发": min(air_, pool), "空运本地不足": max(0.0, air_ - pool),
                     "海运本地可发": min(sea or 0, max(0.0, pool - air_)), "海运本地不足": max(0.0, (sea or 0) - max(0.0, pool - air_)), **ex})
    D = pd.DataFrame(rows)
    if not len(D):
        return P
    D["当前断码"] = (D["FBA可售"] < 1) & (D["日均_预测"] >= 0.1)
    D["需求占比"] = D["日均_预测"] / D.groupby(["店铺", "父ASIN"])["日均_预测"].transform("sum")
    D["主力尺码"] = D["需求占比"] >= 0.05          # 占父体需求5%以上才算主力，新颜色/冷门码只单独计数
    SKU_SUPPLY["df"] = D
    short = lambda m_: m_.split("-", 1)[1] if "-" in str(m_) else str(m_)
    def detail(g, col):
        g = g[g[col] > 0].sort_values(col, ascending=False)
        return "、".join(f"{short(m_)} {v_:.0f}" for m_, v_ in zip(g["MSKU"], g[col]))
    agg = []
    for (st_, pa_), g in D.groupby(["店铺", "父ASIN"]):
        need = float(g["日均_预测"].sum()) * H
        agg.append({"店铺": st_, "父ASIN": pa_, "尺码_在售SKU数": len(g), "尺码_当前断码数": int(g["当前断码"].sum()),
                    "尺码_当前断码_主力数": int((g["当前断码"] & g["主力尺码"]).sum()),
                    "尺码_当前断码": ("、".join(f"{short(m_)}(日均{d_:.1f}，" + (f"已到仓{a_:.0f}件待上架)" if a_ > 0 else "无货在途)")
                                          for m_, d_, a_ in g.loc[g["当前断码"] & g["主力尺码"]].sort_values("日均_预测", ascending=False)[["MSKU", "日均_预测", "已到仓待上架"]].values[:6])
                                   + (f"；另有{int((g['当前断码'] & ~g['主力尺码']).sum())}个冷门码可售为0" if (g["当前断码"] & ~g["主力尺码"]).any() else "")).lstrip("；"),
                    "尺码_断码数_含待交付": int((g["缺货件数_含待交付"] >= 1).sum()),
                    "尺码_缺货件数_保守": float(g["缺货件数_保守"].sum()), "尺码_缺货件数_含待交付": float(g["缺货件数_含待交付"].sum()),
                    "尺码_缺货占需求比例": float(g["缺货件数_含待交付"].sum()) / need if need > 0 else np.nan,
                    "尺码_空运前无法避免缺货件数": float(g["空运前无法避免缺货件数"].sum()),
                    "建议空运件数_尺码合计": float(g["建议空运件数"].fillna(0).sum()),
                    "建议空运件数_尺码合计_按近7天": float(g["建议空运件数_按近7天"].fillna(0).sum()),
                    "空运_本地可发件数": float(g["空运本地可发"].sum()), "空运尺码明细": detail(g, "空运本地可发"),
                    "尺码_空运本地不足件数": float(g["空运本地不足"].sum()), "空运本地不足明细": detail(g, "空运本地不足"),
                    "建议海运发货件数_尺码合计": float(g["建议海运件数"].fillna(0).sum()),
                    "海运_本地可发件数": float(g["海运本地可发"].sum()), "海运尺码明细": detail(g, "海运本地可发"),
                    "尺码_本地仓不足件数": float((g["空运本地不足"] + g["海运本地不足"]).sum()),
                    "尺码_本地仓不足明细": detail(g.assign(_n=g["空运本地不足"] + g["海运本地不足"]), "_n"),
                    "冗余_FBA件数": float(g["FBA冗余件数"].sum()), "冗余_FBA明细": detail(g, "FBA冗余件数"),
                    "冗余_总件数": float(g["总冗余件数"].sum()), "冗余_总明细": detail(g, "总冗余件数"),
                    "冗余_可削减PO件数": float(g["可削减PO件数"].sum()), "冗余_可削减PO明细": detail(g, "可削减PO件数"),
                    "新SKU观察数": int(g["新SKU观察"].sum()),
                    "滞销_SKU数": int(g["滞销"].sum()), "滞销_FBA件数": float(g.loc[g["滞销"], "FBA可售"].sum()),
                    "滞销_本地件数": float(g.loc[g["滞销"], "本地仓计入"].sum()),
                    "滞销明细": detail(g.assign(_n=(g["FBA可售"] + g["本地仓计入"]).where(g["滞销"].astype(bool), 0)), "_n"),
                    "亚马逊冗余件数": float(pd.to_numeric(g["亚马逊冗余件数"], errors="coerce").fillna(0).sum()),
                    "亚马逊建议促销明细": "、".join(f"{short(m_)} 建议价{p_:.2f}" for m_, a_, p_ in zip(g["MSKU"], g["亚马逊建议"], g["亚马逊建议价"])
                                              if str(a_) in ("Create sale", "Lower price", "Create Outlet deal") and pd.notna(p_))[:300],
                    "促销价明细": "、".join(f"{short(m_)} {v_:.2f}" for m_, v_ in sorted(
                        [(m_, float(v_)) for m_, v_ in zip(g["MSKU"], pd.to_numeric(g["促销价"], errors="coerce")) if pd.notna(v_)], key=lambda t_: t_[1])),
                    "冗余_预估月仓储费": float(pd.to_numeric(g.loc[g["FBA冗余件数"] > 0, "预估月仓储费"], errors="coerce").fillna(0).sum())})
    P = P.merge(pd.DataFrame(agg), on=["店铺", "父ASIN"], how="left")
    bad = P[P["尺码_缺货件数_含待交付"].fillna(0) >= 1].sort_values("尺码_缺货件数_含待交付", ascending=False)
    if len(bad):
        q.add("WARN", "尺码级缺货", f"{len(bad)} 个父体有尺码在60天内会缺货(含待交付仍缺)：" +
              "；".join(f"{a_}/{b_} 缺{c_:.0f}件，本地仓不足{d_:.0f}件" for a_, b_, c_, d_ in zip(bad["店铺"].head(5), bad["款"].head(5), bad["尺码_缺货件数_含待交付"].head(5), bad["尺码_本地仓不足件数"].head(5))) +
              "。父体级'断货天数'会被其它尺码库存抵消，补货/发货以尺码级为准")
    return P


def supply_summary(P, replen, ship, cfg, q):
    """把补货建议的 本地可用/待交付/领星建议 并到父体表，并把'待交付'(按预计可售日)和'本地仓库存'(按空运/海运发出)加进断货模拟。"""
    par = replen["parents"]
    snap = ship["snap"] if (ship and ship.get("use")) else replen["snap"]
    if ship and ship.get("use") and abs((ship["snap"] - replen["snap"]).days) > 1:
        q.add("WARN", "快照日不一致", f"FBA货件快照{ship['snap']}与补货建议快照{replen['snap']}相差超过1天，供给链条的时间点可能错位")
    H = int(cfg.get("shipment_horizon_days", 60)); rv = int(cfg.get("receiving_avail_days", 10)); prep = int(cfg.get("local_ship_prep_days", 2))
    ren = {"7天日均": "领星_7天日均", "断货时间": "领星_断货时间", "建议采购日": "领星_建议采购日", "建议本地发货日": "领星_建议本地发货日", "建议采购量": "领星_建议采购量",
           "建议采购量-海派": "领星_建议采购量_海派", "本地发FBA量": "领星_本地发FBA量", "本地发FBA量-海派": "领星_本地发FBA量_海派", "备货时长": "领星_备货时长"}
    keep = ["店铺", "父ASIN", "补货_共享池已计入主站"] + [c for c in POOL_COLS if c in par.columns] + [c for c in ren if c in par.columns]
    P = P.merge(par[keep].rename(columns=ren), on=["店铺", "父ASIN"], how="left")
    cov = P["店铺"].isin(set(par["店铺"]))
    for c in ["本地可用", "待检待上架量", "待交付", "本地仓在途", "采购计划"]:
        if c in P.columns:
            P.loc[cov, c] = P.loc[cov, c].fillna(0)
    for c in ("领星_断货时间", "领星_建议采购日", "领星_建议本地发货日"):
        if c in P.columns:
            P[c] = pd.to_datetime(P[c], errors="coerce").dt.strftime("%Y-%m-%d")
    # ---- PO 聚合(预计可售日：已过预计到货日的按晚到天数顺延)
    po = replen["po"].copy()
    po["顺延天数"] = [max(0, (snap - a_).days) if a_ is not None and not pd.isna(a_) else 0 for a_ in po["预计到货"]]
    po["预计可售_顺延"] = [(b_ + timedelta(days=int(n_))) if b_ is not None and not pd.isna(b_) else None for b_, n_ in zip(po["预计可售"], po["顺延天数"])]
    allmax = max([x for x in po["预计可售_顺延"] if x is not None], default=None)
    agg = {}
    for (st_, pa_), g_ in po.groupby(["店铺", "父ASIN"]):
        per_po = g_.groupby("单据号").agg(数量=("数量", "sum"), 到货=("预计到货", "first"), 可售=("预计可售_顺延", "first"))
        agg[(st_, pa_)] = {"待交付_PO数": len(per_po), "待交付_PO件数": float(per_po["数量"].sum()),
                           "待交付_最早预计到货日": min([x for x in per_po["到货"] if x is not None and not pd.isna(x)], default=None),
                           "待交付_最早预计可售日": min([x for x in per_po["可售"] if x is not None], default=None),
                           "待交付_最晚预计可售日": max([x for x in per_po["可售"] if x is not None], default=None),
                           "待交付_已过预计到货件数": float(per_po.loc[[(x is not None and not pd.isna(x) and x < snap) for x in per_po["到货"]], "数量"].sum())}
    A = pd.DataFrame([{"店铺": k[0], "父ASIN": k[1], **v} for k, v in agg.items()])
    if len(A):
        P = P.merge(A, on=["店铺", "父ASIN"], how="left")
    for c in ("待交付_最早预计到货日", "待交付_最早预计可售日", "待交付_最晚预计可售日"):
        if c in P.columns:
            P[c] = P[c].astype(str).replace({"NaT": "", "None": "", "nan": ""})
    for c in ("待交付_PO数", "待交付_PO件数", "待交付_已过预计到货件数"):
        if c not in P.columns:
            P[c] = np.nan
        P.loc[cov, c] = P.loc[cov, c].fillna(0)
    P["待交付_无PO明细件数"] = (P["待交付"].fillna(0) - P["待交付_PO件数"].fillna(0)).clip(lower=0).where(cov)
    gap = P[P["待交付_无PO明细件数"] > 0]
    if len(gap):
        q.add("WARN", "待交付汇总大于PO明细", f"{len(gap)}个父体的'待交付'汇总比SKU明细里能对上PO的件数多：" + "；".join(f"{a_}/{b_} 汇总{int(c_)} 差{int(d_)}件" for a_, b_, c_, d_ in zip(gap["店铺"].head(5), gap["款"].head(5), gap["待交付"].head(5), gap["待交付_无PO明细件数"].head(5))) +
              "。这部分没有单据号和到货日(可能是没有Listing的SKU的采购)，模拟里按该父体最晚的预计可售日保守处理")
    # ---- 断货模拟：S0=FBA+在途货件(保守)；S1=+待交付；S2=+本地仓库存今天发出(空运/海运)
    lt = (ship or {}).get("lt") or {}
    sea_d = int(round(lt.get("海运", float(cfg.get("lead_sea_days", 30))))); air_d = int(round(lt.get("空运", float(cfg.get("lead_air_days", 12)))))
    arr_c = (ship or {}).get("arrivals_c") or {}
    po_by = {k: g_ for k, g_ in po.groupby(["店铺", "父ASIN"])}
    cyc, ss = int(cfg.get("replen_cycle_days", 10)), int(cfg.get("safety_stock_days", 10))
    cover_end = prep + sea_d + rv + cyc + ss         # 今天海运发出的货要覆盖到第几天(到可售 + 发货周期 + 安全库存)
    res = []
    for _, r in P.iterrows():
        key = (r["店铺"], r["父ASIN"]); dmd = r.get("日均_预测"); S0 = r.get("FBA可售")
        base = list(arr_c.get(key, []))
        pl = []
        g_ = po_by.get(key)
        last_d = None
        if g_ is not None:
            for _, x in g_.iterrows():
                if x["预计可售_顺延"] is not None:
                    pl.append((x["预计可售_顺延"], x["数量"]))
            last_d = max([a_ for a_, _ in pl], default=None)
        gapq = r.get("待交付_无PO明细件数")
        if gapq and gapq > 0 and (last_d or allmax):
            pl.append((last_d or allmax, gapq))
        loc = r.get("本地可用")
        loc = loc if (loc is not None and not (isinstance(loc, float) and np.isnan(loc)) and loc > 0) else 0
        s1 = base + pl
        out1, f1 = _sim_stockout(S0, dmd, s1, snap, H)
        oa, fa = _sim_stockout(S0, dmd, s1 + ([(snap + timedelta(days=prep + air_d + rv), loc)] if loc else []), snap, H)
        os_, fs = _sim_stockout(S0, dmd, s1 + ([(snap + timedelta(days=prep + sea_d + rv), loc)] if loc else []), snap, H)
        w_air, w_sea = snap + timedelta(days=prep + air_d + rv), snap + timedelta(days=prep + sea_d + rv)
        ma = _min_dispatch(S0, dmd, s1, loc, w_air, snap, H); ms = _min_dispatch(S0, dmd, s1, loc, w_sea, snap, H)
        cq = _cover_qty(S0, dmd, s1, w_sea, snap, cover_end)
        # 空运：今天空运发出，覆盖 空运可售日 ~ 海运可售日(之后由今天发的海运接上)；空运可售之前的断货任何发货都补不上
        d_air = (w_air - snap).days
        d7 = r.get("日均7")
        if w_air < w_sea and d7 and dmd and pd.notna(d7) and d7 > dmd * 1.05:   # 敏感性：若近7天的放量持续(日均7高于预测)需要多少空运
            cqa_lo = _cover_qty(S0, d7, s1, w_air, snap, (w_sea - snap).days)
        else:
            cqa_lo = np.nan
        if w_air < w_sea:
            cqa = _cover_qty(S0, dmd, s1, w_air, snap, (w_sea - snap).days)
            pre, _ = _sim_stockout(S0, dmd, s1, snap, d_air - 1) if d_air > 1 else (0 if dmd and dmd > 0 else np.nan, None)
        else:
            cqa, pre = np.nan, np.nan
        res.append((out1, f1, oa, fa, os_, fs, ma, ms, cq, cqa, pre, cqa_lo))
    for i_, c in enumerate(["断货天数_含待交付", "首次断货_含待交付_天后", "断货天数_全供给_空运", "首次断货_全供给_空运_天后", "断货天数_全供给_海运", "首次断货_全供给_海运_天后",
                            "本地仓最少空运件数_避免断货", "本地仓最少海运件数_避免断货", "建议海运发货件数", "建议空运件数", "空运可售前无法避免断货天数", "建议空运件数_按近7天日均"]):
        P[c] = [x[i_] for x in res]
    _loc = P["本地可用"].fillna(0)
    P["空运发货_本地仓不足件数"] = (P["建议空运件数"] - _loc).clip(lower=0).where(P["建议空运件数"].notna())
    _left = (_loc - P["建议空运件数"].fillna(0)).clip(lower=0)          # 本地仓先保空运，剩下的才给海运
    P["海运发货_本地仓不足件数"] = (P["建议海运发货件数"] - _left).clip(lower=0).where(P["建议海运发货件数"].notna())
    P["空运可售_天后"], P["海运可售_天后"] = prep + air_d + rv, prep + sea_d + rv
    P["海运发货_覆盖至第N天"] = cover_end
    P["未来60日缺口件数_含全部供给"] = (P["日均_预测"] * H - P["FBA可售"].fillna(0) - P["在途"].fillna(0) - P["待交付"].fillna(0) - P["本地可用"].fillna(0)).clip(lower=0).where(P["日均_预测"] > 0)
    LEAD_DAYS["full"] = (sea_d, air_d, prep, rv)     # 不能放 P.attrs：之后的 merge 会丢掉 attrs
    try:
        P = sku_supply(P, replen, ship, po, snap, cfg, q)
    except Exception as e:
        q.add("WARN", "尺码级模拟", f"失败，只用父体级结果：{e}")
    return P


def ship_summary(P, ship, cfg, q, L):
    """按 (店铺,父ASIN) 汇总在途货件：到仓分桶 + 用日均销量模拟未来N天是否断货。返回 (P, 明细DataFrame)"""
    d = ship["rows"]; snap = ship["snap"]; H = int(cfg.get("shipment_horizon_days", 60)); rv = int(cfg.get("receiving_avail_days", 10))
    live = d[(d["待收"] > 0) | (d["计划"] > 0)].copy()
    pend = d[d["待上架"] > 0].copy() if "待上架" in d.columns else d.iloc[0:0].copy()
    if len(pend):          # 已签收待上架：单独成行，到仓日=签收日，件数=待上架
        pend["待收"], pend["计划"], pend["状态"], pend["入库开始日"] = pend["待上架"], 0.0, "已签收待上架", pend["签收日"]
        live = pd.concat([live, pend], ignore_index=True)
    m2p = dict(zip(zip(L["店铺"], L["MSKU"]), L["父ASIN"]))
    live["父ASIN"] = live["父ASIN"].where(live["父ASIN"].notna(), pd.Series([m2p.get((s_, k_)) for s_, k_ in zip(live["店铺"], live["MSKU"])], index=live.index))
    live = live.dropna(subset=["父ASIN"])
    qty = live["待收"] + live["计划"]
    is_rcv = live["状态"].isin(["RECEIVING", "已签收待上架"])
    # 到仓日：入库中=开始入库日(缺失用快照日)；已发货=送达时段(起/止)；无窗口=缺失
    # 可售日=到仓日+receiving_avail_days(亚马逊收货上架)，断货模拟一律按可售日入库
    _rs = [x if (x is not None and not pd.isna(x)) else snap for x in live["入库开始日"]]
    live["到仓_乐观"] = [a_ if r else w for r, a_, w in zip(is_rcv, _rs, live["窗口起"])]
    live["到仓_保守"] = [a_ if r else w for r, a_, w in zip(is_rcv, _rs, live["窗口止"])]
    for a_, b_ in (("到仓_乐观", "可售_乐观"), ("到仓_保守", "可售_保守")):
        live[b_] = [(x + timedelta(days=rv)) if (x is not None and not pd.isna(x)) else None for x in live[a_]]
    live["件数"] = qty
    # 计入供给：与表内'在途'同口径——有在售子体的父体只计发往在售子体(或Listing里查不到状态)的件数；没有在售子体的父体('未在售'行)全部计入
    st_map = dict(zip(zip(L["店铺"], L["MSKU"]), L["状态"]))
    sell_par = set(zip(P.loc[P["在售子体数"] > 0, "店铺"], P.loc[P["在售子体数"] > 0, "父ASIN"]))
    live["计入供给"] = [(st_map.get((s_, m_), "在售") == "在售") or ((s_, p_) not in sell_par)
                     for s_, m_, p_ in zip(live["店铺"], live["MSKU"], live["父ASIN"])]
    all_tot = live[live["状态"] != "已签收待上架"].groupby(["店铺", "父ASIN"])["件数"].sum()   # 全部在途(含发往停售子体)，只用于与Listing对账
    detail = live[["店铺", "父ASIN", "货件单号", "状态", "运输方式", "物流中心编码", "发货日", "窗口起", "窗口止", "计入供给", "已发货", "签收量", "件数"]].copy()
    rows = {}
    for (store, par), g in live[live["计入供给"]].groupby(["店铺", "父ASIN"]):
        r = {"在途货件数": g.loc[g["状态"] != "已签收待上架", "货件单号"].nunique(), "货件_已发货在途件数": float(g.loc[g["状态"].isin(["SHIPPED", "IN_TRANSIT"]), "件数"].sum()),
             "货件_入库中待收件数": float(g.loc[g["状态"] == "RECEIVING", "件数"].sum()), "货件_计划入库件数": float(g.loc[g["状态"] == "WORKING", "件数"].sum()),
             "货件_已签收待上架件数": float(g.loc[g["状态"] == "已签收待上架", "件数"].sum()),
             "货件_无送达时段件数": float(g.loc[g["到仓_保守"].isna(), "件数"].sum())}
        dd = g.dropna(subset=["到仓_保守"])
        r["最早到仓日"] = min(g["到仓_乐观"].dropna()) if g["到仓_乐观"].notna().any() else None
        r["最晚到仓日"] = max(dd["到仓_保守"]) if len(dd) else None
        r["最晚可售日"] = max(dd["可售_保守"]) if len(dd) else None
        days_c = pd.Series([(x - snap).days for x in dd["到仓_保守"]], index=dd.index) if len(dd) else pd.Series(dtype=float)
        for lab, lo, hi in (("7日内", -999, 7), ("8-14日", 8, 14), ("15-30日", 15, 30), ("30日后", 31, 9999)):
            r["到仓_保守_" + lab] = float(dd.loc[(days_c >= lo) & (days_c <= hi), "件数"].sum()) if len(dd) else 0.0
        rows[(store, par)] = (r, g)
    ship["arrivals_c"] = {k: [(a_, b_) for a_, b_ in zip(v[1]["可售_保守"], v[1]["件数"]) if a_] for k, v in rows.items()}
    ship["arrivals_sku"] = {}
    _pn = live[live["计入供给"] & live["状态"].isin(["RECEIVING", "已签收待上架"])]
    ship["arrived_sku"] = _pn.groupby(["店铺", "MSKU"])["件数"].sum().to_dict()      # 已到仓(入库中/已签收)还没上架的件数
    for (st_, m_), g_ in live[live["计入供给"]].groupby(["店铺", "MSKU"]):
        ship["arrivals_sku"][(st_, m_)] = [(a_, b_) for a_, b_ in zip(g_["可售_保守"], g_["件数"]) if a_]
    S = pd.DataFrame([{"店铺": k[0], "父ASIN": k[1], **v[0]} for k, v in rows.items()])
    P = P.merge(S, on=["店铺", "父ASIN"], how="left")
    # 有货件文件覆盖的店铺：没有在途货件=0
    cov = P["店铺"].isin(set(d["店铺"].unique()))
    for c in ["在途货件数", "货件_已发货在途件数", "货件_入库中待收件数", "货件_计划入库件数", "货件_已签收待上架件数", "货件_无送达时段件数"] + [c for c in P.columns if c.startswith("到仓_保守_")]:
        if c not in P.columns:
            P[c] = np.nan
        P.loc[cov, c] = P.loc[cov, c].fillna(0)
    for c in ("最早到仓日", "最晚到仓日", "最晚可售日"):          # 与其它日期列一致，存为 YYYY-MM-DD 文本
        P[c] = P[c].astype(str).replace({"NaT": "", "None": "", "nan": ""})
    P["模拟起算日"] = str(snap)                      # 断货模拟/首次断货_天后 从货件快照日起算(不是周结束日)
    # ---- 断货模拟(逐日)：库存 S、日均销量 dmd、按到仓日入库；返回未来H天里断货的天数
    def sim(S0, dmd, arr):
        """逐日模拟：每天先入库当天到仓的货，再按日均销量扣减；库存不够卖满一天的日均销量就记为断货日。到仓日<=今天的按第1天入库；超出H天的忽略"""
        if not (dmd and dmd > 0) or S0 is None or (isinstance(S0, float) and np.isnan(S0)):
            return np.nan, np.nan
        inc = {}
        for dt_, qv in arr:
            k = max(1, (dt_ - snap).days)
            inc[k] = inc.get(k, 0) + qv
        stock, out_days, first = float(S0), 0, None
        for t in range(1, H + 1):
            stock += inc.get(t, 0)
            if stock < dmd:
                out_days += 1
                first = first or t
                stock = 0.0
            else:
                stock -= dmd
        return out_days, first
    res = []
    for _, r in P.iterrows():
        g = rows.get((r["店铺"], r["父ASIN"]), (None, None))[1]
        dmd = r.get("日均_预测")
        if g is None:
            arr_o, arr_c = [], []
        else:
            arr_o = list(zip(g["可售_乐观"], g["件数"])); arr_c = list(zip(g["可售_保守"], g["件数"]))
            arr_o = [(a, b) for a, b in arr_o if a]; arr_c = [(a, b) for a, b in arr_c if a]
        so, fo = sim(r.get("FBA可售"), dmd, arr_o)
        sc, fc = sim(r.get("FBA可售"), dmd, arr_c)
        res.append((so, sc, fc))
    P["断货天数_乐观"] = [x[0] for x in res]; P["断货天数_保守"] = [x[1] for x in res]; P["首次断货_天后"] = [x[2] for x in res]
    need60 = P["日均_预测"] * H
    P["未来60日缺口件数"] = (need60 - P["FBA可售"].fillna(0) - P["在途"].fillna(0)).clip(lower=0).where(P["日均_预测"] > 0)
    # ---- 与 Listing 在途对账
    lv = P.loc[cov, ["店铺", "父ASIN", "在途"]].copy()
    lv["货件合计"] = [float(all_tot.get((a_, b_), 0)) for a_, b_ in zip(lv["店铺"], lv["父ASIN"])]
    lv["差"] = lv["货件合计"] - lv["在途"].fillna(0)
    # Listing 全状态(含停售子体)的在途：用来解释差异——发往停售/未激活子体的在途，不在在售口径的 在途 里
    la = L.assign(_in=L[["FBA计划入库", "FBA标发在途", "FBA入库中"]].fillna(0).sum(axis=1)).groupby(["店铺", "父ASIN"])["_in"].sum()
    lv["Listing含停售"] = [float(la.get((a_, b_), 0)) for a_, b_ in zip(lv["店铺"], lv["父ASIN"])]
    lv["发往非在售子体"] = (lv["Listing含停售"] - lv["在途"].fillna(0)).clip(lower=0)
    _nosale = (P.loc[cov, "在售子体数"].fillna(0) == 0).values
    lv.loc[_nosale, "发往非在售子体"] = lv.loc[_nosale, "Listing含停售"]      # '未在售'行：在途全部发往非在售子体
    lv["未解释差异"] = lv["货件合计"] - lv["Listing含停售"]
    P = P.merge(lv[["店铺", "父ASIN", "发往非在售子体"]].rename(columns={"发往非在售子体": "在途_发往非在售子体件数"}), on=["店铺", "父ASIN"], how="left")
    bad = lv[lv["未解释差异"].abs() > 5]
    ina = lv[lv["发往非在售子体"] > 0]
    q.add("OK" if not len(bad) else "WARN", "对账:货件文件在途 vs Listing在途",
          f"{len(lv)} 个父体：货件文件在途 {int(lv['货件合计'].sum())}，Listing(含停售子体) {int(lv['Listing含停售'].sum())}，在售口径(表内'在途') {int(lv['在途'].fillna(0).sum())}；无法解释的差异(>5件) {len(bad)} 个" +
          (("：" + "；".join(f"{a_}/{b_[-6:]} 货件{int(c_)} vs Listing{int(d_)}" for a_, b_, c_, d_ in zip(bad['店铺'].head(4), bad['父ASIN'].head(4), bad['货件合计'].head(4), bad['Listing含停售'].head(4)))) if len(bad) else ""))
    if len(ina):
        q.add("WARN", "在途发往未激活/停售子体", f"{len(ina)} 个父体共 {int(ina['发往非在售子体'].sum())} 件在途是发往停售/未在售子体的(有在售子体的父体：这部分不在表内'在途'、不计入断货模拟；阶段='未在售'的父体：表内'在途'全部属于此类)：" +
              "；".join(f"{a_}/{b_[-6:]} {int(c_)}件" for a_, b_, c_ in zip(ina['店铺'].head(6), ina['父ASIN'].head(6), ina['发往非在售子体'].head(6))) + "。货到仓前要确认这些子体Listing已激活，否则到仓也卖不了")
    # 最晚发货日：要在首次断货前到仓，海运/空运最晚多少天后发货(负数=已经来不及)
    lt = ship.get("lt") or {}
    sea, air = lt.get("海运", float(cfg.get("lead_sea_days", 30))), lt.get("空运", float(cfg.get("lead_air_days", 12)))
    P["海运最晚发货_天后"] = P["首次断货_天后"] - (sea + rv)      # 发货→可售 = 历史中位运输(发货→开始入库) + 上架天数
    P["空运最晚发货_天后"] = P["首次断货_天后"] - (air + rv)
    return P, detail

ORDER_NEED = ["店铺", "订单状态", "订购日期", "MSKU", "数量", "销售额(Item Price)"]
ORDER_OPT = ["ASIN", "SKU", "订单币种", "是否促销", "是否B2B", "换货订单", "是否退款", "是否退货"]

def read_orders(path, cfg, q, week_end):
    """领星"订单导出"(订单明细，可按日期导出)。只读取需要的列——买家姓名/邮箱、收件人、电话、地址等个人信息列不会被载入内存。
    口径(已与SP-API销售与流量报告逐日核对一致)：订购日期为站点当地时间；件数=数量合计(Canceled 数量为0)；销售额=销售额(Item Price)合计，
    含Pending、含B2B、含换货订单(换货件数计入件数但销售额为0)。"""
    try:
        head = list(pd.read_excel(path, nrows=0).columns)
    except Exception as e:
        q.add("WARN", "订单导出", f"读取失败：{e}")
        return None
    miss = [c for c in ORDER_NEED if c not in head]
    if miss:
        q.add("WARN", "订单导出", f"缺少必要列 {miss}，本次不使用订单导出")
        return None
    d = pd.read_excel(path, usecols=[c for c in ORDER_NEED + ORDER_OPT if c in head])
    n_all = len(d)
    ex = {str(c).upper() for c in (cfg.get("exclude_countries") or [])}
    if ex:
        d = d[~d["店铺"].map(code_of_store).isin(ex)].copy()
    n_ex = n_all - len(d)
    d["t"] = pd.to_datetime(d["订购日期"], errors="coerce")
    d = d.dropna(subset=["t"]).copy()
    if d.empty:
        q.add("WARN", "订单导出", "没有可用的订购日期，本次不使用订单导出")
        return None
    d["date"] = d["t"].dt.strftime("%Y-%m-%d")
    ws, we = (x[:10] for x in _spapi_range(week_end, cfg.get("spapi_window_lag_days", 0)))
    fmin, fmax = d["date"].min(), d["date"].max()
    day = lambda x: datetime.strptime(x, "%Y-%m-%d")
    if not (day(fmin) <= day(ws) + timedelta(days=1) and day(fmax) >= day(we) - timedelta(days=1)):
        q.add("WARN", "订单导出", f"订单日期范围 {fmin}~{fmax} 没有覆盖本次窗口 {ws}~{we}，本次不使用，销量/销售额沿用Listing滚动7日口径。"
              f"请重新导出订单：订购日期选 {ws} ~ {we}")
        return None
    n_out = int(((d["date"] < ws) | (d["date"] > we)).sum())
    d = d[(d["date"] >= ws) & (d["date"] <= we)].copy()
    qty, amt = num(d["数量"]).fillna(0), num(d["销售额(Item Price)"]).fillna(0)
    cur = d["订单币种"].astype(str).str.upper() if "订单币种" in d.columns else d["店铺"].map(lambda x: CUR_BY_CODE.get(code_of_store(x), ""))
    rate = cur.map(cfg["fx_to_usd"])
    if rate.isna().any():
        q.add("ERROR", "订单导出币种", f"币种 {sorted(set(cur[rate.isna()]))} 在 fx_to_usd 里没有汇率，这些行销售额按0计")
    valid = d["订单状态"].astype(str).str.lower() != "canceled"
    yes = lambda c: (d[c].astype(str) == "是") if c in d.columns else pd.Series(False, index=d.index)
    d["件数"] = qty.where(valid, 0)
    d["销售额"] = (amt * rate.fillna(0)).where(valid, 0)
    d["促销件数"] = d["件数"].where(yes("是否促销"), 0)
    d["促销销售额"] = d["销售额"].where(yes("是否促销") & valid, 0)      # 促销(如Vine)行有名义Item Price，但没有真实收入
    d["B2B件数"] = d["件数"].where(yes("是否B2B"), 0)
    d["待处理件数"] = d["件数"].where(d["订单状态"].astype(str).str.lower() == "pending", 0)
    d["退款件数"] = d["件数"].where(yes("是否退款"), 0)
    d["换货件数"] = d["件数"].where(yes("换货订单"), 0)
    sm = ["件数", "销售额", "促销件数", "促销销售额", "B2B件数", "待处理件数", "退款件数", "换货件数"]
    items = d.groupby(["店铺", "MSKU"])[sm].sum().reset_index()
    daily = d.groupby(["店铺", "date"])[["件数", "销售额"]].sum().reset_index()
    st = d["订单状态"].value_counts().to_dict()
    q.add("INFO", "订单导出", f"{len(d)}行订单明细，窗口{ws}~{we}(文件实际{fmin}~{fmax})，店铺{sorted(d['店铺'].unique())}；状态行数{st}；"
          f"件数{d['件数'].sum():.0f}、销售额${d['销售额'].sum():.2f}；已剔除排除站点{n_ex}行、窗口外{n_out}行。只读取必要列，买家/地址信息未载入")
    return {"use": True, "items": items, "daily": daily, "stores": set(d["店铺"].unique()), "window": (ws, we)}

def reconcile_orders_sp(orders, q):
    """订单导出 vs SP 销售与流量报告：按店铺逐日核对件数与销售额"""
    if not orders or not orders.get("use") or not SP_DAILY:
        return
    for store, sp in SP_DAILY.items():
        o = orders["daily"][orders["daily"]["店铺"] == store].set_index("date")
        s_ = sp.set_index("date")
        common = sorted(set(o.index) & set(s_.index))
        if not common:
            continue
        ou, su, oa, sa = o.loc[common, "件数"], s_.loc[common, "units"], o.loc[common, "销售额"], s_.loc[common, "sales"]
        rel_u = abs(float(ou.sum() - su.sum())) / max(float(su.sum()), 1e-9)
        rel_a = abs(float(oa.sum() - sa.sum())) / max(float(sa.sum()), 1e-9)
        q.add("OK" if (rel_u <= 0.02 and rel_a <= 0.02) else "WARN", f"对账:订单导出 vs SP每日订购({store})",
              f"重叠{len(common)}天：件数 {ou.sum():.0f} vs {su.sum():.0f}(差{rel_u:.1%}，逐日最大差{int((ou - su).abs().max())}件)；"
              f"销售额 {oa.sum():.2f} vs {sa.sum():.2f}(差{rel_a:.1%})")

def default_week_end(cfg, today=None):
    """自动周结束日：最近一个"已结束、且亚马逊数据已发布"的周六(周六过了 spapi_sqp_lag_days 天)。
    例：周三运行 -> 上周六；周一运行 -> 上周六；周日运行 -> 再上一个周六。"""
    t = today or datetime.now().date()
    d = t - timedelta(days=int(cfg.get("spapi_sqp_lag_days", 2)))
    sat = d - timedelta(days=(d.weekday() - 5) % 7)
    return sat.strftime("%Y-%m-%d")

_WD = "一二三四五六日"

def describe_windows(week_end, cfg):
    """本次各数据源使用的时间窗口(领星数据要选同样的起止日期)。返回 (行列表, 是否对齐)"""
    st_s, st_e = _spapi_range(week_end, cfg.get("spapi_window_lag_days", 0))
    sc_s, sc_e, _ = _sqp_week(week_end)
    wd = lambda d: f"{d}(周{_WD[datetime.strptime(d, '%Y-%m-%d').weekday()]})"
    lines = [f"SP-API 流量/退货：{wd(st_s[:10])} ~ {wd(st_e[:10])}(7天)",
             f"SP-API 搜索漏斗(品牌分析)：{wd(sc_s[:10])} ~ {wd(sc_e[:10])}(周日~周六)",
             "SP-API 库存计划(库龄/仓储/竞品价)：拉取当时的快照，没有日期窗口"]
    return lines, (st_s[:10] == sc_s[:10] and st_e[:10] == sc_e[:10]), (st_s[:10], st_e[:10])

def cmd_window(a, cfg):
    we = a.week_end or default_week_end(cfg)
    lines, aligned, (s0, e0) = describe_windows(we, cfg)
    print(f"周结束日(自动取最近一个数据已发布的周六)：{we}" if not a.week_end else f"周结束日(手动指定)：{we}")
    for l in lines:
        print("  " + l)
    print("\n领星请这样下载：")
    print(f"  · 广告各报表(活动/广告组/推广商品/广告位/关键词/自动投放/用户搜索)：日期选 {s0} ~ {e0}")
    print(f"  · 订单导出：订购日期选 {s0} ~ {e0}(销量/销售额按此窗口计算，必须完整覆盖)")
    print("  · FBA货件、补货建议(父Asin)：快照，与 Listing 同一天导出(断货模拟从该日起算)")
    print("  · Listing / 补货建议 / 成本表：库存是快照，当天导出即可；其中的'7日销量'是滚动窗口(截止导出日)，与上面日期不完全重合，只作参考")
    print(f"  · 放进文件夹 inputs/{we} 后运行：python weekly_pipeline_spapi.py ingest --inputs inputs/{we}")
    if not aligned:
        print("\n提示：周结束日不是周六，流量/退货窗口与搜索漏斗周期不一致；建议周结束日用周六。")

def cmd_ingest(a, cfg):
    folder = rel(a.inputs) if not os.path.isabs(a.inputs) else a.inputs
    files = {k: find_file(folder, p) for k, p in PATTERNS.items()}
    q = Quality()
    if not files["listing"]:
        sys.exit("缺少 Listing 导出文件，无法继续")
    m = re.search(r"(20\d{6})", os.path.basename(files["listing"]))
    _lst_date = None
    if m:
        _lst_date = datetime.strptime(m.group(1), "%Y%m%d").strftime("%Y-%m-%d")
    if a.week_end:
        week_end, we_src = a.week_end, "手动指定"
    elif str(cfg.get("week_end_default", "last_saturday")) == "listing_date" and m:
        week_end, we_src = datetime.strptime(m.group(1), "%Y%m%d").strftime("%Y-%m-%d"), "取自Listing文件名日期"
    else:
        week_end, we_src = default_week_end(cfg), "自动取最近一个数据已发布的周六"
    start_clock()
    init_log(os.path.join(rel(cfg["out_dir"]), f"ingest_{week_end}.log"))
    log(f"开始 ingest：周结束={week_end}({we_src})；输入目录={folder}；日志同时写入 {cfg['out_dir']}/ingest_{week_end}.log")
    _lines, _aligned, _ = describe_windows(week_end, cfg)
    for _l in _lines:
        log("时间窗口 - " + _l)
    if not _aligned:
        log("[提示] 周结束日不是周六：流量/退货窗口与搜索漏斗周期不一致；领星数据也请按 SP-API 窗口的起止日期下载")
    else:
        log("领星广告报表请选择与 SP-API 流量窗口相同的起止日期(见上)")
    if m:
        _ld = datetime.strptime(m.group(1), "%Y%m%d").strftime("%Y-%m-%d")
        if _ld != week_end and not find_file(folder, "订单导出"):
            q.add("INFO", "Listing导出日≠周结束日",
                  f"Listing 导出日 {_ld}，周结束日 {week_end}：Listing 的7/14/30日销量是滚动窗口(截止导出日附近)，与 SP/广告窗口(周日~周六)不重合，"
                  "对账差异属正常；周度销量请以 销量_SP/销售额_SP(SP口径，已与广告窗口对齐)为准，TACoS_SP、广告销售占比_SP、库存天数_SP是对齐口径；TACoS、库存天数_7(用领星销量7/销售额7)只作参考")
    for k, v in files.items():
        if not v:
            q.add("WARN" if k != "product" else "ERROR", f"缺少文件:{k}", f"未找到文件名含'{PATTERNS[k]}'的Excel")

    log("读取并清洗 Listing ...")
    L = read_listing(files["listing"], cfg, week_end, q)
    ad = {k: read_ad(files[k], cfg, q, k) for k in ["campaign", "group", "product", "placement", "keyword", "auto", "search"] if files[k]}

    log(f"广告报表读取完成({len(ad)}份)，Listing {len(L)}行；读取成本表 ...")
    cost_path = find_file(folder, COST_PATTERN)
    cache = rel("data/cost_latest.xlsx")
    if cost_path:
        os.makedirs(os.path.dirname(cache), exist_ok=True); shutil.copy(cost_path, cache)
    elif os.path.exists(cache):
        cost_path = cache
        q.add("INFO", "成本文件", "本周未上传，沿用上次缓存的成本表")
    cost = read_cost(cost_path, cfg, q)

    # ---- SP-API：自动拉取为底，当周 spapi_parent_weekly*.csv 中的非空值覆盖自动值
    sp, sqp, sph = None, None, None
    spf = glob.glob(os.path.join(folder, "spapi_parent_weekly*.csv"))
    if spf:
        try:
            manual_path = max(spf, key=os.path.getmtime)
            sp = pd.read_csv(manual_path, encoding="utf-8-sig")
            q.add("INFO", "SP-API数据", f"发现手工 CSV：{os.path.basename(manual_path)}")
        except Exception as e:
            q.add("ERROR", "SP-API CSV", f"读取失败：{e}")
    if getattr(a, "refresh_spapi", False):
        cfg["spapi_refresh"] = True
    if getattr(a, "no_spapi", False):
        cfg["spapi_enabled"] = False
    if getattr(a, "spapi_cache_only", False):
        cfg["spapi_cache_only"] = True
    if getattr(a, "sp_wait", None) is not None:
        cfg["spapi_report_timeout_seconds"] = int(a.sp_wait)
    if cfg.get("spapi_enabled", True):
        try:
            auto_sp, sqp, sph = fetch_spapi_parent_weekly(L, week_end, cfg, q)
            if auto_sp is not None and len(auto_sp):
                sp = auto_sp if sp is None else merge_spapi_manual_over_auto(auto_sp, sp)
        except FileNotFoundError as e:
            q.add("WARN", "SP-API凭证", str(e))
        except Exception as e:
            q.add("ERROR", "SP-API自动拉取", f"自动拉取失败：{e}")
    else:
        q.add("INFO", "SP-API自动拉取", "已关闭(config.spapi_enabled=false 或 --no-spapi)，跳过在线拉取")

    # ---- 广告 -> 父体映射
    prod_m, gmap, cmap = None, None, None
    if "product" in ad:
        cm = L[["店铺", "MSKU", "父ASIN", "款"]]
        prod_m = ad["product"].merge(cm, on=["店铺", "MSKU"], how="left")
        un = prod_m[prod_m["父ASIN"].isna()]
        q.add("ERROR" if len(un) else "OK", "推广商品->Listing匹配", f"{len(prod_m) - len(un)}/{len(prod_m)} 行匹配成功；未匹配MSKU：{un['MSKU'].unique()[:10].tolist()}")
        gmap, gmulti = dominant(prod_m, ["店铺", "广告活动", "广告组"])
        cmap, _ = dominant(prod_m, ["店铺", "广告活动"])
        q.add("WARN" if len(gmulti) else "OK", "广告组含多个父体", f"{len(gmulti)} 个广告组对应多个父体(按花费最多的归属)")
        if "campaign" in ad:
            d1, d2 = prod_m["花费"].sum(), ad["campaign"]["花费"].sum()
            q.add("WARN" if abs(d1 - d2) / max(d2, 1e-9) > 0.03 else "OK", "花费对账:推广商品 vs 广告活动", f"{d1:.2f} vs {d2:.2f} (差 {abs(d1-d2)/max(d2,1e-9):.1%})")
        cov = L[L["店铺"].isin(prod_m["店铺"].unique())]
        d3, d4 = prod_m["花费"].sum(), cov[cov["状态"] == "在售"]["7日广告费"].sum()
        _aligned_l = bool(_lst_date) and _lst_date == week_end
        q.add(("WARN" if abs(d3 - d4) / max(d3, 1e-9) > 0.10 else "OK") if _aligned_l else "INFO", "花费对账:广告报表 vs 领星Listing 7日广告费",
              f"{d3:.2f} vs {d4:.2f} (差 {abs(d3-d4)/max(d3,1e-9):.1%})；" + ("差异大时先查统计窗口/口径" if _aligned_l else
              "Listing 的7日广告费是滚动窗口(截止导出日)，与广告报表窗口不重合，仅供参考；广告花费以广告报表(各报表合计一致)为准"))
        nos = L[(~L["店铺"].isin(prod_m["店铺"].unique())) & (L["7日销售额"].fillna(0) > 0)]["店铺"].unique().tolist()
        q.add("INFO" if nos else "OK", "无广告报表的店铺-站点", f"{nos}(有销售但本周无广告数据，其广告列为空)")
    q.add("INFO", "归因回补", f"最近{cfg['attribution_lag_days']}天的广告订单可能未回补完整，最新周ACoS易偏高；请结合前几周判断")

    orders = None
    opath = find_file(folder, "订单导出")
    if opath:
        log("读取订单导出(只读必要列) ...")
        orders = read_orders(opath, cfg, q, week_end)
    else:
        q.add("INFO", "订单导出", "未提供文件名含'订单导出'的订单文件：销量/销售额沿用领星Listing的滚动7日口径(与周窗口不重合)；可用SP口径(销量_SP等)做对齐参考")
    reconcile_orders_sp(orders, q)
    ship = None
    shpath = find_file(folder, "FBA货件")
    if shpath:
        log("读取FBA货件(货件详情) ...")
        ship = read_shipments(shpath, cfg, q, week_end)
    else:
        q.add("INFO", "FBA货件", "未提供文件名含'FBA货件'的货件导出：没有在途货件的到仓时间，断货风险只能用含在途天数粗判(默认在途会按时到)")
    replen = None
    rpath = find_file(folder, "补货建议")
    if rpath:
        log("读取补货建议(本地仓库存/待交付) ...")
        replen = read_replenish(rpath, cfg, q, week_end)
    else:
        q.add("INFO", "补货建议", "未提供文件名含'补货建议'的文件：没有本地仓库存和工厂待交付，断货模拟只含FBA现有+已发货/入库中的货件，补货量无法判断")
    log("拼接父体周宽表 ...")
    P = build_parent(L, prod_m, cost, sp, week_end, cfg, q, orders, ship, replen)
    log(f"父体周宽表完成：{len(P)}行；整理广告明细 ...")

    # ---- 明细表
    D = {}
    if SHIP_DETAIL.get("df") is not None and len(SHIP_DETAIL["df"]):
        sd_ = SHIP_DETAIL["df"].merge(P[["店铺", "父ASIN", "款"]].drop_duplicates(), on=["店铺", "父ASIN"], how="left")
        for c_ in ("发货日", "窗口起", "窗口止"):
            sd_[c_] = sd_[c_].astype(str).replace({"NaT": "", "None": "", "nan": ""})
        D["weekly_shipment"] = sd_.groupby(["店铺", "款", "父ASIN", "货件单号", "状态", "运输方式", "物流中心编码", "发货日", "窗口起", "窗口止", "计入供给"], dropna=False)[["已发货", "签收量", "件数"]].sum().reset_index().rename(columns={"件数": "待收件数"})
    if SKU_SUPPLY.get("df") is not None and len(SKU_SUPPLY["df"]):
        D["weekly_sku_supply"] = SKU_SUPPLY["df"].merge(P[["店铺", "父ASIN", "款"]].drop_duplicates(), on=["店铺", "父ASIN"], how="left")
    if REPLEN_PO.get("df") is not None and len(REPLEN_PO["df"]) and "待交付" in P.columns:
        rp_ = REPLEN_PO["df"].merge(P[["店铺", "父ASIN", "款"]].drop_duplicates(), on=["店铺", "父ASIN"], how="left")
        snap_r = replen["snap"] if replen else None
        rp_["已晚天数"] = [max(0, (snap_r - a_).days) if (snap_r and a_ is not None and not pd.isna(a_)) else 0 for a_ in rp_["预计到货"]]
        for c_ in ("下单", "预计到货", "预计可售"):
            rp_[c_] = rp_[c_].astype(str).replace({"NaT": "", "None": "", "nan": ""})
        D["weekly_po"] = rp_.groupby(["店铺", "款", "父ASIN", "单据号", "状态", "仓库", "下单", "预计到货", "预计可售", "已晚天数"], dropna=False)[["数量"]].sum().reset_index()
    if sqp is not None and len(sqp):
        sqp = sqp.merge(P[["店铺", "父ASIN", "款"]].drop_duplicates(), on=["店铺", "父ASIN"], how="left")
        D["weekly_sqp"] = sqp[["店铺", "款", "父ASIN", "搜索词", "搜索量", "总曝光", "我方曝光", "曝光份额", "总点击", "我方点击", "点击份额",
                               "总加购", "我方加购", "加购份额", "总购买", "我方购买", "购买份额", "周期起", "周期止"]]
    def attach(df, mp, keys):
        if mp is None:
            df["父ASIN"] = "未映射"; df["款"] = "未映射"; return df
        r = df.merge(mp, on=keys, how="left")
        r["父ASIN"] = r["父ASIN"].fillna("未映射"); r["款"] = r["款"].fillna("未映射")
        return r
    if "campaign" in ad:
        c = derive(attach(ad["campaign"].copy(), cmap, ["店铺", "广告活动"]))
        c["日均花费"] = c["花费"] / 7
        c["预算使用率"] = c["日均花费"] / c["预算"].where(c["预算"] > 0)
        D["weekly_campaign"] = c[["店铺", "款", "父ASIN", "广告活动", "有效状态", "预算", "日均花费", "预算使用率", "曝光", "点击", "花费", "广告销售", "广告订单", "ACoS", "CVR", "CPC", "IS"]]
    if "placement" in ad:
        p = attach(ad["placement"].copy(), cmap, ["店铺", "广告活动"])
        D["weekly_placement"] = p[["店铺", "款", "父ASIN", "广告活动", "广告位", "曝光", "点击", "花费", "广告销售", "广告订单"]]
    if "keyword" in ad:
        k = derive(attach(ad["keyword"].copy(), gmap, ["店铺", "广告活动", "广告组"]))
        D["weekly_keyword"] = k[["店铺", "款", "父ASIN", "广告活动", "广告组", "关键词", "匹配方式", "有效状态", "竞价", "曝光", "点击", "花费", "广告销售", "广告订单", "ACoS", "CVR"]]
    if "auto" in ad:
        t = derive(attach(ad["auto"].copy(), gmap, ["店铺", "广告活动", "广告组"]))
        D["weekly_auto"] = t[["店铺", "款", "父ASIN", "广告活动", "广告组", "投放", "有效状态", "竞价", "默认竞价", "曝光", "点击", "花费", "广告销售", "广告订单", "ACoS", "CVR"]]
    if "search" in ad:
        s = attach(ad["search"].copy(), gmap, ["店铺", "广告活动", "广告组"])
        s["来源"] = np.where(s["关键词"].isna(), "自动", "手动")
        s["词类型"] = np.where(s["用户搜索词"].astype(str).str.lower().str.match(r"^b0[a-z0-9]{8}$"), "ASIN", "关键词")
        kws = set()
        if "weekly_keyword" in D:
            kk = D["weekly_keyword"]
            kws = set(zip(kk["店铺"], kk["父ASIN"], kk["关键词"].astype(str).str.lower()))
        s["已投放为手动词"] = [((a_, b_, str(c_).lower()) in kws) or (o_ == "手动") for a_, b_, c_, o_ in zip(s["店铺"], s["父ASIN"], s["用户搜索词"], s["来源"])]
        s = derive(s)
        D["weekly_searchterm"] = s[["店铺", "款", "父ASIN", "广告活动", "广告组", "来源", "关键词", "匹配方式", "投放", "用户搜索词", "词类型", "已投放为手动词", "曝光", "点击", "花费", "广告销售", "广告订单", "ACoS", "CVR"]]
        um = int((s["父ASIN"] == "未映射").sum())
        q.add("WARN" if um else "OK", "搜索词->父体归属", f"{um}/{len(s)} 行搜索词无法归属到父体")

    # ---- 落库 + 导出
    log("写入数据库并导出 Excel/CSV ...")
    Q = q.df()
    db = rel(cfg["db_path"]); os.makedirs(os.path.dirname(db), exist_ok=True)
    con = sqlite3.connect(db)
    upsert(con, "weekly_parent", P, week_end)
    for t, df in D.items():
        upsert(con, t, df, week_end)
    upsert(con, "weekly_quality", Q, week_end)
    if sph is not None and len(sph):
        sph = sph.merge(P[["店铺", "父ASIN", "款"]].drop_duplicates(), on=["店铺", "父ASIN"], how="left")
        sph = sph[["店铺", "款", "父ASIN", "窗口起", "窗口结束", "覆盖天数", "订购件数", "订购销售额", "会话", "页面浏览", "单位会话率", "BuyBox占比"]]
        upsert_windows(con, "weekly_sp_sales", sph, "窗口结束")
    con.close()
    out = rel(cfg["out_dir"]); os.makedirs(out, exist_ok=True)
    P.to_csv(os.path.join(out, f"weekly_parent_{week_end}.csv"), index=False, encoding="utf-8-sig")
    sheets = {"父体周宽表": P, "数据质量": Q, "字段说明": FIELD_DOC}
    fg_ = FREIGHT_GAP.get("df")
    if fg_ is not None and len(fg_):
        fg_ = fg_.drop_duplicates(["店铺", "SKU", "国家名称"]).sort_values(["状态", "店铺", "款", "SKU"])
        fg_.to_csv(os.path.join(out, f"头程缺失清单_{week_end}.csv"), index=False, encoding="utf-8-sig")
        sheets["头程缺失清单"] = fg_
        log(f"头程缺失清单：{len(fg_)}个在售SKU×国家(缺失补不上 {int((fg_['状态'] == '缺失(补不上)').sum())}、已估算 {int((fg_['状态'] != '缺失(补不上)').sum())})，已写入 out/头程缺失清单_{week_end}.csv")
    if sph is not None and len(sph):
        sheets["SP周度销量"] = sph
    names = {"weekly_campaign": "活动", "weekly_placement": "广告位", "weekly_keyword": "关键词", "weekly_auto": "自动投放", "weekly_searchterm": "搜索词", "weekly_sqp": "SQP搜索词", "weekly_po": "待交付PO", "weekly_shipment": "在途货件明细", "weekly_sku_supply": "尺码级补货"}
    for t, df in D.items():
        sheets[names[t]] = df
    save_xlsx(os.path.join(out, f"weekly_{week_end}.xlsx"), sheets)
    log(f"全部完成：周结束={week_end} 父体行={len(P)} 明细表={list(D)}；总用时{_fmt_sec(time.time() - _T0)}")
    print(Q[Q["级别"] != "OK"].to_string(index=False))

# --------------------------------------------------------------------------- pack
def _monthly_prompt(base):
    head = base.partition("# 输出格式")[0]
    head = head.replace("请基于数据给出**本周可以直接执行**的运营建议。", "请基于数据做**月度复盘，并给出下月可直接执行**的运营建议；所有结论以'窗口汇总'(N周合计，比率按合计重算)为准，逐周明细只用来判断趋势。")
    tail = """# 输出格式(月报)
A. 月度总览：表格，列为 指标|本窗口合计或均值|相比前一窗口(没有则写'无')|一句话解读(销量、实收销售额、广告花费、ACoS、TACoS、库存天数、退货率)
B. 款级分层：按 实收销售额 与 估算利润 把父体分为 A(核心)/B(培育)/C(观察或收缩)；每个父体一行：依据数字|分层|下月策略(具体到预算/竞价/补货/定价方向，含数值)
C. 广告结构：活动预算、关键词、自动投放、否定词与收割词的调整清单(对象|动作|数值|依据)
D. 库存与补货：库存天数、含在途天数、到仓时间与断货模拟(断货天数_保守/含待交付/全供给、首次断货)、本地仓库存与待交付、库龄(90+/181+)、在途；列出要补货、要清货、要观望的父体及件数或天数
E. 问题款：评分、退货原因、Buy Box、价格竞争力(只引用数据里有的)
F. 需要人工核实的数据问题；需要补充的数据
G. 下月目标与验证指标：每个目标给 目标值|验证指标|检查时间
"""
    return head + tail

PROMPT = """# 角色与任务
你是亚马逊服饰(神职服装)店铺的运营分析师。下面是按"店铺×父体"汇总的最近N周数据包。请基于数据给出**本周可以直接执行**的运营建议。所有金额已折算为USD，比率均为"先汇总再相除"。
周报分两部分：第一部分"数据报告"系统地写清各板块数据与结论；第二部分"执行建议"单独给出非常具体、可直接执行的动作。

# 第一原则：只用数据包里的数据(最重要，高于其它所有规则)
- 报告里的每个数字，都必须能在数据包中找到(注明出自哪一节/哪个字段)，或由数据包数字直接算出(写出算式)。
- 数据包没有的数据一律写"无数据"，并在"数据限制与缺口"里说明缺哪份报表、影响哪个结论。禁止估算、禁止用行业经验值、禁止编造对比期。
  常见的"没有"：利润报表/结算数据、子体(尺码/颜色/MSKU)级销量与退货、30天环比、搜索排名、竞品销量、今日数据、广告后台以外的归集口径。
- 数据粒度是 店铺×父体；不得对具体尺码/颜色/MSKU 下结论(除非数据包里出现了该子体的数据)。
- 商品属性(男装/女装、品类、袖长)只以"1c 产品档案"为准；严禁根据搜索词、关键词或广告活动名推断商品性别。
  搜索词/关键词表的"性别匹配"列：不符=词的性别与商品相反(如女装连衣裙被 "for men" 触发)，是优先的否定候选、也不能当收割词；中性=词里没写性别。
- 库里只有一周时(见 1b 的说明)，不得写环比、趋势、"改善/恶化"；只能描述本周现状。
- 头程：'头程估算方式'非空的父体，头程是估算值(同款同尺码其它颜色，或 单品毛重×同系列每公斤头程——同系列实测每公斤头程基本一致)，可以用来算单件毛利/盈亏ACoS/利润，但引用时要注明"头程为估算"。

# 硬性规则(违反任何一条，该建议作废)
1. 执行动作只能来自"5. 候选动作"(见 八 节格式)；九、十 节及正文中的每条判断必须包含：对象(用表中的店铺/款/广告活动/关键词原名) | 现状(引用具体数字和周次) | 动作(必须给具体数值：竞价A→B、预算A→B、否定哪个词、补货多少件/多少天) | 依据(推理链，一步一步) | 预期结果与下周验证指标(看哪个数、到多少算对) | 置信度(高/中/低，按样本量) | 风险或前置条件。
2. 样本量门槛：父体或对象的广告订单<5、或点击<30时，不得判定ACoS好坏，只能给"观察"或小幅试探(竞价调整幅度≤10%)，并写明还需要多少点击/订单才能定论。
3. 库存优先：含在途天数<21或库存状态=紧张的父体，不建议加投；断货天数_保守>0 的父体同样不建议加投或做促销拉销量，应先给补货(空运/加急)或控速(降竞价/降预算/提价)的具体方案；库存状态=偏多或有冗余/滞销的父体，不得把"加广告"当作解决办法，应给出清货(优惠券/秒杀/降价，价格不低于 保本价)、减少/延后待交付PO、停止采购滞销尺码的方案(用第5节 冗余清货/削减PO/本地滞销 候选)；同一父体可能一部分尺码缺货、另一部分冗余，要分尺码说。
4. 归因未成熟：最近几天广告订单未回补完整，最新一周ACoS可能偏高。必须结合前几周判断是"趋势"(连续两周同向且幅度超过正常波动)还是"噪音"。仅有1周数据时明确声明无法判断趋势。
5. 缺失数据不推测：SP_开头的字段为空时，不得对流量、转化、Buy Box、退货、库龄、竞品价、SQP下结论；需要时写"缺XX数据，无法判断"，并写清需要哪份数据。禁止用行业经验数字代替表内数据。
6. 盈亏线口径：'盈亏ACoS'为空时只能引用'盈亏ACoS上限_未含头程'，并说明实际盈亏线低于它。采购成本缺失/占位的款不得计算利润；头程为估算的款可以计算，但要注明。
7. 禁止泛泛而谈(如"优化Listing""关注竞品""加强推广")，除非同时指出具体父体/ASIN、对应的数据依据和具体动作。
8. 不确定或数据自相矛盾(参见'数据质量')的地方，单独放入"十一、数据限制与缺口"，不要强行给结论。
9. 数据口径(必须遵守)：
   - 销量来源=订单导出 时，销量7/销售额7 是窗口(周日~周六)内的订单数据，与SP订购件数的对账结果见'数据质量'('对账:订单导出 vs SP每日订购')：OK 时可直接用，WARN 时写入"十一、数据限制与缺口"；销量来源=Listing滚动7日 时它是滚动窗口，周度销量/销售额以 销量_SP/销售额_SP 为准，两者冲突写入"十一、数据限制与缺口"。
   - 订单均价/实收均价已扣除换货单与促销单；SP_流量覆盖天数<7 表示亚马逊最近几天未出数，SP_会话/页面浏览/订购件数/订购销售额只是N天合计，必须折算日均再与7日数据比较(SP_退货率、广告点击占会话已折算)。
   - 退货：退货在收货处理后才入报告，单周波动大；订购件数<20 的父体不得对退货率下结论，退货率结论至少看两周以上；退货原因只在该原因件数>=3时引用。
   - 若有搜索漏斗数据(默认已关闭)：搜索漏斗(SP_搜索*)是父体在搜索结果页的整体曝光→点击→加购→购买(自然+广告)，周期周日~周六，不能归因到具体搜索词，其转化率不等于广告转化率。
   - 库龄/仓储/竞品价是拉取当时的快照；SP_BuyBox占比<90% 时，先指出Buy Box问题，再谈广告。
   - 若有SQP数据：每个ASIN只含Top100查询，只能用于具体搜索词的市场搜索量和我方份额，不得当全量。
   - 搜索词表的'已投放为手动词'：来源=手动 的词已由现有关键词(含近似变体)触发，不得再建议当新关键词添加；只有 来源=自动 且 已投放为手动词=否 的词才是收割候选。
   - 货件ETA：到仓日来自领星FBA货件的送达时段(卖家中心登记值，海运常延误，不等于真实到仓日)；有货件数据时必须用 断货天数_乐观/保守、首次断货_天后、海运/空运最晚发货_天后 判断缺货风险：海运最晚发货_天后<0 表示海运已来不及，只能空运或控速；海运/空运最晚下单_天后(=最晚发货−采购交期)只在没有补货建议数据时使用(不含待交付和本地仓，偏保守)。有补货建议数据时，按供给链条判断(3e表)：先看 断货天数_保守(仅FBA+在途)，再看 断货天数_含待交付(加工厂待交付，按领星预计可售日，已晚到的PO已顺延)，再看 断货天数_全供给_空运/海运(再加本地仓库存今天发出)；只有"全供给"仍断货，才需要新采购，此时用 新采购最晚下单_海运/空运_天后(<0=即使现在向工厂下单也来不及，空=60天内不需要新采购)；全供给不断货但保守断货，建议是"发本地库存"：避免断货的最少件数用 本地仓最少空运/海运件数_避免断货；本周海运实际发货量用 建议海运发货件数(已含发货周期与安全库存，覆盖到 海运发货_覆盖至第N天)，日均一律用 日均_预测(7/14/30/60天加权)，日均7只描述本周；有尺码级结果(3e3、尺码_* 列)时，缺货判断和空运/海运/补货件数一律以尺码级为准(亚马逊按子体判断缺货，已到仓未上架不算有货)，父体级只作参考；本地仓对应尺码不够发的(尺码_本地仓不足件数)要写成催PO/工厂直发/新采购；空运件数用 建议空运件数(今天空运、撑到海运可售日)；空运可售前无法避免断货天数>0 的那几天任何发货都补不上，只能控速(降竞价/降预算/提价)；海运发货_本地仓不足件数(已先扣空运)>0 时差额需新采购或控速。本地可用/待交付是US/UK等多站点共享的同一批货，已只计入主站，不得重复计入；本地可用 是全部颜色尺码的合计，不等于缺货尺码能发的量：有 尺码_本地仓不足件数>0 时，不得写"本地仓库存充足/可直接调拨补足/无需新采购"，本地仓能发多少只看 空运_本地可发件数/海运_本地可发件数(3e3逐尺码)；工厂待交付已过预计到货日的PO(见数据质量)，其交期不可信，须列入"十一、数据限制与缺口"。没有货件或补货建议数据的父体才写"缺ETA/缺待交付与本地仓，补货量未知"，不得假设在途按时到。
   - 表内"在途"已不含发往停售/未在售子体的件数(见 在途_发往非在售子体件数)，这部分货到仓也卖不了，需先激活Listing；FBA货件在途超过历史最长/入库滞留的货件，其到仓日不可信，须在"十一、数据限制与缺口"里列出。
10. 促销单位(订单促销件数，如Vine免费样品)计入销量与销售额(Item Price)，但没有真实收入：评估均价、TACoS、转化率、盈亏和利润时，必须用 订单实收销售额/订单实收均价/TACoS_实收；促销占比高的父体(订单促销占比>30%)其销量不代表自然需求，不得据此加投或补货，需在'十一、数据限制与缺口'里说明。
11. 多周/月度：所有比率(ACoS、TACoS、CVR、退货率)用"窗口汇总"里按合计重算的值，不得对各周比率求平均；逐周明细只用来判断趋势是否连续。
12. 父体流量评估(用"3a 父体流量 vs 广告"表)：
   - 流量规模：会话7，与上周比(会话变化)，与店内其他父体比(会话相对店内中位)；会话<100 或只有单周数据时，流量结论只能是"观察"。
   - 流量结构：广告点击占会话=广告依赖度；自然会话估算=会话−广告点击(粗略)。点击不等于会话，只看相对大小与趋势，不得当作精确占比。
   - 转化：全站转化率与 转化率相对店内中位 比较；广告CVR 与 自然转化率估算 只能看相对高低(口径不同：点击≠会话，广告含7天归因与光环)；广告订购占比>100% 时不得推算自然转化。
   - 判断顺序：① 会话下降且广告点击也下降 → 先查广告曝光/竞价/预算使用率/IS；② 会话稳定但转化率下降或明显低于店内中位 → 查价格竞争力、评分/评论、Buy Box、退货原因、Listing，而不是加广告；③ 广告依赖度高(点击占会话>50%)且自然会话很低 → 说明自然流量弱，但没有搜索曝光数据，无法判断是排名还是需求问题，须在"十一、数据限制与缺口"里写明(可开启搜索漏斗)。
   - 自然排名代理：没有搜索曝光数据时，用 大类排名变化(数值越小越好；变化<0=排名上升)、自然会话估算的周变化 判断自然侧是升是降；大类排名是导出日快照、受全店销量影响，只能看趋势，不能当精确排名，也不能说明是哪个搜索词的问题。
   - 广告预算与流量的关系：用 每会话广告成本 与 每会话销售额 比较，不得只看ACoS。
13. 领星补货建议(3e2)是领星按自己的备货时长(67/75天)和日均口径算的，只作对照；与 3e 的模拟冲突时，以 3e 为准(订单口径日均+货件到仓日+待交付+本地仓)，并在"十一、数据限制与缺口"里说明差异原因(日均口径、到货日、备货时长)；不得直接照抄领星的建议采购量。
14. 口径一致(常见错误)：
   - "单件毛利"是数据包字段(未扣广告)；用 估算利润÷销量 算出的叫"单件净利(含广告)"，不得混称。
   - 库存状态按字段原值引用(紧张/正常/偏多/新品样本不足/无销量/未在售)，不得把"紧张"的父体归入"偏多"；同一父体可以既有尺码缺货又有冗余尺码，要分开写。
   - 仓储费：冗余相关用 冗余_预估月仓储费(只算冗余尺码)，全父体用 SP_预估仓储费，同一段里不得混用。
   - 断货天数/缺货件数越小越好，验证指标写"降到X"，不得写"回升"。
   - 父体转化率低于店内中位，不影响对 ACoS 远低于盈亏线、转化好的单个关键词加价；正文如写"不应加投"，只指父体整体加预算。
   - 引用"合计/共N件"时必须是数据包里的字段或能写出算式的加总；写不出算式就不要写合计。
   - 有 尺码_本地仓不足件数>0 的父体，不得写"本地仓库存充足/可直接调拨避免断货/无需新采购"。

# 输出格式
全文用 Markdown。第一行：# 亚马逊周报 <周结束日>；第二行写统计窗口与"金额USD"。每个板块标题后先给一句结论并标 🔴(严重)/🟡(关注)/🟢(良好)，再给数据(表格优先)，最后写"数据依据：数据包第X节/字段"。

## 第一部分 数据报告
## 一、执行摘要：3~5 条，每条一句结论 + 关键数字 + 🔴🟡🟢
## 二、分析口径：对象(店铺/账号)、窗口(周日~周六，快照日)、数据来源(订单导出/广告报表/SP-API/领星Listing/FBA货件/补货建议)、关键定义(实收销售额、TACoS、估算利润、断货模拟)
## 三、核心指标总览：用 1b 的店铺与全店汇总；有上周时给 本周|上周|环比，没有就写"仅一周数据，无环比"
## 四、本期最异常指标：只选一个最异常的数字，说明为什么异常、口径是否可靠、对结论的影响
## 五、板块分析
### 5.1 销售与利润：销量、销售额、实收销售额、促销(Vine)影响、单件毛利、盈亏ACoS、估算利润(注明头程是否估算、哪些款不能算)
### 5.2 广告：花费/ACoS/TACoS/CVR/CPC，按父体与广告活动；广告位、关键词、搜索词(否定候选/收割候选)
### 5.3 流量与转化：会话、全站转化率、广告点击占会话、自然会话估算、Buy Box
### 5.4 退货与退款：退货件数、退货率、退货原因(按规则只引用件数>=3的原因)
### 5.5 库存与补货：分"缺货"与"冗余/滞销"两块写。缺货：断货模拟、尺码级缺货(3e3)、在途与到仓、工厂待交付、空运/海运件数；冗余/滞销(3e4)：FBA冗余件数、总冗余、可削减PO、滞销SKU与件数(FBA/本地仓)、库龄91+/181+、预估月仓储费、亚马逊冗余与建议价 vs 保本价；新SKU观察数只说明，不当滞销
### 5.6 价格与竞争：我方实际售价(促销价优先)、促销中SKU数、最低促销价 vs 保本价、竞品最低价、价格高于竞品比例(以实际售价算；最低价常是我方促销价，比例<=0 不得写成"比竞品贵")(无数据写无数据)
## 六、板块交叉对比：表格 交叉信号|本期表现|验证结果(成立/部分成立/不成立/无法验证)|根因
## 七、最该关注的3个问题与最该抓住的3个机会：每条带数字

## 第二部分 执行建议
## 八、执行建议清单：只能从数据包"5. 候选动作"里选(最多12条)，本节只输出一个 ```json 代码块，不要再写表格(程序校验后自动生成本节表格)：
```json
{"执行清单": [{"id": "候选ID", "优先级": "P0", "建议值": 数字, "理由": "40字以内：为什么选它、为什么是这个优先级；置信度高/中/低。候选的依据程序会自动列出，不要复述数字", "验证指标": "下周看哪个数、到多少算对"}],
 "人工事项": [{"对象": "店铺/款/原名", "动作": "具体动作和数值", "理由": "引用数据"}]}
```
   - id 只能用候选表里 可选=是 的；可选=否 的是规则阻止的(如断货父体加投)，选了会被程序拦截。
   - 建议值必须在该候选的 允许范围 内；不想改就照抄候选的建议值；没有建议值的候选(催交PO、否定词)可以不写建议值。
   - 优先级默认用候选的 规则优先级，最多上下调一级；同一父体不能既控速又加投，同一对象不能既加又减。
   - 候选里没有的动作(调价、清货、Listing修改、激活子体、补充数据等)放进"人工事项"，程序不校验、不自动执行。
## 九、暂不动作的对象及原因(看起来该动但证据不足、或候选被规则阻止的，说明原因和什么条件下再动)
## 十、执行前需要人工核对的事项(针对八里执行方式=人工/API(需确认)的动作：按尺码核对库存结构、确认PO真实交期、否词前复核搜索词等；不要新增动作)
## 十一、数据限制与缺口(缺哪些数据、哪些结论因此无法下、需要补哪份报表哪个字段)
## 十二、相对上周的判断修正(有上周执行清单时逐条评估；没有写"无")
"""

def fmt(v, c):
    if v is None or (isinstance(v, (float, np.floating)) and np.isnan(v)):
        return ""
    if isinstance(v, (bool, np.bool_)):
        return "是" if v else "否"
    if isinstance(v, (int, np.integer)):
        return str(v)
    if isinstance(v, (float, np.floating)):
        if c in PCT_COLS:
            return f"{v * 100:.1f}%"
        return f"{v:.0f}" if abs(v) >= 100 else (f"{v:.2f}".rstrip("0").rstrip(".") or "0")
    return str(v).replace("|", "/")

def md_table(df, cols=None):
    if df is None or len(df) == 0:
        return "(无数据)\n"
    df = df[cols] if cols else df
    lines = ["| " + " | ".join(map(str, df.columns)) + " |", "|" + "|".join(["---"] * len(df.columns)) + "|"]
    for _, r in df.iterrows():
        lines.append("| " + " | ".join(fmt(r[c], c) for c in df.columns) + " |")
    return "\n".join(lines) + "\n"

def store_summary(W):
    """某一周的父体行 -> 店铺与全店汇总(指标为行，店铺+全店为列)。比率先汇总再相除；缺数据的指标留空而不是0"""
    def agg(d):
        s_ = lambda c: d[c].sum(min_count=1) if c in d.columns else np.nan
        div = lambda a, b: a / b if (pd.notna(a) and pd.notna(b) and b) else np.nan
        sold = d[d["销量7"].fillna(0) > 0] if "销量7" in d.columns else d
        prof_ok = int(sold["估算7日利润_含头程"].notna().sum()) if "估算7日利润_含头程" in sold.columns else 0
        r = {"父体数(有销量/全部)": f"{len(sold)}/{len(d)}", "销量(件)": s_("销量7"), "销售额": s_("销售额7"), "实收销售额": s_("订单实收销售额"),
             "广告花费": s_("广告花费"), "广告销售额": s_("广告销售"), "广告订单": s_("广告订单"), "点击": s_("点击"), "曝光": s_("曝光")}
        r["ACoS"] = div(r["广告花费"], r["广告销售额"]); r["TACoS"] = div(r["广告花费"], r["销售额"])
        r["TACoS_实收"] = div(r["广告花费"], r["实收销售额"]); r["CVR"] = div(r["广告订单"], r["点击"])
        r["广告销售占比"] = div(r["广告销售额"], r["销售额"])
        r["估算利润(含头程)"] = s_("估算7日利润_含头程")
        r["利润可算父体(有销量)"] = f"{prof_ok}/{len(sold)}"
        r["会话"] = s_("会话7"); r["全站转化率"] = div(s_("SP_订购件数"), s_("SP_会话"))
        r["退货件数(SP)"] = s_("SP_退货件数"); r["退货率(件)"] = div(r["退货件数(SP)"], r["销量(件)"])
        r["订单促销件数"] = s_("订单促销件数"); r["订单退款件数"] = s_("订单退款件数")
        r["FBA可售"] = s_("FBA可售"); r["在途"] = s_("在途"); r["本地可用(全部颜色尺码)"] = s_("本地可用"); r["待交付"] = s_("待交付")
        r["库存天数(FBA可售÷日均_预测)"] = div(r["FBA可售"], s_("日均_预测")) if "日均_预测" in d.columns else div(r["FBA可售"], (r["销量(件)"] or np.nan) / 7)
        if "尺码_缺货件数_含待交付" in d.columns:
            r["尺码缺货件数(60天,含待交付)"] = s_("尺码_缺货件数_含待交付"); r["尺码本地仓不足件数"] = s_("尺码_本地仓不足件数")
            r["FBA冗余件数(超90天销量)"] = s_("冗余_FBA件数"); r["冗余尺码月仓储费"] = s_("冗余_预估月仓储费")
            r["滞销件数(FBA/本地)"] = f"{s_('滞销_FBA件数') or 0:.0f}/{s_('滞销_本地件数') or 0:.0f}"
        r["库龄90天以上件数"] = s_("SP_库龄90天以上件数"); r["库龄181天以上件数"] = s_("SP_库龄181天以上件数")
        r["下月预估仓储费(全部库存)"] = s_("SP_预估仓储费")
        r["保守断货父体数"] = int((d["断货天数_保守"].fillna(0) > 0).sum()) if "断货天数_保守" in d.columns else np.nan
        return r
    cols = {st_: agg(g_) for st_, g_ in W.groupby("店铺")}
    cols["全店"] = agg(W)
    return pd.DataFrame(cols)


def _fmt_cell(v, metric):
    if isinstance(v, str):
        return v
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    if metric in ("ACoS", "TACoS", "TACoS_实收", "CVR", "广告销售占比", "全站转化率", "退货率(件)"):
        return f"{v * 100:.1f}%"
    return f"{v:,.0f}" if abs(v) >= 100 else f"{v:.2f}".rstrip("0").rstrip(".")


def summary_md(S, prev=None):
    """店铺汇总表；有上周时追加 全店 本周/上周/环比 表"""
    cols = list(S.columns)
    out = ["| 指标 | " + " | ".join(cols) + " |", "|" + "---|" * (len(cols) + 1)]
    for m in S.index:
        out.append(f"| {m} | " + " | ".join(_fmt_cell(S.at[m, c], m) for c in cols) + " |")
    if prev is not None and "全店" in prev.columns:
        out += ["", "**全店 本周 vs 上周**(比率类给百分点差，其余给环比)", "| 指标 | 本周 | 上周 | 环比 |", "|---|---|---|---|"]
        for m in S.index:
            a, b = S.at[m, "全店"], prev.at[m, "全店"] if m in prev.index else np.nan
            if isinstance(a, str) or isinstance(b, str):
                chg = ""
            elif m in ("ACoS", "TACoS", "TACoS_实收", "CVR", "广告销售占比", "全站转化率", "退货率(件)"):
                chg = f"{(a - b) * 100:+.2f}pp" if pd.notna(a) and pd.notna(b) else ""
            else:
                chg = f"{a / b - 1:+.1%}" if pd.notna(a) and pd.notna(b) and b else ""
            out.append(f"| {m} | {_fmt_cell(a, m)} | {_fmt_cell(b, m)} | {chg} |")
    return "\n".join(out) + "\n"


def _gmatch(term, product_gender):
    """搜索词/关键词的性别 vs 商品性别：一致/不符/中性(词里没写性别)/商品性别未知"""
    tg = text_gender(term)
    if not tg:
        return "中性"
    if product_gender not in ("男", "女"):
        return "商品性别未知"
    return "一致" if tg in (product_gender, "男女") else "不符"


def read_weeks(con, table, weeks):
    ex = con.execute("select name from sqlite_master where type='table' and name=?", (table,)).fetchone()
    if not ex:
        return pd.DataFrame()
    ph = ",".join("?" * len(weeks))
    return pd.read_sql(f'select * from "{table}" where 周结束 in ({ph})', con, params=weeks)

def cmd_pack(a, cfg):
    con = sqlite3.connect(rel(cfg["db_path"]))
    allw = [r[0] for r in con.execute("select distinct 周结束 from weekly_parent order by 周结束")]
    end = a.end or allw[-1]
    ws = [w for w in allw if w <= end][-a.weeks:]
    if not ws:
        sys.exit("库里没有可用的周数据")
    q = []
    if len(ws) < a.weeks:
        q.append(f"请求{a.weeks}周，库里只有{len(ws)}周：{ws}")
    for x, y in zip(ws, ws[1:]):
        gap = (datetime.strptime(y, "%Y-%m-%d") - datetime.strptime(x, "%Y-%m-%d")).days
        if gap != 7:
            q.append(f"{x} 与 {y} 相隔{gap}天(不是7天)，趋势需谨慎")
    latest = ws[-1]
    P = read_weeks(con, "weekly_parent", ws)
    Q = read_weeks(con, "weekly_quality", [latest])
    key = ["店铺", "款", "父ASIN"]
    order = P[P["周结束"] == latest].sort_values("广告花费", ascending=False, na_position="last")[key]
    _lt = P[P["周结束"] == latest]
    def _same(a_, b_):      # 只比较两边都有值的父体(未在售/无SP数据的行不参与)
        if a_ not in _lt.columns or b_ not in _lt.columns:
            return False
        ok_ = _lt[a_].notna() & _lt[b_].notna()
        return bool(ok_.sum() > 0 and np.allclose(_lt.loc[ok_, a_].astype(float), _lt.loc[ok_, b_].astype(float), rtol=1e-4, atol=0.011))
    dup_sp = _same("销量7", "销量_SP") and _same("销售额7", "销售额_SP")      # 订单口径与SP订购完全一致时，不再重复列 _SP 列
    use_sp = P["SP_会话"].notna().any()
    tcols = ["销量7", "销售额7", "广告花费", "广告销售", "广告订单", "点击", "ACoS", "TACoS"] + (["TACoS_实收"] if "TACoS_实收" in P.columns else []) + \
            ["CVR", "FBA可售", "在途", "库存天数_7", "均价", "评分", "评论数"]
    if use_sp:
        if not dup_sp:
            tcols += ["销量_SP", "销售额_SP", "TACoS_SP", "库存天数_SP"]
        tcols += ["SP_会话", "全站转化率", "SP_BuyBox占比", "SP_退货率"]
    long = order.merge(P, on=key, how="left").sort_values(["店铺", "款", "周结束"])
    long["_o"] = long.set_index(key).index.map({tuple(r): i for i, r in enumerate(order.itertuples(index=False))})
    long = long.sort_values(["_o", "周结束"])

    # ---- 读取SP逐窗口销量历史(下面"本周 vs 上周"要用，必须先于循环加载)
    try:
        SPH = pd.read_sql('select * from "weekly_sp_sales"', con)
    except Exception:
        SPH = pd.DataFrame()

    # ---- 本周 vs 上周
    cur = P[P["周结束"] == latest].set_index(key)
    prv = P[P["周结束"] == ws[-2]].set_index(key) if len(ws) > 1 else None
    rows = []
    for k in order.itertuples(index=False):
        k = tuple(k)
        if k not in cur.index:
            continue
        c = cur.loc[k]
        r = {"店铺": k[0], "款": k[1], "阶段": c["阶段"], "样本量": c["样本量"], "库存状态": c["库存状态"], "销量来源": c.get("销量来源")}
        for m in ["销售额7", "广告花费", "广告订单"]:
            r["本周" + m] = c[m]
            if prv is not None and k in prv.index and pd.notna(prv.loc[k, m]) and prv.loc[k, m] != 0:
                r[m + "变化"] = (c[m] - prv.loc[k, m]) / abs(prv.loc[k, m])
        r["本周ACoS"] = c["ACoS"]
        if prv is not None and k in prv.index:
            r["ACoS变化pp"] = (c["ACoS"] - prv.loc[k, "ACoS"]) * 100 if pd.notna(c["ACoS"]) and pd.notna(prv.loc[k, "ACoS"]) else np.nan
        for m in ["销量_SP", "销售额_SP", "TACoS_SP"]:
            if m in c.index and not dup_sp:
                r[m] = c[m]
        if len(SPH) and pd.notna(c.get("销量_SP", np.nan)):
            pw = (datetime.strptime(latest, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
            pr = SPH[(SPH["店铺"] == k[0]) & (SPH["父ASIN"] == k[2]) & (SPH["窗口结束"] == pw)]
            if len(pr) and pr["覆盖天数"].iloc[0] > 0 and pd.notna(pr["订购件数"].iloc[0]):
                prev_u = pr["订购件数"].iloc[0] * 7 / pr["覆盖天数"].iloc[0]
                r["销量_SP上周"] = prev_u
                if prev_u > 0:
                    r["销量_SP变化"] = (c["销量_SP"] - prev_u) / prev_u
        for m in ["库存天数_7", "含在途天数_7", "盈亏ACoS", "盈亏ACoS上限_未含头程", "估算7日利润_含头程"]:
            r[m] = c[m]
        rows.append(r)
    chg = pd.DataFrame(rows)
    for m in ["销售额7变化", "广告花费变化", "广告订单变化", "销量_SP变化"]:
        if m in chg:
            PCT_COLS.add(m)

    # ---- 窗口明细
    top = cfg["top_n"]
    S = read_weeks(con, "weekly_searchterm", ws); K = read_weeks(con, "weekly_keyword", ws)
    SQ = read_weeks(con, "weekly_sqp", ws)
    C = read_weeks(con, "weekly_campaign", ws); PL = read_weeks(con, "weekly_placement", ws); AU = read_weeks(con, "weekly_auto", ws)
    parts = []
    spend_win = P.groupby(key)["广告花费"].sum().sort_values(ascending=False)
    targets = [k for k, v in spend_win.items() if v and v > 0][: top["detail_parents"]]
    for k in targets:
        sel = lambda df: df[(df["店铺"] == k[0]) & (df["款"] == k[1])] if len(df) else df
        _pr = P[(P["周结束"] == latest) & (P["店铺"] == k[0]) & (P["父ASIN"] == k[2])]
        pg_ = str(_pr["商品性别"].iloc[0]) if len(_pr) and "商品性别" in _pr.columns else "未知"
        _cat = str(_pr["品类"].iloc[0]) if len(_pr) and "品类" in _pr.columns else "未知"
        blk = [f"### {k[0]} | {k[1]} | 父ASIN {k[2]} | 商品：{pg_}装 {_cat}\n"]
        if len(C):
            cl = sel(C[C["周结束"] == latest]).copy()
            cw = sel(C).groupby("广告活动").agg(窗口花费=("花费", "sum"), 窗口销售=("广告销售", "sum"), 窗口订单=("广告订单", "sum")).reset_index()
            cw["窗口ACoS"] = cw["窗口花费"] / cw["窗口销售"].where(cw["窗口销售"] > 0)
            cl = cl.merge(cw, on="广告活动", how="left").sort_values("花费", ascending=False)
            blk.append("**广告活动(本周 + 窗口累计)**\n" + md_table(cl, ["广告活动", "有效状态", "预算", "日均花费", "预算使用率", "点击", "广告订单", "ACoS", "IS", "窗口订单", "窗口ACoS"]))
        if len(PL):
            pg = sel(PL).groupby("广告位")[["曝光", "点击", "花费", "广告销售", "广告订单"]].sum().reset_index()
            pg["ACoS"] = pg["花费"] / pg["广告销售"].where(pg["广告销售"] > 0)
            pg["CVR"] = pg["广告订单"] / pg["点击"].where(pg["点击"] > 0)
            pg["CPC"] = pg["花费"] / pg["点击"].where(pg["点击"] > 0)
            blk.append("**广告位(窗口累计)**\n" + md_table(pg, ["广告位", "点击", "花费", "广告订单", "ACoS", "CVR", "CPC"]))
        if len(K):
            kl = sel(K).groupby(["广告组", "关键词", "匹配方式"]).agg(点击=("点击", "sum"), 花费=("花费", "sum"), 广告销售=("广告销售", "sum"), 广告订单=("广告订单", "sum")).reset_index()
            lastbid = sel(K[K["周结束"] == latest]).groupby(["广告组", "关键词", "匹配方式"])["竞价"].first().rename("当前竞价").reset_index()
            kl = kl.merge(lastbid, on=["广告组", "关键词", "匹配方式"], how="left")
            kl["ACoS"] = kl["花费"] / kl["广告销售"].where(kl["广告销售"] > 0)
            kl["CVR"] = kl["广告订单"] / kl["点击"].where(kl["点击"] > 0)
            kl = kl[kl["花费"] > 0].sort_values("花费", ascending=False).head(top["keywords"])
            kl["性别匹配"] = [_gmatch(t_, pg_) for t_ in kl["关键词"]]
            blk.append("**手动关键词(窗口累计，按花费Top)**\n" + md_table(kl, ["关键词", "匹配方式", "性别匹配", "当前竞价", "点击", "花费", "广告订单", "ACoS", "CVR"]))
        if len(AU):
            al = sel(AU[AU["周结束"] == latest])
            al = al[al["花费"] > 0].sort_values("花费", ascending=False)
            blk.append("**自动投放(本周)**\n" + md_table(al, ["广告组", "投放", "竞价", "默认竞价", "点击", "花费", "广告订单", "ACoS"]))
        if len(S):
            sg = sel(S).groupby(["用户搜索词", "来源", "词类型", "已投放为手动词"]).agg(
                周数=("周结束", "nunique"), 点击=("点击", "sum"), 花费=("花费", "sum"), 广告销售=("广告销售", "sum"), 广告订单=("广告订单", "sum")).reset_index()
            sg["ACoS"] = sg["花费"] / sg["广告销售"].where(sg["广告销售"] > 0)
            sg["性别匹配"] = [_gmatch(t_, pg_) for t_ in sg["用户搜索词"]]
            waste = sg[(sg["广告订单"] == 0) & (sg["点击"] >= cfg["min_clicks_waste"])].sort_values("花费", ascending=False).head(top["waste"])
            win = sg[sg["广告订单"] >= 2].sort_values("广告销售", ascending=False).head(top["winners"])
            blk.append(f"**搜索词：窗口内零订单且点击>={cfg['min_clicks_waste']}(观察名单；点击<30只能观察/试降，是否否定以第5节候选为准)**\n" + md_table(waste, ["用户搜索词", "来源", "词类型", "性别匹配", "周数", "点击", "花费"]))
            blk.append("**搜索词：窗口内订单>=2(收割/加价候选；'已投放为手动词'=否 表示尚未单独投放)**\n" + md_table(win, ["用户搜索词", "来源", "词类型", "性别匹配", "已投放为手动词", "周数", "点击", "花费", "广告订单", "ACoS"]))
        if len(SQ):
            sqk = SQ[(SQ["店铺"] == k[0]) & (SQ["父ASIN"] == k[2])]
            if len(sqk):
                w_ = sqk["周结束"].max()
                cs = sqk[sqk["周结束"] == w_].copy()
                per = f"{cs['周期起'].iloc[0]}~{cs['周期止'].iloc[0]}"
                ad_terms = set(sel(S)["用户搜索词"].astype(str).str.lower()) if len(S) else set()
                t_ = cs.sort_values("搜索量", ascending=False).head(top.get("sqp", 10)).copy()
                t_["广告中出现"] = t_["搜索词"].astype(str).str.lower().isin(ad_terms)
                blk.append(f"**SQP 搜索词(SQP周期{per}；每ASIN仅Top100查询；按搜索量降序；'广告中出现'=窗口内广告搜索词里有)**\n"
                           + md_table(t_, ["搜索词", "搜索量", "总点击", "我方点击", "点击份额", "我方购买", "购买份额", "广告中出现"]))
        parts.append("\n".join(blk))

    # ---- 拼装
    prompt = _monthly_prompt(PROMPT) if getattr(a, "report", "weekly") == "monthly" else PROMPT
    if getattr(a, "report", "weekly") == "monthly" and len(ws) < 3:
        q.append(f"月报至少需要3~4周数据，当前只有{len(ws)}周：趋势与月度结论只有周报级可靠度")
    if cfg.get("prompt_file") and os.path.exists(rel(cfg["prompt_file"])):
        prompt = open(rel(cfg["prompt_file"]), encoding="utf-8").read()
    qual = Q[Q["级别"].isin(["WARN", "ERROR", "INFO"])]
    md = [prompt.replace("最近N周", f"最近{len(ws)}周"), "\n---\n", f"# 数据包：{ws[0]} ~ {latest}，共{len(ws)}周(每周为7天汇总快照，周结束日=各周日期)\n"]
    if q:
        md.append("**窗口提示**：" + "；".join(q) + "\n")
    md.append(f"**口径提醒**：金额=USD；库存天数=FBA可售÷7日日均(不含在途)；评分为变体家族共用评分(已剔除新子体的0分)；广告已覆盖=否 的行广告列为空而非0；"
              f"最近{cfg['attribution_lag_days']}天广告订单可能未回补完整。\n")
    if dup_sp:
        md.append("**核对**：销量7/销售额7(订单口径)与 SP 订购件数/订购销售额逐父体完全一致，下面不再重复列出 _SP 列(销量_SP、销售额_SP、TACoS_SP、库存天数_SP)。\n")
    md.append("## 1. 数据质量(最新一周)\n" + md_table(qual))
    # ---- 店铺与全店汇总(报告"核心指标总览"用；有上周数据时给环比，没有就明确写只有一周)
    _S = store_summary(P[P["周结束"] == latest])
    _Sp = store_summary(P[P["周结束"] == ws[-2]]) if len(ws) > 1 else None
    md.append(f"## 1b. 店铺与全店汇总(最新一周 {latest}；金额USD；空白=该项无数据，不是0)\n"
              + ("" if _Sp is not None else "说明：库里只有这一周，**没有上周可比**，报告里不得写环比。\n")
              + summary_md(_S, _Sp))
    _lt_ = P[P["周结束"] == latest]
    if "商品性别" in _lt_.columns:
        md.append("## 1c. 产品档案(商品属性只以此表为准；来自Listing标题，'未知'=标题没写，不得推断)\n"
                  + md_table(_lt_.sort_values(["店铺", "款"]), ["店铺", "款", "父ASIN", "商品性别", "品类", "袖长", "在售子体数", "标题摘要"]))
    # ---- 窗口汇总：N周合计，比率按合计重算(周报=1周；月报=4周)
    _s1 = lambda v_: v_.sum(min_count=1)      # 全为空(如 广告已覆盖=否)时保持空，不变成0
    aggm = {"周数": ("周结束", "nunique"), "销量合计": ("销量7", _s1), "销售额合计": ("销售额7", _s1), "广告花费合计": ("广告花费", _s1),
            "广告销售合计": ("广告销售", _s1), "广告订单合计": ("广告订单", _s1), "点击合计": ("点击", _s1)}
    if "订单实收销售额" in P.columns:
        aggm["实收销售额合计"] = ("订单实收销售额", _s1)
        aggm["实收周数"] = ("订单实收销售额", "count")
    if "SP_退货件数" in P.columns:
        aggm["退货件数合计"] = ("SP_退货件数", lambda v_: v_.sum(min_count=1))
    aggm["估算利润合计"] = ("估算7日利润_含头程", lambda v_: v_.sum(min_count=1))
    if "会话7" in P.columns:
        aggm["会话合计"] = ("会话7", lambda v_: v_.sum(min_count=1))
    wsum = P.groupby(key).agg(**aggm).reset_index()
    lastrow = P[P["周结束"] == latest].set_index(key)
    wsum = wsum.set_index(key)
    wsum["日均销量"] = wsum["销量合计"] / (7 * wsum["周数"])
    if "销量来源" in P.columns:
        wsum["订单口径周数"] = P[P["销量来源"] == "订单导出"].groupby(key).size().reindex(wsum.index).fillna(0).astype(int)
    if "实收销售额合计" in wsum.columns:      # 只有每一周都有订单实收数据时才用实收做分母，否则(混合口径)退回 销售额合计，避免分子分母周数不一致
        _rev = wsum["销售额合计"].where(wsum["实收周数"] < wsum["周数"], wsum["实收销售额合计"])
    else:
        _rev = wsum["销售额合计"]
    wsum["ACoS(窗口)"] = wsum["广告花费合计"] / wsum["广告销售合计"].where(wsum["广告销售合计"] > 0)
    wsum["TACoS(窗口)"] = wsum["广告花费合计"] / _rev.where(_rev > 0)
    wsum["CVR(窗口)"] = wsum["广告订单合计"] / wsum["点击合计"].where(wsum["点击合计"] > 0)
    wsum["FBA可售(最新)"] = lastrow["FBA可售"]
    wsum["在途(最新)"] = lastrow["在途"]
    wsum["库存天数(窗口日均)"] = wsum["FBA可售(最新)"] / wsum["日均销量"].where(wsum["日均销量"] > 0)
    if "退货件数合计" in wsum.columns:
        wsum["退货率(窗口)"] = wsum["退货件数合计"] / wsum["销量合计"].where(wsum["销量合计"] > 0)
    if "会话合计" in wsum.columns:
        wsum["广告点击占会话(窗口)"] = wsum["点击合计"] / wsum["会话合计"].where(wsum["会话合计"] > 0)
        PCT_COLS.add("广告点击占会话(窗口)")
    wsum["最近一周vs周均"] = lastrow["销量7"] / (wsum["销量合计"] / wsum["周数"]).where(wsum["销量合计"] > 0) - 1
    for c_ in ("ACoS(窗口)", "TACoS(窗口)", "CVR(窗口)", "退货率(窗口)", "最近一周vs周均"):
        PCT_COLS.add(c_)
    wsum = wsum.reset_index().sort_values("广告花费合计", ascending=False)
    wcols = ["店铺", "款", "周数"] + (["订单口径周数"] if "订单口径周数" in wsum.columns else []) + ["销量合计", "日均销量", "销售额合计"] + (["实收销售额合计"] if "实收销售额合计" in wsum.columns else []) + \
            (["会话合计", "广告点击占会话(窗口)"] if "会话合计" in wsum.columns else []) + ["广告花费合计", "ACoS(窗口)", "TACoS(窗口)", "CVR(窗口)", "FBA可售(最新)", "在途(最新)", "库存天数(窗口日均)"] + \
            (["退货件数合计", "退货率(窗口)"] if "退货率(窗口)" in wsum.columns else []) + ["估算利润合计", "最近一周vs周均"]
    if "订单口径周数" in wsum.columns:
        _ost = set(P.loc[P["销量来源"] == "订单导出", "店铺"])          # 窗口内至少有一周订单导出的店铺
        _mix = wsum[(wsum["订单口径周数"] > 0) & (wsum["订单口径周数"] < wsum["周数"])]
        _mix = pd.concat([_mix, wsum[(wsum["订单口径周数"] == 0) & wsum["店铺"].isin(_ost)]])
        if len(_mix):
            md.append(f"**口径提醒(多周)**：窗口内有 {len(_mix)} 个父体的部分周没有订单导出，那几周的销量/销售额来自领星Listing滚动口径(与周窗口不重合)，"
                      "与订单口径混合；该类父体的周间销量趋势、窗口合计只作参考。补上缺失周的订单导出后重跑 ingest 即可修正。\n")
        _nos = sorted(wsum.loc[~wsum["店铺"].isin(_ost) & (wsum["销量合计"].fillna(0) > 0), "店铺"].unique())
        if _nos:
            md.append(f"**口径提醒**：店铺 {_nos} 窗口内没有订单导出，其销量/销售额全部来自领星Listing滚动口径(与周窗口不重合)。\n")
    md.append(f"## 2a. 窗口汇总({len(ws)}周合计；比率按合计重算，月报以此为准)\n" + md_table(wsum, wcols))
    md.append("## 2b. 本周 vs 上周(按广告花费降序)\n" + md_table(chg))
    md.append("## 3. 各父体逐周明细(周结束升序)\n" + md_table(long, ["店铺", "款", "周结束"] + tcols))
    spc = ["SP_流量覆盖天数", "SP_会话", "全站转化率", "SP_单位会话率", "SP_BuyBox占比", "SP_搜索曝光", "SP_搜索点击率", "SP_搜索转化率", "SP_搜索购买", "SP_搜索周期", "SP_订购件数", "SP_退货件数", "SP_退货率", "SP_退货原因Top3",
           "SP_库龄90天以上件数", "SP_库龄181天以上件数", "SP_预估仓储费", "标价", "SP_我方价格", "SP_我方实际售价", "SP_促销中SKU数", "SP_最低促销价", "保本价", "SP_精选报价价格", "SP_精选报价为我方", "SP_竞品最低价", "SP_价格高于竞品最低价比例", "SP_SQP周期"]
    spc = [c for c in spc if c in P.columns]
    spl = P[P["周结束"] == latest]
    spl = spl[spl[[c for c in spc if c.startswith("SP_")]].notna().any(axis=1)] if spc else spl.iloc[0:0]
    if "会话7" in P.columns and P[P["周结束"] == latest]["会话7"].notna().any():
        tl = P[P["周结束"] == latest].copy()
        tl = tl[tl["会话7"].notna()].sort_values("会话7", ascending=False)
        if len(ws) > 1:
            pv_ = P[P["周结束"] == ws[-2]].set_index(key)["会话7"].to_dict()
            tl["会话变化"] = [(r_["会话7"] / pv_[(r_["店铺"], r_["款"], r_["父ASIN"])] - 1) if pv_.get((r_["店铺"], r_["款"], r_["父ASIN"])) else np.nan for _, r_ in tl.iterrows()]
        if "大类排名" in P.columns:
            if len(ws) > 1:
                pr_ = P[P["周结束"] == ws[-2]].set_index(key)["大类排名"].to_dict()
                tl["大类排名变化"] = [(r_["大类排名"] / pr_[(r_["店铺"], r_["款"], r_["父ASIN"])] - 1) if (pd.notna(r_["大类排名"]) and pr_.get((r_["店铺"], r_["款"], r_["父ASIN"])) and pd.notna(pr_.get((r_["店铺"], r_["款"], r_["父ASIN"])))) else np.nan for _, r_ in tl.iterrows()]
        is_ = {}
        if len(C):
            cl_ = C[C["周结束"] == latest]
            for (st_, pa_), g_ in cl_.groupby(["店铺", "父ASIN"]):
                w_ = g_["曝光"].fillna(0)
                is_[(st_, pa_)] = float((g_["IS"] * w_).sum() / w_.sum()) if w_.sum() > 0 and g_["IS"].notna().any() else np.nan
        tl["IS(加权)"] = [is_.get((a_, b_), np.nan) for a_, b_ in zip(tl["店铺"], tl["父ASIN"])]
        tl = tl.rename(columns={"点击": "广告点击", "曝光": "广告曝光", "CVR": "广告CVR", "CTR": "广告CTR"})
        for c_ in ("会话变化", "广告CVR", "广告CTR", "自然转化率估算", "广告订购占比", "IS(加权)", "大类排名变化"):
            PCT_COLS.add(c_)
        tc_ = [c for c in ["会话7", "会话变化", "会话相对店内中位", "广告点击", "广告点击占会话", "自然会话估算", "全站转化率", "转化率相对店内中位", "广告CVR", "自然转化率估算",
                           "广告订购占比", "每会话销售额", "每会话广告成本", "广告曝光", "广告CTR", "IS(加权)", "大类排名", "大类排名变化", "SP_BuyBox占比", "评分"] if c in tl.columns]
        md.append("## 3a. 父体流量 vs 广告(最新一周；用于判断流量是否充足、结构是否过度依赖广告、问题在流量还是转化)\n"
                  "说明：会话7=总流量(自然+广告)折算7天；广告点击占会话、自然会话估算是粗略近似(点击≠会话)，只看相对大小与趋势；"
                  "全站转化率=订购件数÷会话；广告CVR=广告订单÷点击，两者口径不同，不能直接相减下结论；广告订购占比>100%时自然转化率估算为空；"
                  "IS(加权)=按曝光加权的广告展示份额；相对店内中位：1=店内中位水平；大类排名=父体下各子体排名的中位数(数值越小越好，变化<0=排名上升)，是导出日快照，只看趋势。\n" + md_table(tl, ["店铺", "款"] + tc_))
    if len(spl):
        spc_used = [c for c in spc if spl[c].notna().any()]          # 全空的列(如默认关闭的搜索漏斗/SQP)不占篇幅
        md.append("## 3b. SP-API 数据(最新一周，按广告花费降序)\n"
                  "价格口径：标价=领星Listing标价；SP_我方价格=库存计划报告 your-price(标价)；SP_我方实际售价=有促销价(sales-price)用促销价，否则标价；"
                  "SP_竞品最低价=新品最低价含运费——常常就是我方自己的促销价；SP_精选报价价格=Buy Box 精选报价(等于我方促销价或标价即为我方)；"
                  "SP_价格高于竞品最低价比例 = 我方实际售价÷最低价−1(逐子体再平均)，<=0 表示我方就是最低价。判断'贵不贵'只能用这个比例，不得用标价去比；"
                  "SP_最低促销价 低于 保本价 表示有尺码在亏本促销。\n" + md_table(spl.sort_values("广告花费", ascending=False, na_position="last"), ["店铺", "款"] + spc_used))
    if len(SPH):
        wins = [(datetime.strptime(latest, "%Y-%m-%d") - timedelta(days=7 * k_)).strftime("%Y-%m-%d") for k_ in range(len(ws) if len(ws) > 1 else a.weeks)][::-1]
        sp_h = SPH[SPH["窗口结束"].isin(wins)].copy()
        nwin_h = sp_h["窗口结束"].nunique() if len(sp_h) else 0
        if len(sp_h) and (nwin_h > 1 or not dup_sp):
            ordk = {(r_["店铺"], r_["父ASIN"]): i_ for i_, r_ in order.reset_index(drop=True).iterrows()}
            sp_h["_o"] = [ordk.get((a_, b_), 999) for a_, b_ in zip(sp_h["店铺"], sp_h["父ASIN"])]
            sp_h = sp_h.sort_values(["_o", "窗口结束"])
            for c_ in ("单位会话率", "BuyBox占比"):
                if c_ in sp_h:
                    PCT_COLS.add(c_)
            md.append("## 3c. SP口径周度销量/流量(窗口=周日~周六，与广告窗口对齐；含回填的历史窗口)\n"
                      "说明：订购件数/订购销售额与广告订单同一'订购'口径；覆盖天数<7表示该窗口亚马逊数据不完整。\n"
                      + md_table(sp_h, ["店铺", "款", "窗口结束", "覆盖天数", "订购件数", "订购销售额", "会话", "单位会话率", "BuyBox占比"]))
    oc = [c for c in ["销量来源", "订单件数", "订单销售额", "订单促销件数", "订单促销销售额", "订单实收销售额", "订单实收均价", "订单换货件数",
                      "订单B2B件数", "订单待处理件数", "订单退款件数"] if c in P.columns]
    ol = P[P["周结束"] == latest]
    if "订单件数" in oc and ol["订单件数"].notna().any():
        ol = ol[ol["订单件数"].notna()].sort_values("订单件数", ascending=False)
        md.append("## 3d. 订单口径(最新一周；销量来源=订单导出 时即 销量7/销售额7 的来源)\n" + md_table(ol, ["店铺", "款"] + oc))
    sc_ = [c for c in ["库存天数_7", "含在途天数_7", "日均7", "FBA可售", "货件_已发货在途件数", "货件_入库中待收件数", "货件_已签收待上架件数", "最早到仓日", "最晚到仓日",
                       "到仓_保守_7日内", "到仓_保守_8-14日", "到仓_保守_15-30日", "到仓_保守_30日后", "断货天数_乐观", "断货天数_保守", "首次断货_天后",
                       "海运最晚发货_天后", "空运最晚发货_天后", "海运最晚下单_天后", "空运最晚下单_天后", "采购交期_天", "未来60日缺口件数", "在途_发往非在售子体件数"] if c in P.columns]
    _lt0 = P[P["周结束"] == latest]
    _sim0 = _lt0["模拟起算日"].dropna().astype(str) if "模拟起算日" in _lt0.columns else pd.Series(dtype=str)
    _sim0 = _sim0[_sim0 != ""]
    sim_note = (f"模拟起算日={_sim0.iloc[0]}(FBA货件/补货快照日，不是周结束日{latest})，首次断货_天后/最晚发货_天后/最晚下单_天后都从这天算。" if len(_sim0) else "")
    has_sup = "断货天数_全供给_海运" in P.columns and P[P["周结束"] == latest]["断货天数_全供给_海运"].notna().any()
    if has_sup:
        cc_ = [c for c in ["库存天数_7", "日均7", "日均_7天", "日均_14天", "日均_30天", "日均_60天", "日均_预测", "FBA可售", "货件_已发货在途件数", "货件_入库中待收件数", "货件_已签收待上架件数", "最晚到仓日", "最晚可售日", "本地可用", "待交付", "待交付_最早预计可售日", "待交付_最晚预计可售日",
                           "待交付_已过预计到货件数", "待交付_无PO明细件数", "断货天数_保守", "首次断货_天后", "断货天数_含待交付", "断货天数_全供给_空运", "断货天数_全供给_海运",
                           "首次断货_全供给_海运_天后", "本地仓最少空运件数_避免断货", "本地仓最少海运件数_避免断货", "空运可售前无法避免断货天数", "建议空运件数", "建议空运件数_按近7天日均", "空运发货_本地仓不足件数",
                           "尺码_当前断码数", "尺码_当前断码_主力数", "尺码_当前断码", "尺码_缺货件数_含待交付", "尺码_缺货占需求比例", "尺码_空运前无法避免缺货件数",
                           "建议空运件数_尺码合计", "空运_本地可发件数", "建议海运发货件数_尺码合计", "海运_本地可发件数", "尺码_本地仓不足件数",
                           "冗余_FBA件数", "冗余_总件数", "冗余_可削减PO件数", "滞销_SKU数", "滞销_FBA件数", "滞销_本地件数", "新SKU观察数", "亚马逊冗余件数", "冗余_预估月仓储费", "保本价",
                           "建议海运发货件数", "海运发货_本地仓不足件数", "空运可售_天后", "海运可售_天后", "海运发货_覆盖至第N天",
                           "新采购最晚下单_海运_天后", "新采购最晚下单_空运_天后",
                           "未来60日缺口件数_含全部供给", "在途_发往非在售子体件数"] if c in P.columns]
        if "尺码_缺货件数_含待交付" in P.columns and P[P["周结束"] == latest]["尺码_缺货件数_含待交付"].notna().any():
            _sup = {"断货天数_全供给_空运", "断货天数_全供给_海运", "首次断货_全供给_海运_天后", "本地仓最少空运件数_避免断货", "本地仓最少海运件数_避免断货",
                    "空运可售前无法避免断货天数", "建议空运件数", "建议空运件数_按近7天日均", "空运发货_本地仓不足件数", "建议海运发货件数", "海运发货_本地仓不足件数",
                    "新采购最晚下单_海运_天后", "新采购最晚下单_空运_天后", "未来60日缺口件数_含全部供给"}
            cc_ = [c for c in cc_ if c not in _sup]            # 父体级结论会被别的尺码库存抵消(例如'本地仓够、无需采购')，有尺码级时不给AI，避免自相矛盾
        sl2 = P[P["周结束"] == latest].copy()
        sl2 = sl2[(sl2["日均7"].fillna(0) > 0) | ((sl2["货件_已发货在途件数"].fillna(0) + sl2["货件_入库中待收件数"].fillna(0) + sl2["待交付"].fillna(0) + sl2["本地可用"].fillna(0)) > 0)]
        sl2 = sl2.sort_values(["断货天数_全供给_海运", "断货天数_保守"], ascending=[False, False], na_position="last")
        md.append("## 3e. 供给链条与断货风险(最新一周；按'全供给海运'断货天数降序)\n"
                  "说明：" + sim_note + "按 日均_预测(7/14/30/60天日均加权，日均7只描述本周)匀速销售逐日模拟未来60天；库存只有'可售'才算有货——已到仓/已签收但还没上架的不算，按可售日入库；"
                  "尺码_* 与 *_尺码合计 是逐子体SKU模拟(亚马逊按子体判断缺货，某尺码可售为0就断货)，补货/发货件数以尺码级为准，父体级列只作参考(会用别的尺码库存抵消)。供给分层：S0=FBA可售+已发货/入库中货件+已签收待上架(可售=送达时段止/开始入库日/签收日+上架天数，送达时段是登记值，海运常延误)→断货天数_保守；"
                  f"S1=S0+工厂待交付(按领星预计可售日，已过预计到货日的按晚到天数顺延)→断货天数_含待交付；S2=S1+本地仓库存今天发出(空运/海运，可售=备货{int(cfg.get('local_ship_prep_days', 2))}天+历史中位运输天数+上架{int(cfg.get('receiving_avail_days', 10))}天)→断货天数_全供给_空运/海运。"
                  "本地仓最少空运/海运件数=刚好避免60天内断货所需的发出量(0=不发也不断货；空=全部发出也避免不了——件数不够或断货早于这批货可售日——或没有本地库存/没有销量)；新采购最晚下单=S2首次断货−采购交期−备货−运输−上架，<0=新下单也来不及，空=60天内不需要新采购(全部供给数量够、断货只因到货时间时也为空)。"
                  f"建议海运发货件数=今天海运发出的量，覆盖从海运可售日到 到可售天数+发货周期{int(cfg.get('replen_cycle_days', 10))}天+安全库存{int(cfg.get('safety_stock_days', 10))}天(见 海运发货_覆盖至第N天)；可售日之前的断货海运补不上，要看空运/控速；建议空运件数=今天空运发出、撑到海运可售日所需的最少件数(0=不用空运)；空运可售前无法避免断货天数>0=空运也来不及，只能控速；海运发货_本地仓不足件数 已先扣掉建议空运件数。"
                  "父体级合计，未按尺码拆分：尺码结构不匹配时实际缺货更早、能发的量更少；本地可用/待交付是多站点共享池，已只计入主站。\n"
                  + md_table(sl2, ["店铺", "款"] + cc_))
        oc2 = [c for c in ["日均7", "领星_7天日均", "首次断货_天后", "领星_断货时间", "领星_建议采购日", "领星_建议采购量", "领星_建议采购量_海派", "领星_本地发FBA量", "领星_本地发FBA量_海派",
                           "领星_建议本地发货日", "领星_备货时长"] if c in sl2.columns]
        try:
            SK_ = read_weeks(con, "weekly_sku_supply", [latest])
        except Exception:
            SK_ = pd.DataFrame()
        if len(SK_):
            SK_ = SK_[(SK_["缺货件数_含待交付"] >= 1) | (SK_["建议空运件数"].fillna(0) > 0) | (SK_["建议海运件数"].fillna(0) > 0) | (SK_["当前断码"].astype(bool))]
            ordp = {k_: i_ for i_, k_ in enumerate(sl2["父ASIN"])}
            SK_ = SK_.assign(_o=SK_["父ASIN"].map(ordp).fillna(999)).sort_values(["_o", "缺货件数_含待交付"], ascending=[True, False]).head(int(cfg.get("top_n", {}).get("sku_rows", 60)))
            for c_ in ("缺货件数_保守", "缺货件数_含待交付", "空运前无法避免缺货件数"):
                SK_[c_] = SK_[c_].round(0)
            md.append("## 3e3. 尺码级缺货与发货(最新一周；逐子体SKU模拟，按父体顺序、缺货件数降序；只列有缺货或需发货的SKU)\n"
                      "说明：日均_预测=该SKU的7/14/30/60天日均加权；在途与待上架=在途货件+已签收待上架(按可售日入库)；本地可用_池=同账号多站点共享的本地仓库存；"
                      "建议空运件数=撑到海运可售日的最少件数，建议海运件数=覆盖到可售+发货周期+安全库存；本地不足=本地仓对应尺码不够发的件数(需催PO/工厂直发/新采购)。"
                      "断码SKU近7天销量会因缺货偏低，其日均_预测可能低估真实需求。\n"
                      + md_table(SK_, ["店铺", "款", "MSKU", "日均_预测", "日均_7天", "FBA可售", "已到仓待上架", "在途与待上架", "待交付", "本地可用_池", "首次缺货_天后",
                                       "缺货件数_保守", "缺货件数_含待交付", "空运前无法避免缺货件数", "建议空运件数", "空运本地不足", "建议海运件数", "海运本地不足"]))
        if len(SK0_ := (read_weeks(con, "weekly_sku_supply", [latest]) if "weekly_sku_supply" in {r_[0] for r_ in con.execute("select name from sqlite_master")} else pd.DataFrame())):
            if "FBA冗余件数" in SK0_.columns:
                ex_ = SK0_[(SK0_["FBA冗余件数"] >= 5) | (SK0_["滞销"].astype(bool)) | (pd.to_numeric(SK0_["亚马逊冗余件数"], errors="coerce").fillna(0) > 0) | (SK0_["可削减PO件数"] >= 1)].copy()
                ex_ = ex_.sort_values(["FBA冗余件数", "总冗余件数"], ascending=False).head(int(cfg.get("top_n", {}).get("sku_rows", 60)))
                for c_ in ("FBA冗余件数", "总冗余件数", "可削减PO件数", "FBA库存天数", "总库存天数"):
                    ex_[c_] = pd.to_numeric(ex_[c_], errors="coerce").round(0)
                ex_["滞销"] = ex_["滞销"].map(lambda v_: "是" if bool(v_) else "")
                md.append(f"## 3e4. 冗余与滞销(尺码级，最新一周；按FBA冗余降序)\n"
                          f"说明：FBA冗余=FBA可售+在途−日均_预测×{int(cfg.get('excess_fba_days', 90))}天；总冗余=再加待交付+本地仓−日均_预测×{int(cfg.get('excess_total_days', 180))}天；"
                          "可削减PO=待交付中超出总冗余线的件数；滞销=开售且首次到FBA都满45天、近30天零销量；开售或到FBA不足45天的新SKU不在此表(见 新SKU观察数)。"
                          "亚马逊冗余件数/建议/建议价来自库存计划报告，建议价常低于保本价(见3e 保本价)，只作参考；预估月仓储费=下月亚马逊预估。\n"
                          + md_table(ex_, ["店铺", "款", "MSKU", "日均_预测", "开售天数", "FBA可售", "在途与待上架", "待交付", "本地可用_池", "FBA库存天数", "总库存天数",
                                           "FBA冗余件数", "总冗余件数", "可削减PO件数", "滞销", "库龄91天以上", "库龄181天以上", "亚马逊冗余件数", "亚马逊建议", "亚马逊建议价", "预估月仓储费"]))
        md.append("## 3e2. 领星补货建议对照(领星按其自己的参数和日均口径算的；仅对照，冲突时以 3e 为准并说明差异)\n" + md_table(sl2, ["店铺", "款"] + oc2))
    if "断货天数_保守" in sc_:
        sl = P[P["周结束"] == latest].copy()
        sl = sl[(sl["日均7"].fillna(0) > 0) | (sl["货件_已发货在途件数"].fillna(0) + sl["货件_入库中待收件数"].fillna(0) > 0)]
        sl = sl.sort_values(["断货天数_保守", "库存天数_7"], ascending=[False, True], na_position="last")
        if not has_sup:
            md.append("## 3e. 在途与断货风险(最新一周；按保守断货天数降序)\n"
                  "说明：" + sim_note + "到仓日=FBA货件送达时段(登记值，未必真实)；乐观=送达时段起、保守=送达时段止，入库中=开始入库日，都再加上架天数才可售；按日均7匀速销售逐日模拟未来60天；"
                  "不含工厂待交付/本地仓库存；海运/空运最晚发货_天后=首次断货_天后−(历史中位运输天数+上架天数)(负数=海运已来不及)。\n"
                  + md_table(sl, ["店铺", "款"] + sc_))
        try:
            SQL_ = read_weeks(con, "weekly_shipment", [latest])
        except Exception:
            SQL_ = pd.DataFrame()
        if len(SQL_):
            SQL_["到仓窗口"] = SQL_["窗口起"].astype(str).str[5:] + "~" + SQL_["窗口止"].astype(str).str[5:]
            if "计入供给" in SQL_.columns:        # sqlite 里布尔值存成 0/1
                SQL_["计入供给"] = SQL_["计入供给"].map(lambda v_: "" if pd.isna(v_) else ("是" if bool(v_) else "否"))
            SQL_ = SQL_.sort_values(["店铺", "款", "货件单号"])
            md.append("## 3f. 在途货件明细(最新一周，按店铺/款；计入供给=否 表示发往停售/未激活子体，不计入表内'在途'和断货模拟)\n"
                      + md_table(SQL_, ["店铺", "款", "货件单号", "状态", "运输方式", "物流中心编码", "发货日", "到仓窗口"] + (["计入供给"] if "计入供给" in SQL_.columns else []) + ["待收件数"]))
    if has_sup:
        try:
            POQ = read_weeks(con, "weekly_po", [latest])
        except Exception:
            POQ = pd.DataFrame()
        if len(POQ):
            POQ = POQ.sort_values(["店铺", "款", "预计可售", "单据号"])
            md.append("## 3f2. 待交付采购单(PO)明细(最新一周；预计到货=到本地仓，预计可售=到FBA可卖；已晚天数>0=预计到货日已过仍未到)\n"
                      + md_table(POQ, ["店铺", "款", "单据号", "数量", "下单", "预计到货", "已晚天数", "预计可售"]))
    md.append("## 4. 高花费父体的广告明细\n" + "\n".join(parts))
    out = rel(cfg["out_dir"]); os.makedirs(out, exist_ok=True)
    # ---- 5. 候选动作(规则生成，AI 只能从中选择；插件据此校验 AI 的执行清单)
    try:
        import actions as ACT
        Cl = C[C["周结束"] == latest].copy() if len(C) else C
        if len(Cl):
            cw_ = C.groupby(["店铺", "父ASIN", "广告活动"]).agg(窗口花费=("花费", "sum"), 窗口销售=("广告销售", "sum"), 窗口订单=("广告订单", "sum")).reset_index()
            cw_["窗口ACoS"] = cw_["窗口花费"] / cw_["窗口销售"].where(cw_["窗口销售"] > 0)
            Cl = Cl.merge(cw_, on=["店铺", "父ASIN", "广告活动"], how="left")
        cands = ACT.build_candidates(P[P["周结束"] == latest], K=K, S=S, AU=AU, C=Cl, cfg=cfg, gmatch=_gmatch)
        md.append(ACT.candidates_md(cands, latest))
        with open(os.path.join(out, f"candidates_{latest}.json"), "w", encoding="utf-8") as f:
            json.dump(ACT.to_jsonable({"week_end": latest, "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                                       "rules": ACT._cfg_rules(cfg), "candidates": cands}), f, ensure_ascii=False, indent=1)
        print(f"[pack] 候选动作 {len(cands)} 条(可选 {sum(c['可选'] for c in cands)})")
    except Exception as e:
        import traceback; traceback.print_exc()
        md.append(f"## 5. 候选动作\n候选动作生成失败：{e}。第二部分 八 节只能写人工事项。\n")
    path = os.path.join(out, f"ai_pack_{len(ws)}w_{latest}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    long.drop(columns=["_o"]).to_csv(os.path.join(out, f"ai_pack_{len(ws)}w_{latest}_parent_long.csv"), index=False, encoding="utf-8-sig")
    print(f"[pack] {path}  (周: {ws}, 约 {os.path.getsize(path)/1024:.0f} KB)")

# --------------------------------------------------------------------------- init
def cmd_init(a, cfg):
    p = os.path.join(HERE, "config.json")
    if not os.path.exists(p):
        with open(p, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
    pd.DataFrame(columns=["店铺", "父ASIN"] + SP_COLS).to_csv(os.path.join(HERE, "spapi_parent_weekly_TEMPLATE.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(columns=["款", "头程_USD"]).to_csv(os.path.join(HERE, "freight_by_style_TEMPLATE.csv"), index=False, encoding="utf-8-sig")
    print("已生成 config.json、spapi_parent_weekly_TEMPLATE.csv、freight_by_style_TEMPLATE.csv")

def main():
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--config")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init")
    i = sub.add_parser("ingest"); i.add_argument("--inputs", required=True); i.add_argument("--week-end")
    i.add_argument("--refresh-spapi", action="store_true", help="忽略SP-API缓存，重新拉取")
    i.add_argument("--no-spapi", action="store_true", help="本次不在线拉取SP-API")
    i.add_argument("--spapi-cache-only", action="store_true", help="只用 spapi_cache_dir 里已下载的报告，不请求亚马逊(离线测试用)")
    i.add_argument("--sp-wait", type=int, default=None, help="SP-API 单份报告最多等多少秒；0=只提交不等待(报告留待下次运行取回)")
    w = sub.add_parser("window", help="只打印本次各数据源的时间窗口，方便按同样日期去领星下载"); w.add_argument("--week-end")
    p = sub.add_parser("pack"); p.add_argument("--weeks", type=int, default=4); p.add_argument("--end")
    p.add_argument("--report", choices=["weekly", "monthly"], default="weekly", help="weekly=周报提示词；monthly=月报提示词(建议 --weeks 4 或 5)")
    a = ap.parse_args()
    cfg = load_config(a.config)
    {"init": cmd_init, "ingest": cmd_ingest, "pack": cmd_pack, "window": cmd_window}[a.cmd](a, cfg)

if __name__ == "__main__":
    main()
