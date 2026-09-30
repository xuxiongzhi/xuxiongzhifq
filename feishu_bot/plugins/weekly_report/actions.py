# actions.py
# 候选动作(规则层) + AI 选择结果校验(校验层)，为以后通过 API 自动执行做准备。
#
#   pack 阶段：build_candidates() 用规则从数据里生成候选动作(每条带 ID、对象原名、当前值、建议值、允许范围、依据、是否可选)，
#              写进数据包第5节，并保存 candidates_<周>.json。
#   AI 阶段：  第二部分 八 节只能从候选里挑 ID(输出 JSON)，可以调整优先级、在允许范围内改建议值、写理由和验证指标。
#   校验阶段：validate() 逐条检查 ID 是否存在、是否被规则阻止、数值是否在范围内、同一对象/父体是否互相冲突；
#              render_md() 用校验结果生成 八 节表格(代替 AI 原文)，结果保存为 actions_<周>.json(通过/拦截+原因)。
#
# 广告对象用领星导出里的原名(广告活动/广告组/关键词/投放)，后续用 Ads API 按名称查 ID 再执行。

import json
import math
import re

import numpy as np
import pandas as pd

DEFAULT_RULES = {
    "min_clicks_judge": 30,          # 点击>=30 或 订单>=5 才能判断 ACoS 好坏(与提示词硬性规则2一致)
    "min_orders_judge": 5,
    "min_clicks_test": 8,            # 8~29 次点击 0 单：只做 ≤10% 的试探降价
    "bid_up_acos_ratio": 0.7,        # ACoS <= 盈亏ACoS×0.7 才考虑加价
    "bid_up_step": 0.15, "bid_up_max_step": 0.25,
    "bid_down_min_ratio": 0.6,       # 降价最多降到原价的 60%
    "test_down_step": 0.10,
    "budget_up_usage": 0.9,          # 预算使用率>=90% 才考虑加预算
    "budget_up_acos_ratio": 0.8,
    "budget_up_step": 0.3, "budget_up_max_step": 0.5,
    "throttle_max_cut": 0.5,         # 控速：预算最多砍一半
    "neg_gender_min_clicks": 3,      # 性别不符的搜索词：点击>=3 且 0 单才列否定候选
    "harvest_min_orders": 2,
    "harvest_min_clicks": 5,         # 只有1~2次点击的"出单词"多是7天归因带来的，不收割
    "air_min_units": 10,             # 建议空运件数少于这个数不单独空运(并入海运)；>=3倍时规则优先级P0，否则P1
    "sea_min_units": 5,
    "excess_min_units": 20,          # FBA冗余(或亚马逊冗余)>=此件数才列清货候选；本地滞销>=此件数才列停采/清仓
    "po_reduce_min_units": 10,
    "price_gap_check": 0.05,
    "price_floor_tol": 0.02,         # 促销价比保本价低超过2%才算亏本促销(保本价本身是估算)         # 实际售价(促销价优先)比竞品最低价高>=5% 才列核价候选              # 海运/尺码缺口少于这个数不单独列候选(随下一批一起处理)
    "block_stock_days": 21,          # 含在途天数<21 不加投(提示词硬性规则3)
    "block_promo_share": 0.3,        # 订单促销占比>30% 不加投(提示词硬性规则10)
    "size_block_30d": 0.10,          # 未来30天(含待交付)尺码缺货>=30天需求10% → 硬阻止加投(近期必然缺货，加来的流量落在缺货尺码)
    "size_block_localgap": 0.05,     # 本地仓对应尺码不足>=60天需求5% → 硬阻止(要靠工厂，短期补不上)
    "size_soft_share": 0.05,         # 60天尺码缺货>=5%但本地仓能补 → 加投必须同时选该父体的空运/海运候选
    "max_per_type": 15,              # 每类候选最多列多少条(按花费/影响排序)
    "max_blocked_per_type": 5,       # 被阻止的候选每类最多列几条(只为让 AI 知道为什么不动)
    "max_selected": 12,
}

TYPE_INFO = {   # 类型: (ID前缀, 中文名, 执行方式)
    "INV_AIR_SHIP": ("KA", "本地仓空运发FBA", "人工"),
    "INV_SEA_SHIP": ("KS", "本地仓海运发FBA", "人工"),
    "PO_FOLLOWUP": ("PO", "催交已逾期采购单", "人工"),
    "PO_NEW": ("PN", "新采购下单", "人工"),
    "PO_SIZE_GAP": ("PG", "尺码缺口：催PO/工厂直发/新采购", "人工"),
    "INV_CLEAR": ("XC", "冗余清货(优惠券/秒杀/降价，不低于保本价)", "人工(需确认)"),
    "PO_REDUCE": ("XP", "削减/延后待交付PO", "人工"),
    "LOCAL_SLOW": ("XL", "本地仓滞销尺码：停止采购/清仓", "人工"),
    "PRICE_CHECK": ("PC", "核价：实际售价高于竞品最低价", "人工"),
    "PRICE_FLOOR": ("PF", "促销价低于保本价：上调或结束促销", "人工(需确认)"),
    "AD_THROTTLE": ("CT", "控速：下调活动预算", "API(需确认)"),
    "AD_BUDGET_UP": ("BU", "上调活动预算", "API(需确认)"),
    "AD_BID_UP": ("UP", "提高竞价", "API"),
    "AD_BID_DOWN": ("DN", "降低竞价", "API"),
    "AD_BID_TEST_DOWN": ("TD", "试探降价(≤10%)", "API"),
    "AD_NEGATIVE": ("NG", "否定精准搜索词", "API(需确认)"),
    "AD_NEGATIVE_ASIN": ("NA", "否定商品(ASIN)", "API(需确认)"),
    "AD_HARVEST": ("HV", "自动词收割为手动精准词", "API(需确认)"),
}
PRIO = ["P0", "P1", "P2", "P3"]
UP_TYPES = {"AD_BID_UP", "AD_BUDGET_UP", "AD_HARVEST"}           # 会拉高销量/花费的动作
DOWN_TYPES = {"AD_BID_DOWN", "AD_BID_TEST_DOWN", "AD_THROTTLE", "AD_NEGATIVE", "AD_NEGATIVE_ASIN"}


def _f(v):
    try:
        v = float(v)
        return None if math.isnan(v) else v
    except (TypeError, ValueError):
        return None


def _r2(v):
    return None if v is None else round(float(v), 2)


def _pct(v):
    return "-" if v is None else f"{v:.1%}"


def _cfg_rules(cfg):
    r = dict(DEFAULT_RULES)
    r.update((cfg or {}).get("action_rules") or {})
    return r


# --------------------------------------------------------------------------- 父体状态
def parent_flags(r, R):
    """该父体能否加投、是否要控速。返回 dict(block=加投阻止原因列表, throttle=控速原因或None)"""
    block = []
    d_cons = _f(r.get("断货天数_保守"))
    if d_cons and d_cons > 0:
        block.append(f"断货天数_保守={d_cons:.0f}(首次断货第{_f(r.get('首次断货_天后')) or 0:.0f}天)")
    dd = _f(r.get("含在途天数_预测")) if r.get("含在途天数_预测") is not None else _f(r.get("含在途天数_7"))
    if dd is not None and dd < R["block_stock_days"]:
        block.append(f"含在途天数_7={dd:.0f}<{R['block_stock_days']}")
    if str(r.get("库存状态") or "") == "紧张":
        block.append("库存状态=紧张")
    pr = _f(r.get("订单促销占比"))
    if pr is not None and pr > R["block_promo_share"]:
        block.append(f"订单促销占比={pr:.0%}>{R['block_promo_share']:.0%}")
    s30, sgap = _f(r.get("尺码_30天缺货占比")), _f(r.get("尺码_本地仓不足占比"))
    rec = _f(r.get("主力尺码恢复有货_天后"))
    when = f"主力尺码预计第{rec:.0f}天补齐后再评估" if rec else "主力尺码靠现有在途/待交付60天内补不齐，需先发货或采购"
    if s30 is not None and s30 >= R["size_block_30d"]:
        block.append(f"未来30天尺码缺货占需求{s30:.0%}({when})")
    if sgap is not None and sgap >= R["size_block_localgap"]:
        block.append(f"本地仓对应尺码不足{_f(r.get('尺码_本地仓不足件数')) or 0:.0f}件(占60天需求{sgap:.0%}，要靠工厂/采购)")
    thr = None
    pre = _f(r.get("空运可售前无法避免断货天数"))
    if pre is not None:
        if pre > 0:
            thr = f"空运可售(第{_f(r.get('空运可售_天后')) or 0:.0f}天)前仍断货{pre:.0f}天，任何发货都补不上"
    elif d_cons and d_cons > 0 and (_f(r.get("空运最晚发货_天后")) or 0) < 0:
        thr = f"断货天数_保守={d_cons:.0f}且空运已来不及(空运最晚发货_天后<0)"
    return {"block": block, "throttle": thr}


def _be(r):
    """盈亏ACoS；没有(头程/成本缺失)就返回 None——没有盈亏线不能判断 ACoS 好坏"""
    return _f(r.get("盈亏ACoS"))


# --------------------------------------------------------------------------- 生成候选
def build_candidates(PL, K=None, S=None, AU=None, C=None, cfg=None, gmatch=None):
    """PL=最新一周父体表；K/S/AU=窗口内关键词/搜索词/自动投放；C=最新一周广告活动(含窗口订单/窗口ACoS)；
    gmatch(词, 商品性别)→一致/不符/中性。返回候选列表(dict)"""
    R = _cfg_rules(cfg)

    def _gm(term, r):
        if r is None or gmatch is None:
            return ""
        return gmatch(str(term), str(r.get("商品性别", "未知")))
    out = []
    PL = PL.copy()
    info = {(r["店铺"], r["父ASIN"]): r for _, r in PL.iterrows()}
    flags = {k: parent_flags(r, R) for k, r in info.items()}

    def add(typ, key, **kw):
        r = info.get(key)
        fl = flags.get(key, {"block": [], "throttle": None})
        c = {"类型": typ, "类型名": TYPE_INFO[typ][1], "执行方式": TYPE_INFO[typ][2],
             "店铺": key[0], "款": (r["款"] if r is not None else ""), "父ASIN": key[1],
             "广告活动": "", "广告组": "", "对象": "", "匹配方式": "", "单位": "",
             "当前值": None, "建议值": None, "允许范围": None, "规则优先级": "P2", "依据": "", "父体概况": "", "可选": True, "阻止原因": "",
             "_rank": 0.0}
        c.update(kw)
        if typ in UP_TYPES and (fl["block"] or fl["throttle"]):
            c["可选"] = False
            c["阻止原因"] = "父体有断货/库存风险，不得加投：" + "；".join(fl["block"] + ([fl["throttle"]] if fl["throttle"] else []))
        out.append(c)

    # ---- 库存：空运/海运发货、采购。有尺码级结果时以尺码级为准(亚马逊按子体判断缺货，尺码之间不能互相顶替)
    for key, r in info.items():
        dmd = _f(r.get("日均_预测")) or _f(r.get("日均7"))
        if not dmd:
            continue
        loc = _f(r.get("本地可用")) or 0
        pre = _f(r.get("空运可售前无法避免断货天数")) or 0
        d7 = _f(r.get("日均_7天")) or _f(r.get("日均7"))
        base = (f"日均_预测={dmd:.1f}(7/14/30/60天加权；近7天{d7 or 0:.1f})，FBA可售={_f(r.get('FBA可售')) or 0:.0f}，本地可用={loc:.0f}")
        size = r.get("建议空运件数_尺码合计") is not None and _f(r.get("建议空运件数_尺码合计")) is not None
        hi7 = _f(r.get("建议空运件数_尺码合计_按近7天")) if size else _f(r.get("建议空运件数_按近7天日均"))
        if size:
            lost = _f(r.get("尺码_缺货件数_含待交付")) or 0
            base += (f"；尺码级：主力尺码当前可售为0共{_f(r.get('尺码_当前断码_主力数')) or 0:.0f}个({r.get('尺码_当前断码') or '-'})，"
                     f"含待交付仍缺{lost:.0f}件(占60天需求{_f(r.get('尺码_缺货占需求比例')) or 0:.0%})，空运前无法避免缺{_f(r.get('尺码_空运前无法避免缺货件数')) or 0:.0f}件")
            air, air_need = _f(r.get("空运_本地可发件数")) or 0, _f(r.get("建议空运件数_尺码合计")) or 0
            sea, sea_need = _f(r.get("海运_本地可发件数")) or 0, _f(r.get("建议海运发货件数_尺码合计")) or 0
            air_txt, sea_txt = r.get("空运尺码明细") or "", r.get("海运尺码明细") or ""
            short_air, short_all = _f(r.get("尺码_空运本地不足件数")) or 0, _f(r.get("尺码_本地仓不足件数")) or 0
            s7 = f"；若近7天日均{d7:.1f}持续需{hi7:.0f}件" if (hi7 and hi7 > air_need + 0.5) else ""
        else:
            air_need = air = _f(r.get("建议空运件数")) or 0
            sea_need = sea = _f(r.get("建议海运发货件数")) or 0
            air_txt = sea_txt = "父体合计，需按尺码拆分"
            s7 = f"；若近7天日均{d7:.1f}持续需{hi7:.0f}件" if (hi7 and hi7 > air_need + 0.5) else ""
            short_air, short_all = _f(r.get("空运发货_本地仓不足件数")) or 0, (_f(r.get("空运发货_本地仓不足件数")) or 0) + (_f(r.get("海运发货_本地仓不足件数")) or 0)
        if air >= R["air_min_units"]:
            add("INV_AIR_SHIP", key, 对象="本地仓→FBA 空运", 单位="件", 当前值=0, 建议值=air, 允许范围=[math.ceil(air * 0.8), math.ceil(air * 1.2)],
                规则优先级="P0" if air >= 3 * R["air_min_units"] else "P1", _rank=1e6 + air,
                父体概况=base, 依据=f"空运第{_f(r.get('空运可售_天后')) or 0:.0f}天可售，撑到海运可售第{_f(r.get('海运可售_天后')) or 0:.0f}天需{air_need:.0f}件，本地仓能发{air:.0f}件：{air_txt}"
                     + s7 + (f"；空运前父体仍断货{pre:.0f}天(需同时控速)" if pre else ""))
        if sea >= R["sea_min_units"]:
            add("INV_SEA_SHIP", key, 对象="本地仓→FBA 海运", 单位="件", 当前值=0, 建议值=sea, 允许范围=[sea, math.ceil(sea * 1.2)],
                规则优先级="P1" if (size and (_f(r.get("尺码_缺货件数_含待交付")) or 0) >= 1) or (_f(r.get("断货天数_含待交付")) or 0) > 0 else "P2", _rank=1e5 + sea,
                父体概况=base, 依据=f"今天海运覆盖到第{_f(r.get('海运发货_覆盖至第N天')) or 0:.0f}天(含发货周期+安全库存)需{sea_need:.0f}件，本地仓能发{sea:.0f}件：{sea_txt}")
        if short_all >= R["sea_min_units"]:
            add("PO_SIZE_GAP", key, 对象="本地仓缺的尺码", 单位="件", 当前值=0, 建议值=short_all, 允许范围=[short_all, math.ceil(short_all * 1.3)],
                规则优先级="P0" if short_air >= R["air_min_units"] else "P1", _rank=1e4 + short_all,
                父体概况=base, 依据=f"本地仓对应尺码不够发 {short_all:.0f} 件(其中空运急需{short_air:.0f}件)：{r.get('尺码_本地仓不足明细') or '-'}；"
                     "先查这些尺码有没有待交付PO可催/工厂直发，没有再新采购")
        late = _f(r.get("待交付_已过预计到货件数")) or 0
        if late > 0:
            add("PO_FOLLOWUP", key, 对象="工厂待交付PO", 单位="件", 当前值=late, 建议值=None, 父体概况=base,
                规则优先级="P1" if (_f(r.get("断货天数_保守")) or 0) > 0 else "P2", _rank=1e4 + late,
                依据=f"已过预计到货日仍未到 {late:.0f} 件(见3f2)；断货天数_保守={_f(r.get('断货天数_保守')) or 0:.0f}，含待交付={_f(r.get('断货天数_含待交付')) or 0:.0f}(按顺延后的可售日算，实际交期不可信)")
        po_d = _f(r.get("新采购最晚下单_海运_天后"))
        gap = _f(r.get("未来60日缺口件数_含全部供给")) or 0
        if po_d is not None and gap > 0:
            add("PO_NEW", key, 对象="工厂新采购", 单位="件", 当前值=0, 建议值=gap, 允许范围=[gap, math.ceil(gap * 1.3)],
                规则优先级="P0" if po_d <= 7 else "P1", _rank=1e4 + gap,
                依据=f"全供给仍缺{gap:.0f}件(未来60日)；海运最晚下单第{po_d:.0f}天(<0=已来不及)")

    # ---- 冗余/滞销：清货促销、削减PO、本地滞销停采(与缺货并存时只针对冗余的尺码)
    for key, r in info.items():
        if (_f(r.get("在售子体数")) or 0) <= 0:
            continue
        exf, amz = _f(r.get("冗余_FBA件数")) or 0, _f(r.get("亚马逊冗余件数")) or 0
        be_p, price = _f(r.get("保本价")), _f(r.get("SP_我方价格")) or _f(r.get("均价"))
        a181, a91 = _f(r.get("SP_库龄181天以上件数")) or 0, _f(r.get("SP_库龄90天以上件数")) or 0
        sto = _f(r.get("冗余_预估月仓储费")) or 0
        ctx = (f"日均_预测={_f(r.get('日均_预测')) or 0:.1f}，FBA可售={_f(r.get('FBA可售')) or 0:.0f}；库龄91天以上{a91:.0f}件、181天以上{a181:.0f}件；"
               f"当前价{price or 0:.2f}、保本价{be_p or 0:.2f}")
        pp = [(m_, float(v_)) for m_, v_ in re.findall(r"([^、\s]+) ([\d.]+)", r.get("促销价明细") or "")]
        if max(exf, amz) >= R["excess_min_units"]:
            q = round(exf if exf >= R["excess_min_units"] else amz)
            amz_txt = r.get("亚马逊建议促销明细") or ""
            low = ""
            if amz_txt and be_p:
                ps = [float(x) for x in re.findall(r"建议价([\d.]+)", amz_txt)]
                if ps and min(ps) < be_p:
                    low = f"；亚马逊建议价最低{min(ps):.2f}低于保本价{be_p:.2f}，不宜照做"
            add("INV_CLEAR", key, 对象="冗余尺码", 单位="件", 当前值=0, 建议值=q, 允许范围=[math.ceil(q * 0.5), math.ceil(q * 1.2)],
                规则优先级="P1" if (a181 > 0 or sto >= 50) else "P2", _rank=5e3 + q, 父体概况=ctx,
                依据=f"FBA在库冗余{exf:.0f}件(超{int((cfg or {}).get('excess_fba_days', 90))}天销量，不含在途)：{(r.get('冗余_FBA明细') or '-')[:160]}；亚马逊估算冗余{amz:.0f}件；"
                     f"冗余部分下月仓储费约${sto:.0f}(按冗余件数分摊)"
                     + (f"；在途到货后再增加冗余{_f(r.get('冗余_在途将增加件数')):.0f}件，这些尺码可暂停发货" if (_f(r.get('冗余_在途将增加件数')) or 0) >= 5 else "") + low
                     + (f"；已有{len(pp)}个尺码在促销(最低{min(v_ for _, v_ in pp):.2f})，先看促销效果再加码" if pp else "") + "；促销价不得低于保本价")
        promo = r.get("促销价明细") or ""
        pp = [(m_, float(v_)) for m_, v_ in re.findall(r"([^、\s]+) ([\d.]+)", promo)]
        if pp and be_p:
            below = [(m_, v_) for m_, v_ in pp if v_ < be_p * (1 - R["price_floor_tol"])]
            if below:
                add("PRICE_FLOOR", key, 对象="促销中的尺码", 单位="个SKU", 当前值=len(below), 建议值=None, 规则优先级="P1", _rank=6e3 + len(below), 父体概况=ctx,
                    依据=f"促销价低于保本价{be_p:.2f}(不含广告与仓储费)：" + "、".join(f"{m_} {v_:.2f}" for m_, v_ in below[:10]) +
                         "；若是有意清滞销/冗余尺码可保留，否则上调促销价或结束促销")
        gap = _f(r.get("SP_价格高于竞品最低价比例"))
        if gap is not None and gap >= R["price_gap_check"]:
            add("PRICE_CHECK", key, 对象="价格", 单位="%", 当前值=round(gap * 100, 1), 建议值=None, 规则优先级="P2", _rank=2e3 + gap, 父体概况=ctx,
                依据=f"实际售价(促销价优先){_f(r.get('SP_我方实际售价')) or 0:.2f} 比竞品最低价{_f(r.get('SP_竞品最低价')) or 0:.2f} 高{gap:.1%}；"
                     f"全站转化率{_f(r.get('全站转化率')) or 0:.1%}；核对竞品是否同款同规格后再决定是否调价(不得低于保本价)")
        po_cut = _f(r.get("冗余_可削减PO件数")) or 0
        if po_cut >= R["po_reduce_min_units"]:
            add("PO_REDUCE", key, 对象="待交付PO中冗余的尺码", 单位="件", 当前值=_f(r.get("待交付")) or 0, 建议值=round(po_cut),
                允许范围=[math.ceil(po_cut * 0.5), math.ceil(po_cut)], 规则优先级="P1", _rank=4e3 + po_cut, 父体概况=ctx,
                依据=f"这些尺码算上待交付后超过180天销量：{r.get('冗余_可削减PO明细') or '-'}；与工厂协商减少或延后，未开工的优先")
        sl = _f(r.get("滞销_本地件数")) or 0
        if sl >= R["excess_min_units"]:
            add("LOCAL_SLOW", key, 对象="本地仓滞销尺码", 单位="件", 当前值=sl, 建议值=None, 规则优先级="P3", _rank=3e3 + sl, 父体概况=ctx,
                依据=f"开售与到FBA都满45天、近30天零销量的尺码：{r.get('滞销明细') or '-'}(含FBA {(_f(r.get('滞销_FBA件数')) or 0):.0f}件、本地仓{sl:.0f}件)；"
                     "停止采购这些尺码，本地仓可考虑清仓/捆绑，不要再发FBA")

    # ---- 控速(预算)与加预算：活动级，最新一周
    if C is not None and len(C):
        for _, c in C.iterrows():
            key = (c["店铺"], c["父ASIN"])
            r = info.get(key)
            if r is None or str(c.get("有效状态", "")).lower() not in ("enabled", "启用", "开启"):
                continue
            fl = flags[key]
            bud, daily, use = _f(c.get("预算")), _f(c.get("日均花费")), _f(c.get("预算使用率"))
            if not bud or not daily:
                continue
            if fl["throttle"]:
                dmd = _f(r.get("日均7")) or 0
                pre = _f(r.get("空运可售前无法避免断货天数")) or 0
                d_air = _f(r.get("空运可售_天后")) or 0
                need = pre / (d_air - 1) if d_air > 1 and pre else 0.3           # 需要少卖的比例(近似=空运前断货天数占比)
                ad_share = min(1.0, (_f(r.get("广告订单")) or 0) / ((_f(r.get("销量7")) or 0) or 1))
                cut = min(R["throttle_max_cut"], need / ad_share) if ad_share > 0 else R["throttle_max_cut"]
                cut = max(cut, 0.1)
                cur = min(bud, daily)
                add("AD_THROTTLE", key, 广告活动=c["广告活动"], 对象="活动日预算", 单位="USD/天", 当前值=_r2(bud),
                    建议值=_r2(cur * (1 - cut)), 允许范围=[_r2(cur * (1 - R["throttle_max_cut"])), _r2(cur * 0.95)],
                    规则优先级="P0", _rank=5e5 + daily,
                    依据=f"{fl['throttle']}；需少卖约{need:.0%}，广告订单占销量{ad_share:.0%}；预算{bud:.2f}、日均花费{daily:.2f}(使用率{_pct(use)})，"
                         f"按日均花费下调{cut:.0%}。预算低于日均花费才会真正限流")
            be = _be(r)
            w_ord, w_acos = _f(c.get("窗口订单")), _f(c.get("窗口ACoS"))
            if use is not None and use >= R["budget_up_usage"] and be and w_acos is not None and w_ord and w_ord >= R["min_orders_judge"] \
                    and w_acos <= be * R["budget_up_acos_ratio"]:
                add("AD_BUDGET_UP", key, 广告活动=c["广告活动"], 对象="活动日预算", 单位="USD/天", 当前值=_r2(bud),
                    建议值=_r2(bud * (1 + R["budget_up_step"])),
                    允许范围=[_r2(bud * 1.1), _r2(bud * (1 + R["budget_up_max_step"]))], 规则优先级="P2", _rank=daily,
                    依据=f"预算使用率{_pct(use)}；窗口订单{w_ord:.0f}、窗口ACoS {_pct(w_acos)} ≤ 盈亏ACoS {_pct(be)}×{R['budget_up_acos_ratio']}")

    # ---- 竞价：手动关键词 / 自动投放(窗口累计判断，用最新一周竞价)
    def bid_rules(df, grp, obj_col, label):
        if df is None or not len(df):
            return
        latest = df["周结束"].max()
        cur = df[df["周结束"] == latest].groupby(grp).agg(竞价=("竞价", "first"), 状态=("有效状态", "first")).reset_index()
        agg = df.groupby(grp).agg(点击=("点击", "sum"), 花费=("花费", "sum"), 广告销售=("广告销售", "sum"), 广告订单=("广告订单", "sum")).reset_index()
        agg = agg.merge(cur, on=grp, how="inner")
        for _, x in agg.iterrows():
            key = (x["店铺"], x["父ASIN"])
            r = info.get(key)
            bid, clk, od, spend, sales = _f(x["竞价"]), _f(x["点击"]) or 0, _f(x["广告订单"]) or 0, _f(x["花费"]) or 0, _f(x["广告销售"]) or 0
            if r is None or not bid or str(x["状态"]).lower() not in ("enabled", "启用", "开启") or spend <= 0:
                continue
            be = _be(r)
            acos = spend / sales if sales > 0 else None
            gm = x.get("性别匹配", "")
            base = dict(广告活动=x["广告活动"], 广告组=x["广告组"], 对象=f"{label} {x[obj_col]}", 匹配方式=str(x.get("匹配方式", "") or ""),
                        单位="USD", 当前值=_r2(bid))
            ev = f"窗口点击{clk:.0f}、订单{od:.0f}、花费{spend:.2f}、ACoS {_pct(acos)}、盈亏ACoS {_pct(be)}" + (f"、性别匹配={gm}" if gm else "")
            judged = clk >= R["min_clicks_judge"] or od >= R["min_orders_judge"]
            if judged and od > 0 and be and acos is not None and acos <= be * R["bid_up_acos_ratio"] and gm != "不符":
                be_cpc = sales / clk * be if clk else None            # 盈亏平衡 CPC = 每次点击销售额 × 盈亏ACoS
                hi = min(bid * (1 + R["bid_up_max_step"]), be_cpc) if be_cpc else bid * (1 + R["bid_up_max_step"])
                lo = bid * 1.05
                if hi >= lo:
                    rel = _f(r.get("转化率相对店内中位"))
                    add("AD_BID_UP", key, **base, 建议值=_r2(min(bid * (1 + R["bid_up_step"]), hi)), 允许范围=[_r2(lo), _r2(hi)],
                        规则优先级="P2", _rank=sales, 依据=ev + f"；该词CVR {od / clk:.1%}" + (f"(父体全站转化率为店内中位{rel:.2f}倍，加价只针对这个转化好的词)" if rel is not None and rel < 0.8 else "")
                        + (f"；盈亏平衡CPC={be_cpc:.2f}" if be_cpc else ""))
            elif judged and od > 0 and be and acos is not None and acos > be:
                tgt = bid * max(R["bid_down_min_ratio"], be / acos)
                add("AD_BID_DOWN", key, **base, 建议值=_r2(tgt), 允许范围=[_r2(bid * R["bid_down_min_ratio"]), _r2(bid * 0.95)],
                    规则优先级="P1" if spend >= 20 else "P2", _rank=spend, 依据=ev + f"；ACoS高于盈亏线，目标价=竞价×盈亏ACoS/ACoS")
            elif clk >= R["min_clicks_judge"] and od == 0:
                add("AD_BID_DOWN", key, **base, 建议值=_r2(bid * 0.7), 允许范围=[_r2(bid * 0.5), _r2(bid * 0.8)],
                    规则优先级="P1", _rank=spend, 依据=ev + f"；点击≥{R['min_clicks_judge']}仍0单")
            elif R["min_clicks_test"] <= clk < R["min_clicks_judge"] and od == 0:
                add("AD_BID_TEST_DOWN", key, **base, 建议值=_r2(bid * (1 - R["test_down_step"])),
                    允许范围=[_r2(bid * (1 - R["test_down_step"])), _r2(bid * 0.95)], 规则优先级="P3", _rank=spend,
                    依据=ev + f"；样本不足(点击<{R['min_clicks_judge']})，只能试探≤10%")

    if K is not None and len(K):
        k_ = K.copy()
        k_["性别匹配"] = [_gm(t, info.get((s, p))) for t, s, p in zip(k_["关键词"], k_["店铺"], k_["父ASIN"])]
        bid_rules(k_, ["店铺", "父ASIN", "广告活动", "广告组", "关键词", "匹配方式", "性别匹配"], "关键词", "关键词")
    if AU is not None and len(AU):
        bid_rules(AU, ["店铺", "父ASIN", "广告活动", "广告组", "投放"], "投放", "自动投放")

    # ---- 搜索词：否定 / 收割(父体维度判断，否定落到出过花费的广告组)
    if S is not None and len(S):
        s_ = S.copy()
        s_["_t"] = s_["用户搜索词"].astype(str).str.strip().str.lower()
        par = s_.groupby(["店铺", "父ASIN", "_t"]).agg(点击=("点击", "sum"), 花费=("花费", "sum"), 广告订单=("广告订单", "sum"),
                                                     广告销售=("广告销售", "sum")).reset_index()
        par = {(a, b, t): x for a, b, t, x in zip(par["店铺"], par["父ASIN"], par["_t"], par.to_dict("records"))}
        grp = s_.groupby(["店铺", "父ASIN", "广告活动", "广告组", "_t", "词类型", "来源"]).agg(
            点击=("点击", "sum"), 花费=("花费", "sum"), 广告订单=("广告订单", "sum"), 广告销售=("广告销售", "sum"),
            已投放=("已投放为手动词", "max")).reset_index()
        manual_camp = {}
        if K is not None and len(K):
            for (st, pa), g in K.groupby(["店铺", "父ASIN"]):
                g2 = g[g["有效状态"].astype(str).str.lower() == "enabled"]
                g2 = g2.groupby(["广告活动", "广告组"])["花费"].sum().sort_values(ascending=False)
                if len(g2):
                    manual_camp[(st, pa)] = g2.index[0]
        for _, x in grp.iterrows():
            key = (x["店铺"], x["父ASIN"])
            r = info.get(key)
            if r is None:
                continue
            tot = par[(x["店铺"], x["父ASIN"], x["_t"])]
            gm = _gm(x["_t"], r) if x["词类型"] != "ASIN" else ""
            is_asin = x["词类型"] == "ASIN" or re.fullmatch(r"b0[0-9a-z]{8}", x["_t"] or "")
            ev = (f"该组点击{x['点击']:.0f}、花费{x['花费']:.2f}、订单{x['广告订单']:.0f}；父体合计点击{tot['点击']:.0f}、订单{tot['广告订单']:.0f}"
                  + (f"；性别匹配={gm}(商品{r.get('商品性别', '未知')}装)" if gm else ""))
            neg_reason = None
            if tot["广告订单"] == 0 and x["点击"] > 0:
                if gm == "不符" and tot["点击"] >= R["neg_gender_min_clicks"]:
                    neg_reason = "搜索词性别与商品相反"
                elif tot["点击"] >= R["min_clicks_judge"]:
                    neg_reason = f"父体合计点击≥{R['min_clicks_judge']}仍0单"
            if neg_reason:
                add("AD_NEGATIVE_ASIN" if is_asin else "AD_NEGATIVE", key, 广告活动=x["广告活动"], 广告组=x["广告组"],
                    对象=x["_t"], 匹配方式="否定精准" if not is_asin else "否定商品", 规则优先级="P1" if gm == "不符" else "P2",
                    _rank=x["花费"], 依据=f"{neg_reason}；{ev}")
            if (x["来源"] == "自动" and not is_asin and tot["广告订单"] >= R["harvest_min_orders"] and tot["点击"] >= R["harvest_min_clicks"]
                    and not int(x["已投放"] or 0)
                    and gm != "不符" and x["广告订单"] >= 1):
                cpc = x["花费"] / x["点击"] if x["点击"] else None
                be = _be(r)
                be_cpc = (tot["广告销售"] / tot["点击"] * be) if (be and tot["点击"]) else None
                tgt = manual_camp.get(key)
                if cpc:
                    hi = min(cpc * 1.3, be_cpc) if be_cpc else cpc * 1.3
                    lo = cpc * 0.8
                    add("AD_HARVEST", key, 广告活动=(tgt[0] if tgt else "(需新建手动活动)"), 广告组=(tgt[1] if tgt else ""),
                        对象=x["_t"], 匹配方式="精准匹配", 单位="USD", 当前值=None, 建议值=_r2(min(max(cpc, lo), hi)),
                        允许范围=[_r2(min(lo, hi)), _r2(hi)], 规则优先级="P2", _rank=x["广告销售"],
                        依据=f"来源=自动({x['广告活动']})、尚未投放为手动词；{ev}；该词CPC={cpc:.2f}" + (f"、盈亏平衡CPC={be_cpc:.2f}" if be_cpc else ""))

    # ---- 排序、截断、编号
    res = []
    soft = {k for k, r in info.items() if (_f(r.get("尺码_缺货占需求比例")) or 0) >= R["size_soft_share"]}
    for typ, (pre, _, _) in TYPE_INFO.items():
        g = sorted([c for c in out if c["类型"] == typ], key=lambda c: -c["_rank"])
        g = ([c for c in g if c["可选"]][: R["max_per_type"]] + [c for c in g if not c["可选"]][: R["max_blocked_per_type"]])
        for i, c in enumerate(g, 1):
            c["id"] = f"{pre}{i:02d}"
            c.pop("_rank", None)
            res.append(c)
    # 条件加投：父体60天有尺码缺货但本地仓能补(没被硬阻止) → 加投类候选必须和该父体的空运/海运候选一起选
    for c in res:
        key = (c["店铺"], c["父ASIN"])
        if c["类型"] in UP_TYPES and c["可选"] and key in soft:
            dep = [x["id"] for x in res if (x["店铺"], x["父ASIN"]) == key and x["类型"] in ("INV_AIR_SHIP", "INV_SEA_SHIP") and x["可选"]]
            r = info[key]
            if dep:
                c["需同时选"] = dep
                c["依据"] += f"【前提：本父体60天尺码缺货{(_f(r.get('尺码_缺货占需求比例')) or 0):.0%}，本地仓能补，须同时执行 {'、'.join(dep)}】"
            else:
                c["可选"] = False
                c["阻止原因"] = f"本父体60天尺码缺货{(_f(r.get('尺码_缺货占需求比例')) or 0):.0%}，且没有可执行的发货候选"
    return res


# --------------------------------------------------------------------------- 数据包第5节
def _range_txt(c):
    rg = c.get("允许范围")
    return "" if not rg else f"{rg[0]}~{rg[1]}"


def candidates_md(cands, week_end=""):
    if not cands:
        return "## 5. 候选动作\n本周规则没有生成任何候选动作。第二部分 八 节的 JSON 输出空列表 []。\n"
    head = ("## 5. 候选动作(规则生成；第二部分 八 节只能从这里选 ID)\n"
            "说明：每条候选已按规则算好对象原名、当前值、建议值和允许范围；可选=否 的是被规则阻止的(原因见'阻止原因')，不得选，可在 九 节解释为什么不动。"
            "建议值可在允许范围内调整；规则优先级是默认值，可上下调一级。广告对象名=领星/广告后台原名，执行时按名称查 ID。\n")
    rows = ["| ID | 类型 | 店铺/款 | 广告活动 / 广告组 | 对象 | 匹配 | 当前值 | 建议值 | 允许范围 | 单位 | 规则优先级 | 执行方式 | 可选 | 依据 / 阻止原因 |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for c in cands:
        ag = " / ".join(x for x in (c["广告活动"], c["广告组"]) if x)
        why = c["依据"] + (f"【阻止：{c['阻止原因']}】" if not c["可选"] else "")
        rows.append("| " + " | ".join(str(v).replace("|", "/") for v in [
            c["id"], c["类型名"], f"{c['店铺']}/{c['款']}", ag, c["对象"], c["匹配方式"],
            "" if c["当前值"] is None else c["当前值"], "" if c["建议值"] is None else c["建议值"], _range_txt(c), c["单位"],
            c["规则优先级"], c["执行方式"], "是" if c["可选"] else "否", why]) + " |")
    n_ok = sum(c["可选"] for c in cands)
    return head + parent_md(cands, "### 5a. 库存类候选涉及的父体概况(每个父体只列一次)") + \
        f"\n### 5b. 候选列表：共 {len(cands)} 条，可选 {n_ok} 条，被阻止 {len(cands) - n_ok} 条。\n\n" + "\n".join(rows) + "\n"


def parent_md(items, title):
    """库存类动作的父体概况(日均、断码、缺货、本地仓)，每个父体只列一次，避免每行重复"""
    seen, L = {}, []
    for c in items:
        if c.get("父体概况") and (c["店铺"], c["款"]) not in seen:
            seen[(c["店铺"], c["款"])] = c["父体概况"]
    if not seen:
        return ""
    L += [title, "| 店铺/款 | 概况 |", "|---|---|"]
    for (st, k), v in seen.items():
        L.append(f"| {st}/{k} | {str(v).replace('|', '/')} |")
    return "\n".join(L) + "\n"


# --------------------------------------------------------------------------- 校验 AI 的选择
def extract_json(text):
    """从 AI 文本里取出第一个 ```json 代码块(或裸 JSON 数组)。返回 (对象, 代码块在原文中的 span) 或 (None, None)"""
    m = re.search(r"```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```", text, flags=re.S)
    if not m:
        return None, None
    raw = m.group(1)
    try:
        return json.loads(raw), m.span()
    except json.JSONDecodeError:
        try:   # 常见小毛病：尾逗号
            return json.loads(re.sub(r",\s*([\]}])", r"\1", raw)), m.span()
        except json.JSONDecodeError:
            return None, m.span()


def _prio_shift(p, n):
    i = PRIO.index(p) + n
    return PRIO[max(0, min(len(PRIO) - 1, i))]


def validate(sel, cands, cfg=None):
    """sel = AI 的 JSON：{"执行清单":[{id,优先级,建议值,理由,验证指标}], "人工事项":[{对象,动作,理由}]} 或直接是列表。
    返回 dict(passed=[...], blocked=[...], manual=[...])，每条附 状态 与 原因"""
    R = _cfg_rules(cfg)
    if isinstance(sel, list):
        sel = {"执行清单": sel}
    items = sel.get("执行清单") or sel.get("actions") or []
    manual = sel.get("人工事项") or sel.get("manual") or []
    by_id = {c["id"]: c for c in cands}
    passed, blocked, seen = [], [], set()
    for it in items:
        if not isinstance(it, dict):
            continue
        cid = str(it.get("id") or it.get("ID") or "").strip()
        rec = {"id": cid, "AI优先级": str(it.get("优先级") or "").upper().strip(), "AI建议值": it.get("建议值"),
               "理由": str(it.get("理由") or ""), "验证指标": str(it.get("验证指标") or "")}
        c = by_id.get(cid)
        if c is None:
            if cid.lower() in ("", "none", "null", "人工"):       # 没有 ID 的当人工事项
                manual.append({"对象": it.get("对象", ""), "动作": it.get("动作", ""), "理由": rec["理由"]})
                continue
            blocked.append({**rec, "状态": "拦截", "原因": "候选里没有这个ID(对象或动作不是规则生成的)"})
            continue
        rec.update({k: v for k, v in c.items()})
        if cid in seen:
            continue
        seen.add(cid)
        if not c["可选"]:
            blocked.append({**rec, "状态": "拦截", "原因": c["阻止原因"] or "规则阻止"})
            continue
        v = _f(it.get("建议值"))
        if c["允许范围"]:
            lo, hi = c["允许范围"]
            if it.get("建议值") in (None, ""):
                v = c["建议值"]
            elif v is None:
                blocked.append({**rec, "状态": "拦截", "原因": f"建议值不是数字：{it.get('建议值')}"})
                continue
            elif not (lo - 1e-9 <= v <= hi + 1e-9):
                blocked.append({**rec, "状态": "拦截", "原因": f"建议值{v}超出允许范围{lo}~{hi}"})
                continue
        else:
            v = c["建议值"]
        rec["最终值"] = v
        p = rec["AI优先级"] if rec["AI优先级"] in PRIO else c["规则优先级"]
        d = PRIO.index(p) - PRIO.index(c["规则优先级"])
        note = []
        if abs(d) > 1:
            p = _prio_shift(c["规则优先级"], 1 if d > 0 else -1)
            note.append(f"AI优先级{rec['AI优先级']}与规则{c['规则优先级']}相差超过一级，已调为{p}")
        rec["最终优先级"], rec["备注"] = p, "；".join(note)
        passed.append(rec)
    # ---- 冲突：同父体既控速/否定又加投；同对象既加又减
    par_down = {(x["店铺"], x["父ASIN"]) for x in passed if x["类型"] == "AD_THROTTLE"}
    obj_dir = {}
    for x in passed:
        k = (x["店铺"], x["父ASIN"], x["广告活动"], x["广告组"], x["对象"])
        obj_dir.setdefault(k, set()).add("up" if x["类型"] in UP_TYPES else ("down" if x["类型"] in DOWN_TYPES else "other"))
    neg_terms = {(x["店铺"], x["父ASIN"], x["对象"]) for x in passed if x["类型"] in ("AD_NEGATIVE", "AD_NEGATIVE_ASIN")}
    chosen = {x["id"] for x in passed}
    keep = []
    for x in passed:
        why = None
        k = (x["店铺"], x["父ASIN"], x["广告活动"], x["广告组"], x["对象"])
        if x["类型"] in UP_TYPES and (x["店铺"], x["父ASIN"]) in par_down:
            why = "同一父体已选控速，不能同时加投"
        elif {"up", "down"} <= obj_dir.get(k, set()):
            why = "同一对象同时有加和减的动作"
        elif x["类型"] == "AD_HARVEST" and (x["店铺"], x["父ASIN"], x["对象"]) in neg_terms:
            why = "同一搜索词既要否定又要收割"
        elif x.get("需同时选") and not set(x["需同时选"]) <= chosen:
            why = f"该父体有尺码缺货，加投前须先发货：需同时选 {'、'.join(sorted(set(x['需同时选']) - chosen))}"
        if why:
            blocked.append({**x, "状态": "拦截", "原因": "冲突：" + why})
        else:
            keep.append({**x, "状态": "通过", "原因": ""})
    keep.sort(key=lambda x: PRIO.index(x["最终优先级"]))
    if len(keep) > R["max_selected"]:
        for x in keep[R["max_selected"]:]:
            blocked.append({**x, "状态": "拦截", "原因": f"超过每周最多{R['max_selected']}条"})
        keep = keep[: R["max_selected"]]
    manual = [m for m in manual if isinstance(m, dict)]
    return {"passed": keep, "blocked": blocked, "manual": manual}


def _action_txt(x):
    t = x["类型"]
    v = x.get("最终值")
    if t == "LOCAL_SLOW":
        return f"{x['类型名']}({x['当前值']:.0f} 件)"
    if t in ("INV_AIR_SHIP", "INV_SEA_SHIP", "PO_NEW", "PO_SIZE_GAP", "INV_CLEAR", "PO_REDUCE"):
        return f"{x['类型名']} {v:.0f} 件" if v is not None else x["类型名"]
    if t == "PO_FOLLOWUP":
        return f"{x['类型名']}({x['当前值']:.0f} 件已过预计到货日)"
    if t in ("AD_NEGATIVE", "AD_NEGATIVE_ASIN"):
        return f"{x['类型名']}：{x['对象']}"
    if t == "AD_HARVEST":
        return f"新增精准词「{x['对象']}」竞价 {v}"
    return f"{x['类型名']}：{x['对象']} {x['当前值']} → {v} {x['单位']}".strip()


def render_md(res):
    """八 节：由校验结果生成的执行清单(代替 AI 原文的 JSON)"""
    L = ["## 八、执行建议清单(AI 从规则候选中选择，已经程序校验)"]
    P_ = res["passed"]
    if P_:
        L += ["| 优先级 | ID | 店铺/款 | 广告活动 / 广告组 | 动作 | 规则依据 | AI理由 | 验证指标 | 执行方式 |", "|---|---|---|---|---|---|---|---|---|"]
        for x in P_:
            ag = " / ".join(v for v in (x["广告活动"], x["广告组"]) if v)
            L.append("| " + " | ".join(str(v).replace("|", "/").replace("\n", " ") for v in [
                x["最终优先级"] + (f"({x['备注']})" if x.get("备注") else ""), x["id"], f"{x['店铺']}/{x['款']}", ag, _action_txt(x),
                x["依据"], (x["理由"][:80] + "…") if len(x["理由"]) > 80 else x["理由"], x["验证指标"], x["执行方式"]]) + " |")
    else:
        L.append("本周没有通过校验的执行动作。")
    pm = parent_md(P_, "### 涉及父体的库存概况")
    if pm:
        L += ["", pm]
    if res["blocked"]:
        L += ["", "### 被程序拦截的 AI 建议(不执行)", "| ID | 对象 | AI理由 | 拦截原因 |", "|---|---|---|---|"]
        for x in res["blocked"]:
            obj = f"{x.get('店铺', '')}/{x.get('款', '')} {x.get('对象', '')}".strip(" /")
            L.append("| " + " | ".join(str(v).replace("|", "/").replace("\n", " ") for v in [x["id"] or "-", obj or "-", x.get("理由", ""), x["原因"]]) + " |")
    if res["manual"]:
        L += ["", "### 人工事项(不在候选内，未经规则校验，只供人工判断)", "| 对象 | 动作 | 理由 |", "|---|---|---|"]
        for m in res["manual"]:
            L.append("| " + " | ".join(str(m.get(k, "")).replace("|", "/").replace("\n", " ") for k in ("对象", "动作", "理由")) + " |")
    return "\n".join(L) + "\n"


def to_jsonable(o):
    if isinstance(o, dict):
        return {k: to_jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [to_jsonable(v) for v in o]
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating, float)):
        return None if (isinstance(o, float) and math.isnan(o)) or (isinstance(o, np.floating) and np.isnan(o)) else float(o)
    if isinstance(o, np.bool_):
        return bool(o)
    return o


# --------------------------------------------------------------------------- AI 正文一致性检查
LINT_METRICS = {   # 关键词 -> 父体表里可作为出处的字段(单父体值、全部合计、任意两父体之和都算有出处)
    "待上架": ["货件_已签收待上架件数", "货件_入库中待收件数"],
    "缺": ["尺码_缺货件数_含待交付", "尺码_缺货件数_保守", "尺码_本地仓不足件数", "尺码_空运本地不足件数", "尺码_空运前无法避免缺货件数"],
    "冗余": ["冗余_FBA件数", "冗余_总件数", "亚马逊冗余件数"],
    "不足": ["尺码_本地仓不足件数", "尺码_空运本地不足件数"],
    "滞销": ["滞销_FBA件数", "滞销_本地件数"],
}
_LOCAL_OK = re.compile(r"本地仓[^。；\n]{0,12}(充足|足够|够用)|无需(新增)?(新)?采购|不需要新增采购|暂不需要新增采购|直接(空运|海运)?调拨[^。；\n]{0,8}避免断货")


def lint_report(text, P):
    """检查 AI 写的正文(不含程序生成的 八 节)里常见的自相矛盾与无出处数字。返回问题列表(字符串)"""
    issues = []
    body = re.sub(r"(?ms)^\s*#{1,3}\s*\**八、.*?(?=^\s*#{1,3}\s*\**九、|\Z)", "", text)
    clauses = [c for c in re.split(r"[。；\n]", body) if c.strip()]
    P = P.copy()
    num = lambda c: pd.to_numeric(P[c], errors="coerce") if c in P.columns else pd.Series(dtype=float)
    # 1) 本地仓"充足/无需采购" vs 尺码级本地仓不足
    if "尺码_本地仓不足件数" in P.columns:
        gap = P[num("尺码_本地仓不足件数") >= 5]
        if len(gap):
            for c in clauses:
                if _LOCAL_OK.search(c):
                    hit = [k for k in gap["款"].astype(str) if k in c or k[-3:] in c]
                    if hit or not re.search(r"ZJ|SZ|HX", c):
                        issues.append(f"写了本地仓够用/无需采购：「{c.strip()[:80]}」，但尺码级本地仓不足：" +
                                      "、".join(f"{a_} {b_:.0f}件" for a_, b_ in zip(gap["款"], num("尺码_本地仓不足件数")[gap.index]) if not hit or a_ in hit))
    # 2) 库存状态混称：同一句里写"偏多"，却点名了 库存状态=紧张 的父体
    if "库存状态" in P.columns:
        tight = set(P.loc[P["库存状态"] == "紧张", "款"].astype(str))
        for c in clauses:
            if "偏多" in c:
                bad = [k for k in tight if k in c or re.search(rf"/{k[-3:]}(?!\d)", c)]
                if bad:
                    issues.append(f"「{c.strip()[:80]}」把库存状态=紧张的 {'、'.join(bad)} 和'偏多'写在一起")
    # 3) 方向写反
    for c in clauses:
        if re.search(r"(断货天数|缺货件数|缺口)[^，,]{0,12}(回升|升至|提高到)", c):
            issues.append(f"「{c.strip()[:80]}」：断货天数/缺货件数应该下降，写成了回升")
    # 4) 关键词旁的件数必须有出处：数字归到它前面最近的关键词，只认单父体值或合计(合计允许±5%：可能只加了部分父体)
    singles, totals = {}, {}
    for kw, cols in LINT_METRICS.items():
        sv, tv = set(), []
        for col in cols:
            v = num(col).dropna()
            v = v[v > 0]
            sv |= set(v.round(0).astype(int))
            tv += [float(v.sum()), float(v[v >= 20].sum())]
        if kw == "待上架" and "尺码_当前断码" in P.columns:      # 主力断码里的'已到仓N件待上架'
            for t_ in P["尺码_当前断码"].dropna().astype(str):
                sv |= {int(x) for x in re.findall(r"已到仓(\d+)件", t_)}
        for dc in [c for c in P.columns if str(c).endswith("明细") and any(k in str(c) for k in ("冗余", "滞销", "缺", "不足", "尺码"))]:
            for t_ in P[dc].dropna().astype(str):          # 尺码级明细里的件数(如 HL-3XL 28)也算有出处
                sv |= {int(float(x)) for x in re.findall(r" (\d+(?:\.\d+)?)(?:、|$)", t_)}
        tv = [t for t in tv if t > 0]
        singles[kw], totals[kw] = sv, tv + [a + b for i, a in enumerate(tv) for b in tv[i + 1:]]   # 两个合计相加(如 FBA+本地)
    kws = "|".join(LINT_METRICS)
    for c in clauses:
        for m in re.finditer(r"(?<![\d,.])(\d{1,3}(?:,\d{3})+|\d{2,5})\s*件", c):
            pre = c[max(0, m.start() - 16):m.start()]
            ks = [(pre.rfind(k), k) for k in LINT_METRICS if k in pre]
            if not ks or re.search(r"[/、()（）]", pre[max(k for k, _ in ks):]):
                continue                                   # 前面没有关键词，或关键词和数字之间隔着别的对象
            kw = max(ks)[1]
            n_ = int(m.group(1).replace(",", ""))
            if not singles[kw]:
                continue
            if any(abs(n_ - v) <= 1 for v in singles[kw]) or any(abs(n_ - t) <= max(2, 0.05 * t) for t in totals[kw]):
                continue
            snip = c[max(0, m.start() - 30):m.end() + 10].strip()
            issues.append(f"「…{snip}…」里的 {n_} 件在数据包的'{kw}'相关字段里找不到出处")
    out, seen = [], set()
    for x in issues:
        if x not in seen:
            seen.add(x); out.append(x)
    return out


def lint_md(issues):
    if not issues:
        return ""
    return ("\n## 附：程序一致性检查(自动)\n以下是程序在 AI 正文里发现的疑似矛盾或无出处数字，引用前请人工核对：\n" +
            "\n".join(f"- {x}" for x in issues) + "\n")
