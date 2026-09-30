# recognize.py
# 领星导出文件识别器：按表头(而不是文件名)判断每个文件是哪一类报表，
# 统一改名后放进 data/raw/<周结束日>/，并检查是否齐全、订单是否覆盖周窗口。
#
# 用法(命令行，插件也是这样调用)：
#   python recognize.py import --src data/inbox --week-end 2026-09-26          # 从收件箱识别、改名、移入 raw/<周>/
#   python recognize.py check  --week-end 2026-09-26                           # 只检查 raw/<周>/ 是否齐全
#   加 --json 输出机器可读结果(插件解析用)；--root 指定工作区(默认 data/，测试工作区为 test/)
#
# 输出：data/raw/<周>/manifest.json
#   {"week_end", "window", "files": {类型: {...}}, "missing_required", "missing_optional", "unknown", "checks", "ok"}

import argparse, glob, json, os, re, shutil, sys, warnings
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.abspath(__file__))
RAW_ROOT = os.path.join(HERE, "data", "raw")              # 工作区可用 set_root() 切换(测试工作区 test/)
SPAPI_CACHE = os.path.join(HERE, "data", "spapi_cache")


def set_root(root):
    global RAW_ROOT, SPAPI_CACHE
    RAW_ROOT = os.path.join(root, "raw")
    SPAPI_CACHE = os.path.join(root, "spapi_cache")

# 类型 -> (中文名, 标准文件名模板, 是否必需)
# 标准文件名要包含周脚本 find_file 用的关键字；快照类文件名带 _YYYYMMDD_ (脚本从这里取快照日)
TYPES = {
    "listing":   ("Listing",       "Listing{snap}.xlsx",          True),
    "product":   ("推广商品报告",   "推广商品报告.xlsx",            True),
    "campaign":  ("广告活动报告",   "广告活动报告.xlsx",            True),
    "group":     ("广告组报告",     "广告组报告.xlsx",              False),
    "placement": ("广告位报告",     "广告位报告.xlsx",              False),
    "keyword":   ("关键词报告",     "关键词报告.xlsx",              False),
    "auto":      ("自动投放报告",   "自动投放报告.xlsx",            False),
    "search":    ("用户搜索报告",   "用户搜索报告.xlsx",            False),
    "orders":    ("订单导出",       "订单导出.xlsx",                True),
    "cost":      ("导出产品(按SKU)", "导出产品_SKU.xlsx",           False),
    "shipments": ("FBA货件",        "FBA货件_{snap}_000000.xlsx",  False),
    "replenish": ("补货建议(父Asin)", "补货建议_父Asin_{snap}_000000.xlsx", False),
}
# SP-API 下载缓存：<店铺>_<报告>_<周结束日>.bin，原样放进 spapi_cache
SPAPI_RE = re.compile(r"([A-Za-z0-9]+-[A-Z]{2})_(SALES_TRAFFIC|RETURNS|INV_PLANNING|SEARCH_CATALOG_[0-9-]+|SQP_[0-9a-f]+)(?:_lag\d+)?_(\d{4}-\d{2}-\d{2})\.bin$")


def _cols(path, sheet=0, header=0):
    import pandas as pd
    try:
        return [str(c).strip() for c in pd.read_excel(path, sheet_name=sheet, nrows=0, header=header).columns]
    except Exception:
        return []


def classify(path):
    """返回 (类型 或 None, 说明)"""
    import pandas as pd
    try:
        sheets = pd.ExcelFile(path).sheet_names
    except Exception as e:
        return None, f"无法打开：{e}"
    if "货件详情" in sheets:
        c = _cols(path, "货件详情")
        if "货件单号" in c and "货件状态" in c:
            return "shipments", "FBA货件(货件详情)"
    if any("头程" in s and "国家" in s for s in sheets):
        c = _cols(path, 0)
        if "*SKU" in c or "SKU" in c:
            return "cost", "产品导出(含头程表)"
    if "今日采购清单" in sheets or "全量数据总表" in sheets:
        return None, "补货工具输出(已加工数据，非领星原始导出)，周报不使用"
    c = set(_cols(path, 0))
    if {"父ASIN汇总行", "欧洲/北美汇总行", "本地可用", "待交付"} <= c:
        return "replenish", "补货建议(父Asin视图)"
    if {"订单号", "订购日期", "订单状态", "MSKU"} <= c:
        return "orders", "订单导出"
    if {"MSKU", "FNSKU", "父ASIN", "状态", "店铺"} <= c and "7日销量" in c:
        return "listing", "Listing"
    if "店铺名称" in c and "广告活动" in c:        # 广告报表：按特征列区分
        if "用户搜索词" in c:
            return "search", "用户搜索报告"
        if "广告位" in c:
            return "placement", "广告位报告"
        if "MSKU" in c:
            return "product", "推广商品报告"
        if "关键词" in c and "匹配方式" in c:
            return "keyword", "关键词报告"
        if "投放" in c:
            return "auto", "自动投放报告"
        if "广告组" in c:
            return "group", "广告组报告"
        if "预算" in c:
            return "campaign", "广告活动报告"
    return None, "无法识别的表头：" + "、".join(list(c)[:8])


def _snap_from_name(name):
    m = re.search(r"(20\d{2})(\d{2})(\d{2})", name)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).strftime("%Y%m%d")
        except ValueError:
            return None
    return None


def window_of(week_end):
    e = datetime.strptime(week_end, "%Y-%m-%d")
    return (e - timedelta(days=6)).strftime("%Y-%m-%d"), week_end


def _check_orders(path, week_end):
    import pandas as pd
    ws, we = window_of(week_end)
    d = pd.read_excel(path, usecols=["店铺", "订购日期"])
    t = pd.to_datetime(d["订购日期"], errors="coerce").dt.strftime("%Y-%m-%d").dropna()
    if t.empty:
        return False, "订单导出没有可用的订购日期"
    lo, hi = t.min(), t.max()
    ok = lo <= ws and hi >= we
    stores = sorted(d["店铺"].dropna().astype(str).unique())
    msg = f"订单日期 {lo}~{hi}，周窗口 {ws}~{we}，店铺 {stores}"
    return ok, (msg if ok else msg + "：没有完整覆盖周窗口，请按窗口重新导出订单")


def build_manifest(week_dir, week_end, extra=None):
    files = {}
    unknown = []
    for f in sorted(glob.glob(os.path.join(week_dir, "*.xls*"))):
        if os.path.basename(f).startswith("~$"):
            continue
        t, note = classify(f)
        if t:
            if t in files:
                unknown.append({"file": os.path.basename(f), "note": f"与 {files[t]['file']} 同为{TYPES[t][0]}，重复"})
            else:
                files[t] = {"file": os.path.basename(f), "name": TYPES[t][0], "snapshot": _snap_from_name(os.path.basename(f))}
        else:
            unknown.append({"file": os.path.basename(f), "note": note})
    checks = []
    if "orders" in files:
        try:
            ok, msg = _check_orders(os.path.join(week_dir, files["orders"]["file"]), week_end)
        except Exception as e:
            ok, msg = False, f"订单检查失败：{e}"
        checks.append({"item": "订单覆盖周窗口", "ok": ok, "detail": msg})
    snaps = {k: v["snapshot"] for k, v in files.items() if k in ("listing", "shipments", "replenish") and v.get("snapshot")}
    if len(set(snaps.values())) > 1:
        checks.append({"item": "快照日一致", "ok": False, "detail": f"快照日不同：{snaps}(库存/在途/本地仓的时间点会错位)"})
    elif snaps:
        checks.append({"item": "快照日一致", "ok": True, "detail": f"快照日 {list(snaps.values())[0]}"})
    cache = sorted(os.path.basename(p) for p in glob.glob(os.path.join(SPAPI_CACHE, f"*_{week_end}.bin")))
    miss_req = [TYPES[k][0] for k, v in TYPES.items() if v[2] and k not in files]
    miss_opt = [TYPES[k][0] for k, v in TYPES.items() if not v[2] and k not in files]
    man = {"week_end": week_end, "window": list(window_of(week_end)), "files": files,
           "missing_required": miss_req, "missing_optional": miss_opt, "unknown": unknown,
           "spapi_cache": cache, "checks": checks,
           "ok": not miss_req and all(c["ok"] for c in checks),
           "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    if extra:
        man.update(extra)
    with open(os.path.join(week_dir, "manifest.json"), "w", encoding="utf-8") as fp:
        json.dump(man, fp, ensure_ascii=False, indent=2)
    return man


def do_import(src, week_end, move=True):
    """识别 src 里的文件，按标准名放进 raw/<week_end>/；SP-API .bin 放进 spapi_cache。返回 manifest"""
    week_dir = os.path.join(RAW_ROOT, week_end)
    os.makedirs(week_dir, exist_ok=True)
    os.makedirs(SPAPI_CACHE, exist_ok=True)
    moved, skipped = [], []
    today = datetime.now().strftime("%Y%m%d")
    for f in sorted(glob.glob(os.path.join(src, "*"))):
        base = os.path.basename(f)
        if not os.path.isfile(f) or base.startswith("~$"):
            continue
        m = SPAPI_RE.search(base)
        if m:
            dst = os.path.join(SPAPI_CACHE, m.group(0))
            (shutil.move if move else shutil.copy2)(f, dst)
            moved.append({"src": base, "dst": "spapi_cache/" + m.group(0), "type": "SP-API缓存"})
            continue
        if not base.lower().endswith((".xlsx", ".xls")):
            skipped.append({"file": base, "note": "不是Excel，已忽略"})
            continue
        t, note = classify(f)
        if not t:
            skipped.append({"file": base, "note": note})
            continue
        snap = _snap_from_name(base) or today
        dst_name = TYPES[t][1].format(snap=snap)
        dst = os.path.join(week_dir, dst_name)
        if os.path.exists(dst):
            os.replace(dst, dst + ".bak")          # 同类旧文件保留一份备份
        (shutil.move if move else shutil.copy2)(f, dst)
        moved.append({"src": base, "dst": dst_name, "type": TYPES[t][0], "snapshot_from_name": bool(_snap_from_name(base))})
    return build_manifest(week_dir, week_end, {"last_import": {"moved": moved, "skipped": skipped}})


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    i = sub.add_parser("import"); i.add_argument("--src", required=True); i.add_argument("--week-end", required=True)
    i.add_argument("--copy", action="store_true", help="复制而不是移动")
    c = sub.add_parser("check"); c.add_argument("--week-end", required=True)
    for p_ in (i, c):
        p_.add_argument("--json", action="store_true")
        p_.add_argument("--root", default="", help="工作区根目录(含 raw/ 与 spapi_cache/)")
    a = ap.parse_args()
    if a.root:
        set_root(os.path.abspath(a.root))
    if a.cmd == "import":
        man = do_import(a.src, a.week_end, move=not a.copy)
    else:
        wd = os.path.join(RAW_ROOT, a.week_end)
        if not os.path.isdir(wd):
            print(json.dumps({"error": f"没有文件夹 {wd}"}, ensure_ascii=False) if a.json else f"没有文件夹 {wd}")
            sys.exit(2)
        man = build_manifest(wd, a.week_end)
    if a.json:
        print(json.dumps(man, ensure_ascii=False))
    else:
        print(json.dumps(man, ensure_ascii=False, indent=2))
    sys.exit(0 if man["ok"] else 1)


if __name__ == "__main__":
    main()
