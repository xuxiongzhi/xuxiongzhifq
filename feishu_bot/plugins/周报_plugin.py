# plugins/周报_plugin.py
# 亚马逊周报插件：领星数据(按周文件夹) + SP-API → 父体周宽表 / AI 数据包 → 飞书卡片与附件
# ────────────────────────────────────────────────────────────────
#  数据引擎在 plugins/weekly_report/(weekly_pipeline_spapi.py、recognize.py、config.json)，
#  本插件只负责指令、进度回复、卡片与推送；重活用子进程跑，避免阻塞店铺后端、也不占插件进程内存。
#
#  目录：
#    weekly_report/data/inbox/          ← 领星原始导出(文件名随意)与 SP-API 下载(.bin)放这里
#    weekly_report/data/raw/<周结束日>/  ← /周报 导入 后按表头识别、统一改名存放(含 manifest.json)
#    weekly_report/data/spapi_cache/    ← SP-API 报告缓存
#    weekly_report/data/weekly.db       ← 历史库(多周趋势、月报)
#    weekly_report/out/                 ← weekly_<周>.xlsx / weekly_parent_<周>.csv / ai_pack_*.md / 日志
#
#  第一期(当前)：导入/检查/生成/测试/发送；AI 周报、定时、月报在后续版本加入。
# ────────────────────────────────────────────────────────────────

import os
import sys
import re
import json
import glob
import threading
import subprocess
import logging
from datetime import datetime, timedelta

try:
    import requests
except ImportError:
    requests = None

try:
    import config          # 店铺后端的 config.py：GATEWAY_BASE_URL / CHAT_ID / STORE_NAME
except ImportError:
    config = None

# ── 插件元信息 ─────────────────────────────────────────────────
TRIGGERS    = ["/周报"]
PRIORITY    = 30
DESCRIPTION = "亚马逊周报：领星周数据+SP-API合并，生成父体周宽表与AI数据包，推送飞书"

HELP_CATEGORY = "数据报表"
HELP_EMOJI    = "📊"
HELP_GROUP    = "亚马逊周报"

HELP_DETAIL = [
    "/周报              查看本周进度：文件是否齐全、上次生成结果",
    "/周报 窗口          领星各报表该下载哪几天(周日~周六)",
    "/周报 导入 [日期]    识别 inbox 里的领星文件，改名放进该周文件夹",
    "/周报 检查 [日期]    检查该周文件是否齐全、订单是否覆盖整周",
    "/周报 生成 [日期]    拉SP-API并生成周宽表与AI数据包",
    "/周报 测试 [日期]    离线生成：只用已下载的SP-API缓存，不请求亚马逊",
    "/周报 发送 [日期]    把该周的周宽表/数据包作为附件发到群里",
]

HELP_TIPS = [
    "💡 日期=周结束日(周六)，不填则自动取最近一个数据已发布的周六",
    "💡 领星文件名随意，按表头自动识别；放进 weekly_report/data/inbox 后发 /周报 导入",
    "💡 同一时间只运行一个周报任务，生成约需1~40分钟(取决于SP-API是否已缓存)",
]

# ── 路径与参数 ─────────────────────────────────────────────────
_HERE       = os.path.dirname(os.path.abspath(__file__))
ENGINE_DIR  = os.path.join(_HERE, "weekly_report")
SCRIPT      = os.path.join(ENGINE_DIR, "weekly_pipeline_spapi.py")
RECOGNIZER  = os.path.join(ENGINE_DIR, "recognize.py")
DATA_DIR    = os.path.join(ENGINE_DIR, "data")
INBOX_DIR   = os.path.join(DATA_DIR, "inbox")
RAW_ROOT    = os.path.join(DATA_DIR, "raw")
OUT_DIR     = os.path.join(ENGINE_DIR, "out")
LOG_DIR     = os.path.join(DATA_DIR, "logs")
STATE_FILE  = os.path.join(DATA_DIR, "周报_state.json")
for _d in (INBOX_DIR, RAW_ROOT, OUT_DIR, LOG_DIR):
    os.makedirs(_d, exist_ok=True)

PACK_WEEKS      = 4        # 数据包包含最近几周(趋势判断用；库里不足时自动按实际周数)
INGEST_TIMEOUT  = 3600     # SP-API 总时限2400秒 + 读表余量
SPAPI_LAG_DAYS  = 2        # 与 config.json 的 spapi_sqp_lag_days 一致：周六过后至少2天才算数据已发布

_run_lock = threading.Lock()
_running  = {"task": None, "since": None}


# ════════════════════════════════════════════════════════════════
#  工具函数
# ════════════════════════════════════════════════════════════════

def _env() -> dict:
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    return env


def _read_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_state(patch: dict):
    data = _read_state()
    data.update(patch)
    tmp = STATE_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        logging.error(f"[周报] 状态写入失败: {e}")


def _default_week_end(today=None) -> str:
    """最近一个已结束且数据已发布的周六(与周报脚本 default_week_end 相同)"""
    d = (today or datetime.now().date()) - timedelta(days=SPAPI_LAG_DAYS)
    return (d - timedelta(days=(d.weekday() - 5) % 7)).strftime("%Y-%m-%d")


def _parse_week(arg: str) -> tuple[str | None, str]:
    """从参数里取日期；支持 2026-09-26 / 20260926 / 9-26。返回 (周结束日, 错误信息)"""
    arg = (arg or "").strip()
    if not arg:
        return _default_week_end(), ""
    m = re.search(r"(20\d{2})[-/.]?(\d{1,2})[-/.]?(\d{1,2})", arg) or re.search(r"(\d{1,2})[-/.](\d{1,2})", arg)
    try:
        if m and len(m.groups()) == 3:
            d = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
        elif m:
            d = datetime(datetime.now().year, int(m.group(1)), int(m.group(2))).date()
        else:
            return None, f"看不懂日期：{arg}，请用 2026-09-26 这样的格式"
    except ValueError:
        return None, f"日期不存在：{arg}"
    if d.weekday() != 5:
        return None, f"{d} 是周{'一二三四五六日'[d.weekday()]}，周结束日需要是周六"
    return d.strftime("%Y-%m-%d"), ""


def _run(args: list, timeout: int, log_name: str) -> tuple[int, str]:
    """子进程运行，stdout+stderr 写入日志文件；返回 (退出码, 输出尾部)"""
    log_path = os.path.join(LOG_DIR, log_name)
    try:
        proc = subprocess.run([sys.executable] + args, cwd=ENGINE_DIR, env=_env(),
                              capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout)
        out = (proc.stdout or "") + ("\n[stderr]\n" + proc.stderr if proc.stderr else "")
        code = proc.returncode
    except subprocess.TimeoutExpired as e:
        out, code = f"超时(>{timeout}秒)\n{e.stdout or ''}", -9
    except Exception as e:
        out, code = f"启动失败：{e}", -1
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"\n===== {datetime.now():%Y-%m-%d %H:%M:%S} {' '.join(args)} (exit {code}) =====\n{out}\n")
    except Exception:
        pass
    return code, out


def _tail(text: str, n: int = 12) -> str:
    lines = [l for l in (text or "").splitlines() if l.strip()]
    return "\n".join(lines[-n:])


def _card(title: str, blocks: list, template: str = "blue") -> str:
    """blocks: markdown 字符串或 'hr'"""
    els = [{"tag": "hr"} if b == "hr" else {"tag": "markdown", "content": b} for b in blocks if b]
    return json.dumps({"config": {"wide_screen_mode": True},
                       "header": {"title": {"tag": "plain_text", "content": title}, "template": template},
                       "elements": els}, ensure_ascii=False)


def _load_manifest(week_end: str) -> dict | None:
    p = os.path.join(RAW_ROOT, week_end, "manifest.json")
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _manifest_md(man: dict) -> str:
    files = man.get("files", {})
    lines = [f"**周窗口** {man['window'][0]} ~ {man['window'][1]}",
             f"**已识别** {len(files)} 类：" + "、".join(v["name"] for v in files.values())]
    if man.get("missing_required"):
        lines.append(f"🔴 **缺必需文件**：{'、'.join(man['missing_required'])}")
    if man.get("missing_optional"):
        lines.append(f"🟡 缺可选文件：{'、'.join(man['missing_optional'])}")
    for c in man.get("checks", []):
        lines.append(("✅ " if c["ok"] else "⚠️ ") + f"{c['item']}：{c['detail']}")
    lines.append(f"SP-API 缓存：{len(man.get('spapi_cache', []))} 份")
    if man.get("unknown"):
        lines.append("❔ 未使用的文件：" + "；".join(f"{u['file']}({u['note'][:30]})" for u in man["unknown"][:5]))
    return "\n".join(lines)


def _outputs(week_end: str) -> list:
    names = [f"weekly_{week_end}.xlsx", f"weekly_parent_{week_end}.csv"]
    names += [os.path.basename(p) for p in sorted(glob.glob(os.path.join(OUT_DIR, f"ai_pack_*w_{week_end}.md")))]
    return [os.path.join(OUT_DIR, n) for n in names if os.path.exists(os.path.join(OUT_DIR, n))]


# ── 飞书推送(店铺后端进程不能 import feishu_gateway，一律走网关 HTTP 代理) ──

def _gw() -> str:
    return getattr(config, "GATEWAY_BASE_URL", "http://127.0.0.1:8000") if config else "http://127.0.0.1:8000"


def _send_file(path: str, message_id: str = "", chat_id: str = "") -> tuple[bool, str]:
    """通过网关 /gw_proxy/send_file 发送附件(需网关新增该接口，见 docs/gateway_send_file.md)"""
    if requests is None:
        return False, "缺少 requests"
    try:
        r = requests.post(f"{_gw()}/gw_proxy/send_file",
                          json={"file_path": os.path.abspath(path), "message_id": message_id, "chat_id": chat_id}, timeout=120)
        if r.status_code == 404:
            return False, "网关还没有 /gw_proxy/send_file 接口"
        data = r.json()
        return (data.get("ok", False), data.get("error", ""))
    except Exception as e:
        return False, str(e)


# ════════════════════════════════════════════════════════════════
#  主入口
# ════════════════════════════════════════════════════════════════

def handle(message_id: str, text: str, reply_fn, user_id: str | None = None) -> bool:
    text = (text or "").strip()
    if not text.startswith("/周报"):
        return False
    rest = text[len("/周报"):].strip()
    sub, _, arg = rest.partition(" ")
    sub = sub.strip()

    if sub in ("", "状态"):
        reply_fn(message_id, _status_card(arg))
        return True
    if sub == "窗口":
        _start(message_id, reply_fn, "窗口", _do_window, arg, exclusive=False)
        return True
    if sub in ("导入", "检查", "生成", "测试", "发送"):
        fn = {"导入": _do_import, "检查": _do_check, "生成": _do_build, "测试": _do_build, "发送": _do_send}[sub]
        _start(message_id, reply_fn, sub, fn, arg, exclusive=sub in ("导入", "生成", "测试"))
        return True
    reply_fn(message_id, "用法：\n" + "\n".join(HELP_DETAIL))
    return True


def _start(message_id, reply_fn, name, fn, arg, exclusive=True):
    week_end, err = _parse_week(arg)
    if err:
        reply_fn(message_id, f"❌ {err}")
        return
    if exclusive:
        if not _run_lock.acquire(blocking=False):
            since = _running["since"] or ""
            reply_fn(message_id, f"⏳ 已有周报任务在运行：{_running['task']}(开始于 {since})，请稍后再试")
            return
        _running.update(task=f"{name} {week_end}", since=datetime.now().strftime("%H:%M:%S"))

    def _worker():
        try:
            fn(message_id, reply_fn, week_end, name)
        except Exception as e:
            logging.exception(f"[周报] {name} 异常")
            reply_fn(message_id, f"❌ 周报{name}异常：{type(e).__name__}: {e}")
        finally:
            if exclusive:
                _running.update(task=None, since=None)
                _run_lock.release()

    threading.Thread(target=_worker, daemon=True, name=f"周报-{name}").start()


# ════════════════════════════════════════════════════════════════
#  各子指令
# ════════════════════════════════════════════════════════════════

def _status_card(arg: str) -> str:
    week_end, err = _parse_week(arg)
    if err:
        return f"❌ {err}"
    st = _read_state()
    man = _load_manifest(week_end)
    inbox = [f for f in os.listdir(INBOX_DIR) if not f.startswith(".")]
    blocks = [f"**周结束日** {week_end}(不填日期时自动取最近一个数据已发布的周六)"]
    if _running["task"]:
        blocks.append(f"⏳ 运行中：{_running['task']}(开始于 {_running['since']})")
    blocks.append("hr")
    blocks.append(_manifest_md(man) if man else f"📂 该周还没有导入文件。收件箱里有 {len(inbox)} 个文件，发 /周报 导入 {week_end}")
    outs = _outputs(week_end)
    last = st.get("last_build", {})
    blocks.append("hr")
    if outs:
        blocks.append("**已生成**：" + "、".join(os.path.basename(p) for p in outs)
                      + (f"\n上次：{last.get('mode', '')} {last.get('at', '')}" if last.get("week_end") == week_end else ""))
    else:
        blocks.append("还没有生成结果：文件齐全后发 /周报 生成(或离线 /周报 测试)")
    return _card(f"📊 周报进度 {week_end}", blocks)


def _do_window(message_id, reply_fn, week_end, _name):
    code, out = _run([SCRIPT, "window", "--week-end", week_end], 60, "window.log")
    if code != 0:
        reply_fn(message_id, f"❌ 获取窗口失败：\n{_tail(out)}")
        return
    lines = [l for l in out.strip().splitlines() if "放进文件夹" not in l]     # 去掉命令行用法提示，换成插件用法
    lines.append(f"  · 文件名随意，放进 {INBOX_DIR} 后发 /周报 导入 {week_end}")
    reply_fn(message_id, "\n".join(lines))


def _do_import(message_id, reply_fn, week_end, _name):
    inbox = [f for f in os.listdir(INBOX_DIR) if os.path.isfile(os.path.join(INBOX_DIR, f))]
    if not inbox:
        reply_fn(message_id, f"📭 收件箱是空的：{INBOX_DIR}\n请把领星导出文件(和SP-API下载的 .bin)放进去再发 /周报 导入")
        return
    reply_fn(message_id, f"⏳ 正在识别 {len(inbox)} 个文件(按表头判断类型)...")
    code, out = _run([RECOGNIZER, "import", "--src", INBOX_DIR, "--week-end", week_end, "--json"], 600, "recognize.log")
    man = _load_manifest(week_end)
    if not man:
        reply_fn(message_id, f"❌ 识别失败：\n{_tail(out)}")
        return
    li = man.get("last_import", {})
    moved = li.get("moved", [])
    blocks = [f"已归档 {len(moved)} 个文件到 raw/{week_end}/：",
              "\n".join(f"· {m['type']} ← {m['src'][:40]}" for m in moved[:20])]
    nosnap = [m["type"] for m in moved if m.get("snapshot_from_name") is False and m["type"] in ("Listing", "FBA货件", "补货建议(父Asin)")]
    if nosnap:
        blocks.append(f"⚠️ {'、'.join(nosnap)} 的文件名里没有日期，快照日按今天记录")
    if li.get("skipped"):
        blocks.append("未使用：" + "；".join(f"{s['file'][:30]}({s['note'][:30]})" for s in li["skipped"][:8]))
    blocks += ["hr", _manifest_md(man)]
    blocks.append("✅ 必需文件齐全，可以发 /周报 生成" if man["ok"] else "⚠️ 还不能生成：补齐上面标红的文件后再导入")
    reply_fn(message_id, _card(f"📥 周报导入 {week_end}", blocks, "green" if man["ok"] else "orange"))


def _do_check(message_id, reply_fn, week_end, _name):
    code, out = _run([RECOGNIZER, "check", "--week-end", week_end, "--json"], 300, "recognize.log")
    man = _load_manifest(week_end)
    if not man:
        reply_fn(message_id, f"📂 {week_end} 还没有导入任何文件。把文件放进收件箱后发 /周报 导入 {week_end}")
        return
    reply_fn(message_id, _card(f"🔍 周报文件检查 {week_end}", [_manifest_md(man)], "green" if man["ok"] else "orange"))


def _do_build(message_id, reply_fn, week_end, name):
    man = _load_manifest(week_end)
    if not man:
        reply_fn(message_id, f"📂 {week_end} 还没有导入文件，请先 /周报 导入 {week_end}")
        return
    if man.get("missing_required"):
        reply_fn(message_id, f"❌ 缺必需文件：{'、'.join(man['missing_required'])}，无法生成")
        return
    offline = name == "测试"
    t0 = datetime.now()
    reply_fn(message_id, f"⏳ 正在生成 {week_end} 周报数据({'离线：只用SP-API缓存' if offline else '含SP-API拉取，最长约40分钟'})...")
    args = [SCRIPT, "ingest", "--inputs", os.path.join(RAW_ROOT, week_end), "--week-end", week_end]
    if offline:
        args.append("--spapi-cache-only")
    code, out = _run(args, INGEST_TIMEOUT, f"ingest_{week_end}.log")
    if code != 0:
        reply_fn(message_id, f"❌ 合并数据失败(exit {code})：\n{_tail(out, 15)}")
        return
    code, out2 = _run([SCRIPT, "pack", "--weeks", str(PACK_WEEKS), "--end", week_end], 600, f"pack_{week_end}.log")
    if code != 0:
        reply_fn(message_id, f"❌ 数据包生成失败(exit {code})：\n{_tail(out2, 15)}")
        return
    secs = int((datetime.now() - t0).total_seconds())
    _write_state({"last_build": {"week_end": week_end, "mode": "离线测试" if offline else "正式", "at": datetime.now().strftime("%Y-%m-%d %H:%M"), "seconds": secs}})
    reply_fn(message_id, _summary_card(week_end, secs, offline))


def _do_send(message_id, reply_fn, week_end, _name):
    outs = _outputs(week_end)
    if not outs:
        reply_fn(message_id, f"📭 {week_end} 还没有生成结果，请先 /周报 生成")
        return
    fails = []
    for p in outs:
        ok, err = _send_file(p, message_id=message_id)
        if not ok:
            fails.append((os.path.basename(p), err))
    if fails:
        reply_fn(message_id, "⚠️ 附件发送失败：\n" + "\n".join(f"· {n}：{e}" for n, e in fails)
                 + f"\n\n文件在：{OUT_DIR}")


# ════════════════════════════════════════════════════════════════
#  结果摘要卡片(读 CSV/xlsx，在插件进程里只做轻量解析)
# ════════════════════════════════════════════════════════════════

def _summary_card(week_end: str, secs: int, offline: bool) -> str:
    import pandas as pd
    blocks = []
    csv = os.path.join(OUT_DIR, f"weekly_parent_{week_end}.csv")
    try:
        P = pd.read_csv(csv, encoding="utf-8-sig")
    except Exception as e:
        return _card(f"📊 周报数据 {week_end}", [f"已生成，但读取结果失败：{e}"], "orange")

    # 店铺汇总(金额均为USD)
    g = P.groupby("店铺").agg(销量=("销量7", "sum"), 销售额=("销售额7", "sum"), 广告花费=("广告花费", "sum"), 广告销售=("广告销售", "sum"))
    rows = []
    for s, r in g.iterrows():
        if not (r["销量"] or r["广告花费"]):
            continue
        acos = f"{r['广告花费'] / r['广告销售']:.1%}" if r["广告销售"] else "-"
        tacos = f"{r['广告花费'] / r['销售额']:.1%}" if r["销售额"] else "-"
        rows.append(f"| {s} | {r['销量']:.0f} | ${r['销售额']:,.0f} | ${r['广告花费']:,.0f} | {acos} | {tacos} |")
    if rows:
        blocks.append("**店铺汇总**\n| 店铺 | 销量 | 销售额 | 广告花费 | ACoS | TACoS |\n|---|---|---|---|---|---|\n" + "\n".join(rows))

    # 断货风险：保守断货>0 或 建议海运发货>0
    if "断货天数_保守" in P.columns:
        risk = P[(P["断货天数_保守"].fillna(0) > 0) | (P.get("建议海运发货件数", pd.Series(0, index=P.index)).fillna(0) > 0)]
        risk = risk.sort_values("首次断货_天后", na_position="last").head(8)
        if len(risk):
            lines = []
            for _, r in risk.iterrows():
                seg = [f"**{r['款']}**({r['店铺'].split('-')[-1]})"]
                if pd.notna(r.get("首次断货_天后")):
                    seg.append(f"第{int(r['首次断货_天后'])}天断货，保守断{int(r['断货天数_保守'])}天")
                if pd.notna(r.get("断货天数_全供给_空运")) and r["断货天数_全供给_空运"] > 0:
                    seg.append(f"全供给空运仍断{int(r['断货天数_全供给_空运'])}天")
                if r.get("建议海运发货件数", 0) and r["建议海运发货件数"] > 0:
                    seg.append(f"建议海运发 {int(r['建议海运发货件数'])} 件")
                lines.append("· " + "，".join(seg))
            sim0 = P["模拟起算日"].dropna().iloc[0] if "模拟起算日" in P.columns and P["模拟起算日"].notna().any() else ""
            blocks.append(f"**断货风险**(父体合计，需按尺码核对{('；从 ' + str(sim0) + ' 起算') if sim0 else ''})\n" + "\n".join(lines))

    # 数据质量
    try:
        Q = pd.read_excel(os.path.join(OUT_DIR, f"weekly_{week_end}.xlsx"), sheet_name="数据质量")
        bad = Q[Q["级别"].isin(["ERROR", "WARN"])]
        if len(bad):
            blocks.append(f"**数据质量**：{int((Q['级别'] == 'ERROR').sum())} 个错误、{int((Q['级别'] == 'WARN').sum())} 个警告\n"
                          + "\n".join(f"· [{r['级别']}] {r['检查项']}" for _, r in bad.head(8).iterrows()))
    except Exception:
        pass

    outs = _outputs(week_end)
    blocks += ["hr", "**文件**：" + "、".join(os.path.basename(p) for p in outs) + "\n发 /周报 发送 获取附件"]
    note = "离线测试(SP-API 只用缓存)" if offline else "正式生成"
    blocks.insert(0, f"{note}，用时 {secs // 60} 分 {secs % 60} 秒；金额=USD；周窗口 {(datetime.strptime(week_end, '%Y-%m-%d') - timedelta(days=6)):%Y-%m-%d} ~ {week_end}")
    return _card(f"📊 周报数据 {week_end}", blocks, "blue")
