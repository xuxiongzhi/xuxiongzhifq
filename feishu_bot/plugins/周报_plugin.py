# plugins/周报_plugin.py
# 亚马逊周报插件：领星周数据 + SP-API → 周宽表与AI数据包 → AI 生成最终周报 → 周报文件发群 + 概述
# ────────────────────────────────────────────────────────────────
#  数据引擎在 plugins/weekly_report/(weekly_pipeline_spapi.py、recognize.py、config.json)，
#  本插件只负责指令、AI 调用、推送与定时；数据处理用子进程跑，不阻塞店铺后端。
#
#  两个工作区，互不影响：
#    正式 weekly_report/data/  inbox/(领星导出放这里) raw/<周>/ spapi_cache/ weekly.db  → 输出 weekly_report/out/
#    测试 weekly_report/test/  inputs/<周>/(固定测试文件：领星导出+SP-API .bin) raw/ spapi_cache/ weekly.db out/
#         测试只用已下载的 SP-API 缓存，不请求亚马逊；每次测试从 inputs 复制，原文件不动，可反复跑。
#
#  群里只发 AI 生成的最终周报文件(周报_<周>.md)，结束发一条概述；周宽表/数据包保存在 out/ 备查。
#  定时：每周五 13:00(北京时间)跑上一周(周日~周六)，默认关闭，领星自动化接入后 /周报 定时 开启。
# ────────────────────────────────────────────────────────────────

import os
import sys
import re
import json
import glob
import shutil
import threading
import subprocess
import logging
import time
import uuid
from datetime import datetime, timedelta

try:
    import config          # 店铺后端 config.py：CHAT_ID 等
except ImportError:
    config = None

try:
    import ai_runner
    _HAS_AI = True
except Exception:
    ai_runner = None
    _HAS_AI = False

# ── 插件元信息 ─────────────────────────────────────────────────
TRIGGERS    = ["/周报"]
PRIORITY    = 30
DESCRIPTION = "亚马逊周报：领星周数据+SP-API合并分析，AI生成周报文件发群并给出概述；每周五13:00可定时"

HELP_CATEGORY = "数据报表"
HELP_EMOJI    = "📊"
HELP_GROUP    = "亚马逊周报"

HELP_DETAIL = [
    "/周报              查看进度：本周文件是否齐全、周报是否已生成、定时状态",
    "/周报 测试 [日期]    用测试文件夹的固定数据跑完整流程(只用SP-API缓存)，发周报+概述",
    "/周报 窗口          领星各报表该下载哪几天(周日~周六)",
    "/周报 导入 [日期]    识别 inbox 里的领星文件，改名放进该周文件夹",
    "/周报 检查 [日期]    检查该周文件是否齐全、订单是否覆盖整周",
    "/周报 生成 [日期]    正式生成：拉SP-API→合并→AI周报→发文件+概述",
    "/周报 AI [日期]      只重跑AI周报(数据已生成时)，发文件+概述",
    "/周报 发送 [日期]    重发该周的AI周报文件",
    "/周报 绑定          定时周报推送到本群",
    "/周报 定时 开启|关闭  每周五13:00(北京时间)自动跑上一周",
]

HELP_TIPS = [
    "💡 日期=周结束日(周六)；正式指令不填则取最近一个数据已发布的周六，测试指令不填则取测试文件夹里最新的一周",
    "💡 测试文件放 weekly_report/test/inputs/<周结束日>/，文件名随意，按表头识别",
    "💡 群里只发AI最终周报；周宽表、数据包保存在 out/ 备查",
]

# ── 路径与参数 ─────────────────────────────────────────────────
_HERE      = os.path.dirname(os.path.abspath(__file__))
ENGINE_DIR = os.path.join(_HERE, "weekly_report")
SCRIPT     = os.path.join(ENGINE_DIR, "weekly_pipeline_spapi.py")
RECOGNIZER = os.path.join(ENGINE_DIR, "recognize.py")
BASE_CFG   = os.path.join(ENGINE_DIR, "config.json")
LOG_DIR    = os.path.join(ENGINE_DIR, "logs")
STATE_FILE = os.path.join(ENGINE_DIR, "周报_state.json")

WS = {
    "正式": {"root": os.path.join(ENGINE_DIR, "data"), "out": os.path.join(ENGINE_DIR, "out"),
             "inbox": os.path.join(ENGINE_DIR, "data", "inbox"), "cache_only": False},
    "测试": {"root": os.path.join(ENGINE_DIR, "test"), "out": os.path.join(ENGINE_DIR, "test", "out"),
             "inputs": os.path.join(ENGINE_DIR, "test", "inputs"), "cache_only": True},
}
for _w in WS.values():
    for _k in ("root", "out", "inbox", "inputs"):
        if _w.get(_k):
            os.makedirs(_w[_k], exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

PACK_WEEKS     = 4          # 数据包含最近几周(趋势用；库里不足按实际)
INGEST_TIMEOUT = 3600       # SP-API 总时限2400秒 + 读表余量
AI_TIMEOUT     = 900
SPAPI_LAG_DAYS = 2          # 与 config.json 的 spapi_sqp_lag_days 一致
SCHED_WEEKDAY  = 4          # 周五(Monday=0)
SCHED_HOUR     = 13         # 北京时间
SCHED_MINUTE   = 0
BJ_OFFSET      = timedelta(hours=8)

_run_lock = threading.Lock()
_running  = {"task": None, "since": None}
_INSTANCE_ID  = str(uuid.uuid4())
_sched_thread = None


# ════════════════════════════════════════════════════════════════
#  基础工具
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


def _bj_now() -> datetime:
    return datetime.utcnow() + BJ_OFFSET


def _default_week_end(today=None) -> str:
    """最近一个已结束且数据已发布的周六(与周报脚本 default_week_end 相同)"""
    d = (today or _bj_now().date()) - timedelta(days=SPAPI_LAG_DAYS)
    return (d - timedelta(days=(d.weekday() - 5) % 7)).strftime("%Y-%m-%d")


def _latest_test_week() -> str | None:
    ds = sorted(d for d in os.listdir(WS["测试"]["inputs"])
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) and os.path.isdir(os.path.join(WS["测试"]["inputs"], d)))
    return ds[-1] if ds else None


def _parse_week(arg: str, default: str | None) -> tuple[str | None, str]:
    """参数里的日期(2026-09-26 / 20260926 / 9-26)；空则用 default。返回 (周结束日, 错误)"""
    arg = (arg or "").strip()
    if not arg:
        return (default, "") if default else (None, "没有可用的周")
    m = re.search(r"(20\d{2})[-/.]?(\d{1,2})[-/.]?(\d{1,2})", arg)
    try:
        if m:
            d = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
        else:
            m = re.search(r"(\d{1,2})[-/.](\d{1,2})", arg)
            if not m:
                return None, f"看不懂日期：{arg}，请用 2026-09-26 这样的格式"
            d = datetime(_bj_now().year, int(m.group(1)), int(m.group(2))).date()
    except ValueError:
        return None, f"日期不存在：{arg}"
    if d.weekday() != 5:
        return None, f"{d} 是周{'一二三四五六日'[d.weekday()]}，周结束日需要是周六"
    return d.strftime("%Y-%m-%d"), ""


def _ws_config(ws: dict) -> str:
    """生成该工作区的运行配置(在 config.json 基础上改路径)，返回文件路径"""
    with open(BASE_CFG, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg["db_path"] = os.path.join(ws["root"], "weekly.db")
    cfg["out_dir"] = ws["out"]
    cfg["spapi_cache_dir"] = os.path.join(ws["root"], "spapi_cache")
    for k, v in (cfg.get("spapi_accounts") or {}).items():       # 凭证路径按引擎目录解析成绝对路径
        if v and not os.path.isabs(v):
            cfg["spapi_accounts"][k] = os.path.normpath(os.path.join(ENGINE_DIR, v))
    p = os.path.join(ws["root"], "config_run.json")
    with open(p, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    return p


def _run(args: list, timeout: int, log_name: str) -> tuple[int, str]:
    """子进程运行；stdout+stderr 追加到 logs/；返回 (退出码, 输出)"""
    try:
        proc = subprocess.run([sys.executable] + args, cwd=ENGINE_DIR, env=_env(), capture_output=True,
                              text=True, encoding="utf-8", errors="replace", timeout=timeout)
        out = (proc.stdout or "") + ("\n[stderr]\n" + proc.stderr if proc.stderr else "")
        code = proc.returncode
    except subprocess.TimeoutExpired as e:
        out, code = f"超时(>{timeout}秒)\n{e.stdout or ''}", -9
    except Exception as e:
        out, code = f"启动失败：{e}", -1
    try:
        with open(os.path.join(LOG_DIR, log_name), "a", encoding="utf-8") as f:
            f.write(f"\n===== {datetime.now():%Y-%m-%d %H:%M:%S} {' '.join(os.path.basename(a) for a in args[:2])} "
                    f"{' '.join(args[2:])} (exit {code}) =====\n{out}\n")
    except Exception:
        pass
    return code, out


def _tail(text: str, n: int = 12) -> str:
    return "\n".join([l for l in (text or "").splitlines() if l.strip()][-n:])


def _load_manifest(ws: dict, week_end: str) -> dict | None:
    try:
        with open(os.path.join(ws["root"], "raw", week_end, "manifest.json"), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _manifest_text(man: dict) -> str:
    lines = [f"周窗口 {man['window'][0]} ~ {man['window'][1]}；已识别 {len(man.get('files', {}))} 类报表，SP-API 缓存 {len(man.get('spapi_cache', []))} 份"]
    if man.get("missing_required"):
        lines.append(f"🔴 缺必需文件：{'、'.join(man['missing_required'])}")
    if man.get("missing_optional"):
        lines.append(f"🟡 缺可选文件：{'、'.join(man['missing_optional'])}")
    for c in man.get("checks", []):
        lines.append(("✅ " if c["ok"] else "⚠️ ") + f"{c['item']}：{c['detail']}")
    if man.get("unknown"):
        lines.append("未使用：" + "；".join(f"{u['file'][:30]}({u['note'][:24]})" for u in man["unknown"][:5]))
    return "\n".join(lines)


def _report_path(ws: dict, week_end: str) -> str:
    return os.path.join(ws["out"], f"周报_{week_end}.md")


def _pack_path(ws: dict, week_end: str) -> str | None:
    ps = sorted(glob.glob(os.path.join(ws["out"], f"ai_pack_*w_{week_end}.md")), key=os.path.getmtime)
    return ps[-1] if ps else None


# ── 飞书推送(与 库存处理/送仓时间 插件同一套已验证的做法) ──────────

def _target_chat() -> str:
    return _read_state().get("bind_chat_id") or (getattr(config, "CHAT_ID", "") if config else "")


def _push_text(chat_id: str, text: str):
    import feishu_gateway as gw
    gw.send_to_chat(chat_id, text)


def _send_file(path: str, message_id: str = "", chat_id: str = ""):
    """上传文件；有 chat_id 用主动发送，否则回复 message_id。失败抛异常"""
    import requests as req
    import feishu_gateway as gw
    token = req.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                     json={"app_id": gw.APP_ID, "app_secret": gw.APP_SECRET}, timeout=15).json().get("tenant_access_token")
    if not token:
        raise RuntimeError("获取飞书 token 失败")
    headers = {"Authorization": f"Bearer {token}"}
    name = os.path.basename(path)
    with open(path, "rb") as f:
        up = req.post("https://open.feishu.cn/open-apis/im/v1/files", headers=headers,
                      data={"file_type": "stream", "file_name": name}, files={"file": (name, f)}, timeout=120).json()
    if up.get("code") != 0:
        raise RuntimeError(f"文件上传失败: {up.get('msg')}")
    content = json.dumps({"file_key": up["data"]["file_key"]})
    if chat_id:
        url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
        body = {"receive_id": chat_id, "msg_type": "file", "content": content}
    else:
        url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply"
        body = {"msg_type": "file", "content": content}
    res = req.post(url, headers={**headers, "Content-Type": "application/json; charset=utf-8"}, json=body, timeout=30).json()
    if res.get("code") != 0:
        raise RuntimeError(f"文件已上传但发送失败: {res.get('msg')}")


# ════════════════════════════════════════════════════════════════
#  主入口
# ════════════════════════════════════════════════════════════════

def handle(message_id: str, text: str, reply_fn, user_id: str | None = None) -> bool:
    text = (text or "").strip()
    if not text.startswith("/周报"):
        return False
    sub, _, arg = text[len("/周报"):].strip().partition(" ")
    sub, arg = sub.strip(), arg.strip()

    if sub in ("", "状态"):
        reply_fn(message_id, _status_text(arg))
    elif sub == "绑定":
        _do_bind(message_id, reply_fn)
    elif sub == "定时":
        reply_fn(message_id, _sched_cmd(arg))
    elif sub == "测试":
        _start(message_id, reply_fn, "测试", arg, _latest_test_week(), exclusive=True)
    elif sub in ("窗口", "导入", "检查", "生成", "AI", "ai", "发送"):
        _start(message_id, reply_fn, sub.upper() if sub.lower() == "ai" else sub, arg, _default_week_end(),
               exclusive=sub in ("导入", "生成", "AI", "ai"))
    else:
        reply_fn(message_id, "用法：\n" + "\n".join(HELP_DETAIL))
    return True


def _start(message_id, reply_fn, name, arg, default_week, exclusive=True):
    week_end, err = _parse_week(arg, default_week)
    if err:
        tip = "；测试文件请放进 weekly_report/test/inputs/<周结束日>/" if name == "测试" and not default_week else ""
        reply_fn(message_id, f"❌ {err}{tip}")
        return
    if exclusive:
        if not _run_lock.acquire(blocking=False):
            reply_fn(message_id, f"⏳ 已有周报任务在运行：{_running['task']}(开始于 {_running['since']})，请稍后再试")
            return
        _running.update(task=f"{name} {week_end}", since=datetime.now().strftime("%H:%M:%S"))
    fn = {"窗口": _do_window, "导入": _do_import, "检查": _do_check, "生成": _do_formal, "AI": _do_ai_only,
          "发送": _do_resend, "测试": _do_test}[name]

    def _worker():
        try:
            fn(message_id, reply_fn, week_end)
        except Exception as e:
            logging.exception(f"[周报] {name} 异常")
            reply_fn(message_id, f"❌ 周报{name}异常：{type(e).__name__}: {e}")
        finally:
            if exclusive:
                _running.update(task=None, since=None)
                _run_lock.release()

    threading.Thread(target=_worker, daemon=True, name=f"周报-{name}").start()


# ════════════════════════════════════════════════════════════════
#  流程步骤
# ════════════════════════════════════════════════════════════════

def _import(ws: dict, src: str, week_end: str, copy: bool) -> tuple[dict | None, str]:
    args = [RECOGNIZER, "import", "--src", src, "--week-end", week_end, "--root", ws["root"], "--json"]
    if copy:
        args.append("--copy")
    code, out = _run(args, 600, "recognize.log")
    return _load_manifest(ws, week_end), out


def _build(ws: dict, week_end: str) -> tuple[bool, str]:
    """合并数据(ingest) + 生成AI数据包(pack)。返回 (成功, 错误信息)"""
    cfg = _ws_config(ws)
    args = [SCRIPT, "--config", cfg, "ingest", "--inputs", os.path.join(ws["root"], "raw", week_end), "--week-end", week_end]
    if ws["cache_only"]:
        args.append("--spapi-cache-only")
    code, out = _run(args, INGEST_TIMEOUT, f"ingest_{week_end}.log")
    if code != 0:
        return False, f"合并数据失败(exit {code})：\n{_tail(out, 15)}"
    code, out = _run([SCRIPT, "--config", cfg, "pack", "--weeks", str(PACK_WEEKS), "--end", week_end], 600, f"pack_{week_end}.log")
    if code != 0:
        return False, f"数据包生成失败(exit {code})：\n{_tail(out, 15)}"
    return True, ""


def _section(md: str, letter: str, next_letter: str) -> str:
    m = re.search(rf"(?ms)^\s*(?:#+\s*)?\**{letter}[.．、]\s*.*?(?=^\s*(?:#+\s*)?\**{next_letter}[.．、]|\Z)", md)
    return m.group(0).strip() if m else ""


def _ai_report(ws: dict, week_end: str) -> tuple[str | None, str]:
    """数据包 → ai_runner → 周报_<周>.md。返回 (文件路径, 提示/错误)"""
    if not _HAS_AI:
        return None, "未找到 ai_runner 模块，无法生成AI周报"
    pack = _pack_path(ws, week_end)
    if not pack:
        return None, "没有找到该周的AI数据包，请先生成数据"
    with open(pack, "r", encoding="utf-8") as f:
        prompt = f.read()
    prev_we = (datetime.strptime(week_end, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
    prev = _report_path(ws, prev_we)
    if os.path.exists(prev):                      # 上周AI周报的执行清单，供 E 节评估执行效果
        with open(prev, "r", encoding="utf-8") as f:
            a_sec = _section(f.read(), "A", "B")
        if a_sec:
            prompt += f"\n\n---\n# 上周({prev_we})AI周报的执行清单(供 E 节评估；如无法确认是否执行，写'未知')\n{a_sec}\n"
    prompt += ("\n\n---\n# 输出要求\n用 Markdown 输出，按 A~E 五节，每节用二级标题(## A. …)；A 节表格最多10行；"
               "全文控制在约4000字以内，不要复述数据包原文。第一行写标题：# 亚马逊周报 " + week_end)
    try:
        text = ai_runner.run_ai(prompt, timeout=AI_TIMEOUT)
    except Exception as e:
        return None, f"AI 调用失败：{e}"
    text = (text or "").strip()
    if not text or text.startswith("❌"):
        return None, f"AI 调用失败：{text[:300] or '空回复'}"
    note = ""
    if not re.search(r"(?m)^\s*(?:#+\s*)?\**E[.．、]", text):
        note = "⚠️ 周报没有 E 节，可能被输出长度截断"
    path = _report_path(ws, week_end)
    head = (f"<!-- 生成：{datetime.now():%Y-%m-%d %H:%M}；数据包：{os.path.basename(pack)}；"
            f"工作区：{'测试' if ws is WS['测试'] else '正式'} -->\n")
    with open(path, "w", encoding="utf-8") as f:
        f.write(head + text + "\n")
    return path, note


def _overview(ws: dict, week_end: str, report: str, note: str, secs: int) -> str:
    """结束概述：店铺汇总 + 断货风险数 + 本周执行清单前3条"""
    import pandas as pd
    lines = [f"📊 亚马逊周报 {week_end}（周窗口 {(datetime.strptime(week_end, '%Y-%m-%d') - timedelta(days=6)):%m-%d}~{week_end[5:]}）"
             + ("【测试数据】" if ws is WS["测试"] else "")]
    try:
        P = pd.read_csv(os.path.join(ws["out"], f"weekly_parent_{week_end}.csv"), encoding="utf-8-sig")
        g = P.groupby("店铺")[["销量7", "销售额7", "广告花费", "广告销售"]].sum()
        for s, r in g.iterrows():
            if not (r["销量7"] or r["广告花费"]):
                continue
            acos = f"{r['广告花费'] / r['广告销售']:.1%}" if r["广告销售"] else "-"
            tacos = f"{r['广告花费'] / r['销售额7']:.1%}" if r["销售额7"] else "-"
            lines.append(f"· {s}：销量 {r['销量7']:.0f}，销售额 ${r['销售额7']:,.0f}，广告 ${r['广告花费']:,.0f}，ACoS {acos}，TACoS {tacos}")
        if "断货天数_保守" in P.columns:
            risk = P[P["断货天数_保守"].fillna(0) > 0].sort_values("首次断货_天后")
            if len(risk):
                lines.append("· 断货风险：" + "、".join(f"{r['款']}(第{int(r['首次断货_天后'])}天)" for _, r in risk.head(5).iterrows()))
    except Exception as e:
        lines.append(f"· 汇总数据读取失败：{e}")
    try:                                          # 数据质量：错误/警告数，SP-API 缺失要点名(否则流量/退货/库龄结论不可用)
        Q = pd.read_excel(os.path.join(ws["out"], f"weekly_{week_end}.xlsx"), sheet_name="数据质量")
        n_e, n_w = int((Q["级别"] == "ERROR").sum()), int((Q["级别"] == "WARN").sum())
        sp_bad = Q[Q["级别"].isin(["ERROR", "WARN"]) & Q["检查项"].astype(str).str.startswith("SP-API")]["检查项"].tolist()
        if n_e or n_w:
            lines.append(f"· 数据质量：{n_e} 个错误、{n_w} 个警告(详见周报 C 节)" + (f"；SP-API 未取全：{'、'.join(sp_bad[:4])}" if sp_bad else ""))
    except Exception:
        pass
    try:
        with open(report, "r", encoding="utf-8") as f:
            a_sec = _section(f.read(), "A", "B")
        rows = [l for l in a_sec.splitlines() if l.strip().startswith("|") and not re.match(r"^\s*\|[\s:|-]+\|\s*$", l)][1:]
        if rows:
            lines.append("· 本周执行清单(前3条)：")
            for l in rows[:3]:
                cells = [c.strip() for c in l.strip().strip("|").split("|")]
                lines.append("  " + " | ".join(c for c in cells[1:3] if c)[:120])
    except Exception:
        pass
    if note:
        lines.append(note)
    lines.append(f"完整周报见附件 {os.path.basename(report)}；用时 {secs // 60} 分 {secs % 60} 秒")
    return "\n".join(lines)


def _finish(ws, week_end, reply_fn, message_id, chat_id, t0) -> None:
    """AI 周报 → 发文件 → 概述"""
    reply_fn(message_id, "🧠 数据已生成，AI 正在撰写周报…")
    path, note = _ai_report(ws, week_end)
    if not path:
        reply_fn(message_id, f"❌ {note}\n(周宽表与数据包已保存在 {ws['out']})")
        return
    try:
        _send_file(path, message_id=message_id, chat_id=chat_id)
    except Exception as e:
        note = (note + "\n" if note else "") + f"⚠️ 周报文件发送失败：{e}(文件在 {path})"
    _write_state({f"last_{'test' if ws is WS['测试'] else 'formal'}": {
        "week_end": week_end, "at": datetime.now().strftime("%Y-%m-%d %H:%M"), "report": os.path.basename(path)}})
    reply_fn(message_id, _overview(ws, week_end, path, note, int(time.time() - t0)))


def _do_test(message_id, reply_fn, week_end):
    ws = WS["测试"]
    src = os.path.join(ws["inputs"], week_end)
    if not os.path.isdir(src) or not os.listdir(src):
        reply_fn(message_id, f"📂 测试文件夹没有这一周的数据：{src}\n请把领星导出和 SP-API .bin 放进去(文件名随意)")
        return
    t0 = time.time()
    reply_fn(message_id, f"⏳ 周报测试 {week_end}：识别测试文件 → 合并数据(只用SP-API缓存) → AI 周报…")
    shutil.rmtree(os.path.join(ws["root"], "raw", week_end), ignore_errors=True)      # 每次从 inputs 重新复制
    man, out = _import(ws, src, week_end, copy=True)
    if not man:
        reply_fn(message_id, f"❌ 识别失败：\n{_tail(out)}")
        return
    if man.get("missing_required"):
        reply_fn(message_id, "❌ 测试文件不全：\n" + _manifest_text(man))
        return
    ok, err = _build(ws, week_end)
    if not ok:
        reply_fn(message_id, f"❌ {err}")
        return
    _finish(ws, week_end, reply_fn, message_id, "", t0)


def _do_formal(message_id, reply_fn, week_end, chat_id=""):
    ws = WS["正式"]
    man = _load_manifest(ws, week_end)
    if not man:
        reply_fn(message_id, f"📂 {week_end} 还没有导入文件，请先把领星导出放进 inbox 并发 /周报 导入 {week_end}")
        return
    if man.get("missing_required"):
        reply_fn(message_id, "❌ 文件不全，无法生成：\n" + _manifest_text(man))
        return
    t0 = time.time()
    reply_fn(message_id, f"⏳ 正在生成 {week_end} 周报(含SP-API拉取，最长约40分钟)…")
    ok, err = _build(ws, week_end)
    if not ok:
        reply_fn(message_id, f"❌ {err}")
        return
    _finish(ws, week_end, reply_fn, message_id, chat_id, t0)


def _do_ai_only(message_id, reply_fn, week_end):
    ws = WS["正式"]
    if not _pack_path(ws, week_end):
        reply_fn(message_id, f"📭 {week_end} 还没有生成数据，请先 /周报 生成 {week_end}")
        return
    _finish(ws, week_end, reply_fn, message_id, "", time.time())


def _do_resend(message_id, reply_fn, week_end):
    for ws in (WS["正式"], WS["测试"]):
        p = _report_path(ws, week_end)
        if os.path.exists(p):
            try:
                _send_file(p, message_id=message_id)
            except Exception as e:
                reply_fn(message_id, f"❌ 发送失败：{e}(文件在 {p})")
            return
    reply_fn(message_id, f"📭 {week_end} 还没有AI周报，请先 /周报 生成 {week_end}")


def _do_window(message_id, reply_fn, week_end):
    code, out = _run([SCRIPT, "window", "--week-end", week_end], 60, "window.log")
    if code != 0:
        reply_fn(message_id, f"❌ 获取窗口失败：\n{_tail(out)}")
        return
    lines = [l for l in out.strip().splitlines() if "放进文件夹" not in l]
    lines.append(f"  · 文件名随意，放进 {WS['正式']['inbox']} 后发 /周报 导入 {week_end}")
    reply_fn(message_id, "\n".join(lines))


def _do_import(message_id, reply_fn, week_end):
    ws = WS["正式"]
    files = [f for f in os.listdir(ws["inbox"]) if os.path.isfile(os.path.join(ws["inbox"], f))]
    if not files:
        reply_fn(message_id, f"📭 收件箱是空的：{ws['inbox']}\n请把领星导出(和SP-API .bin)放进去再发 /周报 导入")
        return
    man, out = _import(ws, ws["inbox"], week_end, copy=False)
    if not man:
        reply_fn(message_id, f"❌ 识别失败：\n{_tail(out)}")
        return
    moved = man.get("last_import", {}).get("moved", [])
    msg = [f"📥 已归档 {len(moved)} 个文件到 {week_end}：" + "、".join(m["type"] for m in moved), _manifest_text(man),
           "✅ 必需文件齐全，可以发 /周报 生成" if man["ok"] else "⚠️ 补齐标红的文件后再导入"]
    reply_fn(message_id, "\n".join(msg))


def _do_check(message_id, reply_fn, week_end):
    ws = WS["正式"]
    _run([RECOGNIZER, "check", "--week-end", week_end, "--root", ws["root"], "--json"], 300, "recognize.log")
    man = _load_manifest(ws, week_end)
    reply_fn(message_id, f"🔍 {week_end}\n" + _manifest_text(man) if man else f"📂 {week_end} 还没有导入任何文件")


def _status_text(arg: str) -> str:
    week_end, err = _parse_week(arg, _default_week_end())
    if err:
        return f"❌ {err}"
    st = _read_state()
    ws = WS["正式"]
    man = _load_manifest(ws, week_end)
    lines = [f"📊 周报状态（正式周：{week_end}）"]
    if _running["task"]:
        lines.append(f"⏳ 运行中：{_running['task']}(开始于 {_running['since']})")
    lines.append(_manifest_text(man) if man else f"该周还没有导入文件(inbox 里有 {len(os.listdir(ws['inbox']))} 个文件)")
    lines.append("AI周报：" + ("已生成 " + os.path.basename(_report_path(ws, week_end)) if os.path.exists(_report_path(ws, week_end)) else "未生成"))
    t = st.get("last_test")
    lines.append(f"最近测试：{t['week_end']}({t['at']})" if t else f"最近测试：无(测试周：{_latest_test_week() or '测试文件夹为空'})")
    lines.append(_sched_status())
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════
#  绑定群 & 定时(每周五 13:00 北京时间，跑上一周；默认关闭)
# ════════════════════════════════════════════════════════════════

def _do_bind(message_id, reply_fn):
    try:
        import feishu_gateway as gw
        chat_id = gw.get_chat_id_by_msg(message_id)
    except Exception as e:
        reply_fn(message_id, f"❌ 绑定失败：{e}")
        return
    if not chat_id:
        reply_fn(message_id, "❌ 获取群 ID 失败，请重试")
        return
    _write_state({"bind_chat_id": chat_id})
    reply_fn(message_id, f"✅ 定时周报将推送到本群（chat_id: {chat_id}）")


def _next_run() -> datetime:
    now = _bj_now()
    t = now.replace(hour=SCHED_HOUR, minute=SCHED_MINUTE, second=0, microsecond=0) + timedelta(days=(SCHED_WEEKDAY - now.weekday()) % 7)
    return t if t > now else t + timedelta(days=7)


def _sched_status() -> str:
    st = _read_state()
    on = st.get("sched_enabled", False)
    last = st.get("sched_last", {})
    return (f"定时：{'🟢 已开启' if on else '⚪ 未开启'}，每周五 {SCHED_HOUR:02d}:{SCHED_MINUTE:02d}(北京时间)跑上一周"
            + (f"，下次 {_next_run():%Y-%m-%d %H:%M}" if on else "")
            + (f"；上次 {last.get('at')} {last.get('result')}" if last else "")
            + f"；推送群 {_target_chat() or '未绑定'}")


def _sched_cmd(arg: str) -> str:
    if arg == "开启":
        if not _target_chat():
            return "❌ 还没有推送群：请先在目标群发 /周报 绑定"
        _write_state({"sched_enabled": True})
        return "✅ " + _sched_status()
    if arg == "关闭":
        _write_state({"sched_enabled": False})
        return "⏸️ " + _sched_status()
    return _sched_status()


def _scheduled_job():
    chat = _target_chat()
    week_end = _default_week_end()
    mid = "scheduler"
    reply = lambda _m, text: _push_text(chat, text)
    ws = WS["正式"]
    if not _run_lock.acquire(blocking=False):
        reply(mid, f"⚠️ 定时周报 {week_end} 跳过：已有任务在运行({_running['task']})")
        return "跳过(有任务在运行)"
    _running.update(task=f"定时 {week_end}", since=datetime.now().strftime("%H:%M:%S"))
    try:
        if os.listdir(ws["inbox"]):
            _import(ws, ws["inbox"], week_end, copy=False)
        man = _load_manifest(ws, week_end)
        if not man or man.get("missing_required"):
            reply(mid, f"⚠️ 定时周报 {week_end} 未运行：领星文件不全\n" + (_manifest_text(man) if man else "该周没有任何文件"))
            return "文件不全"
        _do_formal(mid, reply, week_end, chat_id=chat)
        return "已执行"
    finally:
        _running.update(task=None, since=None)
        _run_lock.release()


def _scheduler_loop():
    while True:
        if _read_state().get("sched_instance") != _INSTANCE_ID:
            return                                     # 插件热重载后旧线程退出
        wait = (_next_run() - _bj_now()).total_seconds()
        slept = 0
        while slept < wait:
            time.sleep(min(300, wait - slept))
            slept += 300
            if _read_state().get("sched_instance") != _INSTANCE_ID:
                return
        if _read_state().get("sched_enabled") and _target_chat():
            try:
                result = _scheduled_job()
            except Exception as e:
                logging.exception("[周报] 定时任务异常")
                result = f"异常：{e}"
                try:
                    _push_text(_target_chat(), f"❌ 定时周报异常：{e}")
                except Exception:
                    pass
            _write_state({"sched_last": {"at": _bj_now().strftime("%Y-%m-%d %H:%M"), "result": result}})
        time.sleep(90)


def _bootstrap():
    global _sched_thread
    _write_state({"sched_instance": _INSTANCE_ID})
    _sched_thread = threading.Thread(target=_scheduler_loop, daemon=True, name="WeeklyReportScheduler")
    _sched_thread.start()


if os.environ.get("WEEKLY_REPORT_NO_SCHEDULER") != "1":
    _bootstrap()
