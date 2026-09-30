# plugins/经营经营周报_plugin.py
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
#  定时：每周五 13:00(北京时间)跑上一周(周日~周六)，默认关闭，领星自动化接入后 /经营周报 定时 开启。
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
TRIGGERS    = ["/经营周报"]
PRIORITY    = 30
DESCRIPTION = "亚马逊周报：领星周数据+SP-API合并分析，AI生成周报文件发群并给出概述；每周五13:00可定时"

HELP_CATEGORY = "数据报表"
HELP_EMOJI    = "📊"
HELP_GROUP    = "亚马逊周报"

HELP_DETAIL = [
    "/经营周报              查看进度：本周文件是否齐全、周报是否已生成、定时状态",
    "/经营周报 测试 [日期]    用测试文件夹的固定数据跑完整流程(只用SP-API缓存)，发周报+概述",
    "/经营周报 窗口          领星各报表该下载哪几天(周日~周六)",
    "/经营周报 导入 [日期]    识别 inbox 里的领星文件，改名放进该周文件夹",
    "/经营周报 检查 [日期]    检查该周文件是否齐全、订单是否覆盖整周",
    "/经营周报 生成 [日期]    正式生成：拉SP-API→合并→AI周报→发文件+概述",
    "/经营周报 AI [日期]      只重跑AI周报(数据已生成时)，发文件+概述",
    "/经营周报 发送 [日期]    重发该周的AI周报(PDF)",
    "/经营周报 绑定          定时周报推送到本群",
    "/经营周报 定时 开启|关闭  每周五13:00(北京时间)自动跑上一周",
]

HELP_TIPS = [
    "💡 日期=周结束日(周六)；正式指令不填则取最近一个数据已发布的周六，测试指令不填则取测试文件夹里最新的一周",
    "💡 测试文件放 weekly_report/test/inputs/<周结束日>/，文件名随意，按表头识别",
    "💡 群里只发AI最终周报(PDF)；Markdown原稿、周宽表、数据包保存在 out/ 备查",
    "💡 PDF 用本机 Chrome/Edge 生成(需 pip install markdown)；转换失败会改发 Markdown 并提示原因",
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


def _log(msg: str, level: str = "info"):
    """控制台(店铺后端 cmd 窗口)日志，统一前缀 [经营周报]"""
    getattr(logging, level)(f"[经营周报] {msg}")


_STEP_NAMES = {"recognize.py": "识别", "window": "窗口", "ingest": "合并", "pack": "数据包"}


def _run(args: list, timeout: int, log_name: str) -> tuple[int, str]:
    """子进程运行：输出逐行实时打印到控制台，同时追加到 logs/<log_name>；超时强制结束。返回 (退出码, 全部输出)"""
    step = next((v for k, v in _STEP_NAMES.items() if k in args[0] or k in args), os.path.basename(args[0]))
    t0 = time.time()
    _log(f"[{step}] 开始：{os.path.basename(args[0])} {' '.join(a for a in args[1:] if not os.path.isabs(a))}")
    lines, code = [], -1
    try:
        proc = subprocess.Popen([sys.executable, "-u"] + args, cwd=ENGINE_DIR, env=_env(), stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", bufsize=1)
        killer = threading.Timer(timeout, proc.kill)
        killer.start()
        try:
            for line in proc.stdout:
                line = line.rstrip()
                lines.append(line)
                show = re.sub(r"\s{2,}", "  ", line.strip())          # 表格对齐用的长空格压缩掉
                if show and not show.startswith("{"):                # 识别器给插件用的 JSON 不打印
                    _log(f"[{step}] {show[:300]}")
            code = proc.wait()
        finally:
            killer.cancel()
        if time.time() - t0 >= timeout:
            code = -9
            lines.append(f"超时(>{timeout}秒)，已强制结束")
    except Exception as e:
        lines.append(f"启动失败：{e}")
    out = "\n".join(lines)
    _log(f"[{step}] 结束：exit {code}，用时 {time.time() - t0:.0f} 秒", "info" if code == 0 else "error")
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
    if not text.startswith("/经营周报"):
        return False
    sub, _, arg = text[len("/经营周报"):].strip().partition(" ")
    sub, arg = sub.strip(), arg.strip()

    if sub in ("", "状态"):
        reply_fn(message_id, _status_text(arg))
    elif sub == "绑定":
        _do_bind(message_id, reply_fn)
    elif sub == "定时":
        reply_fn(message_id, _sched_cmd(arg))
    elif sub == "测试":
        _start(message_id, reply_fn, "测试", arg, _latest_test_week(), exclusive=True, user_id=user_id)
    elif sub in ("窗口", "导入", "检查", "生成", "AI", "ai", "发送"):
        _start(message_id, reply_fn, sub.upper() if sub.lower() == "ai" else sub, arg, _default_week_end(),
               exclusive=sub in ("导入", "生成", "AI", "ai"), user_id=user_id)
    else:
        reply_fn(message_id, "用法：\n" + "\n".join(HELP_DETAIL))
    return True


def _start(message_id, reply_fn, name, arg, default_week, exclusive=True, user_id=None):
    week_end, err = _parse_week(arg, default_week)
    if err:
        tip = "；测试文件请放进 weekly_report/test/inputs/<周结束日>/" if name == "测试" and not default_week else ""
        reply_fn(message_id, f"❌ {err}{tip}")
        return
    _log(f"收到指令：{name} {week_end}(用户 {user_id or '定时'}，消息 {message_id})")
    if exclusive:
        if not _run_lock.acquire(blocking=False):
            _log(f"已有任务在运行({_running['task']})，拒绝本次 {name}", "warning")
            reply_fn(message_id, f"⏳ 已有周报任务在运行：{_running['task']}(开始于 {_running['since']})，请稍后再试")
            return
        _running.update(task=f"{name} {week_end}", since=datetime.now().strftime("%H:%M:%S"))
    fn = {"窗口": _do_window, "导入": _do_import, "检查": _do_check, "生成": _do_formal, "AI": _do_ai_only,
          "发送": _do_resend, "测试": _do_test}[name]

    def _worker():
        try:
            fn(message_id, reply_fn, week_end)
        except Exception as e:
            logging.exception(f"[经营周报] {name} 异常")
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
    man = _load_manifest(ws, week_end)
    if man:
        _log(f"[识别] 识别到 {len(man.get('files', {}))} 类报表：{'、'.join(v['name'] for v in man.get('files', {}).values())}；"
             f"SP-API缓存 {len(man.get('spapi_cache', []))} 份；"
             + (f"缺必需：{'、'.join(man['missing_required'])}" if man.get("missing_required") else "必需文件齐全"))
    return man, out


def _build(ws: dict, week_end: str) -> tuple[bool, str]:
    """合并数据(ingest) + 生成AI数据包(pack)。返回 (成功, 错误信息)"""
    cfg = _ws_config(ws)
    _log(f"[合并] 工作区：{'测试(只用SP-API缓存)' if ws['cache_only'] else '正式(会请求SP-API，最长约40分钟)'}；输出目录 {ws['out']}")
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


def _sec(md: str, start: str, end: str) -> str:
    """取 Markdown 中从标题 start(正则) 到标题 end(正则) 之间的内容"""
    m = re.search(rf"(?ms)^\s*#*\s*\**(?:{start}).*?(?=^\s*#*\s*\**(?:{end})|\Z)", md)
    return m.group(0).strip() if m else ""


def _prev_actions(ws: dict, week_end: str) -> tuple[str, str]:
    """上周AI周报的执行建议清单(新格式 八、；兼容旧格式 A.)"""
    prev_we = (datetime.strptime(week_end, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
    p = _report_path(ws, prev_we)
    if not os.path.exists(p):
        return prev_we, ""
    with open(p, "r", encoding="utf-8") as f:
        md = f.read()
    return prev_we, _sec(md, "八、", "九、") or _sec(md, "A[.．、]", "B[.．、]")


def _call_ai(prompt: str, label: str = "AI") -> tuple[str | None, str]:
    """调用 ai_runner；等待期间每30秒打印一次心跳，结束打印用时与字数"""
    t0, done = time.time(), threading.Event()
    _log(f"[{label}] 提交模型：输入 {len(prompt):,} 字符，等待回复…")

    def _beat():
        while not done.wait(30):
            _log(f"[{label}] 仍在等待模型回复，已等 {time.time() - t0:.0f} 秒")
    threading.Thread(target=_beat, daemon=True, name="周报-AI心跳").start()
    try:
        text = (ai_runner.run_ai(prompt, timeout=AI_TIMEOUT) or "").strip()
    except Exception as e:
        _log(f"[{label}] 调用失败：{e}", "error")
        return None, f"AI 调用失败：{e}"
    finally:
        done.set()
    if not text or text.startswith("❌"):
        _log(f"[{label}] 调用失败：{text[:300] or '空回复'}", "error")
        return None, f"AI 调用失败：{text[:300] or '空回复'}"
    _log(f"[{label}] 完成：输出 {len(text):,} 字符，用时 {time.time() - t0:.0f} 秒")
    return text, ""


def _engine_mod(name: str):
    """按文件加载 weekly_report/ 下的模块(render_pdf、actions)，不污染店铺后端的 sys.path"""
    import importlib.util
    spec = importlib.util.spec_from_file_location(f"wr_{name}", os.path.join(ENGINE_DIR, f"{name}.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _actions_path(ws: dict, week_end: str) -> str:
    return os.path.join(ws["out"], f"actions_{week_end}.json")


def _check_actions(ws: dict, week_end: str, part2: str) -> tuple[str, str]:
    """校验 AI 在 八 节选的候选动作：ID 存在、未被规则阻止、数值在允许范围、无冲突。
    用校验结果生成 八 节表格替换 AI 的 JSON，结果存 actions_<周>.json。返回 (新的第二部分, 提示)"""
    cpath = os.path.join(ws["out"], f"candidates_{week_end}.json")
    if not os.path.exists(cpath):
        _log("[校验] 没有候选动作文件，跳过校验", "warning")
        return part2, "⚠️ 没有候选动作文件，执行清单未经程序校验"
    ACT = _engine_mod("actions")
    with open(cpath, "r", encoding="utf-8") as f:
        cj = json.load(f)
    sel, span = ACT.extract_json(part2)
    rec = {"week_end": week_end, "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M"), "candidates": os.path.basename(cpath)}
    if sel is None:
        _log("[校验] AI 没有按格式输出 JSON，执行清单未校验", "warning")
        rec.update(status="未校验", reason="AI 没有输出可解析的 JSON", passed=[], blocked=[], manual=[])
        note = "⚠️ AI 没有按格式输出执行清单 JSON，八 节未经程序校验，不能用于自动执行"
        new = part2
    else:
        res = ACT.validate(sel, cj["candidates"], {"action_rules": cj.get("rules") or {}})
        rec.update(status="已校验", **res)
        _log(f"[校验] 执行清单：通过 {len(res['passed'])} 条，拦截 {len(res['blocked'])} 条，人工事项 {len(res['manual'])} 条")
        for x in res["blocked"]:
            _log(f"[校验] 拦截 {x.get('id') or '-'}：{x['原因']}")
        table = ACT.render_md(res)
        m = re.search(r"(?ms)^\s*#{1,3}\s*\**八、.*?(?=^\s*#{1,3}\s*\**九、|\Z)", part2)
        if m and m.start() <= span[0] < m.end():
            new = part2[:m.start()] + table + "\n" + part2[m.end():]
        else:
            new = part2[:span[0]] + table + part2[span[1]:]
        note = f"执行清单通过校验 {len(res['passed'])} 条" + (f"，拦截 {len(res['blocked'])} 条(原因见周报 八 节)" if res["blocked"] else "")
    with open(_actions_path(ws, week_end), "w", encoding="utf-8") as f:
        json.dump(ACT.to_jsonable(rec), f, ensure_ascii=False, indent=1)
    _log(f"[校验] 已保存 {_actions_path(ws, week_end)}")
    return new, note


def _ai_mode() -> str:
    """single=一次调用写完整周报；split=分两次(①数据报告 ②执行建议)。
    只有 anthropic 中转通道(ai_runner 里 max_tokens=4096)一次写不下完整周报，需要分两次；
    Claude CLI(Pro 订阅)/Gemini 输出上限够，一次写完，数据包只提交一次。可用环境变量 WEEKLY_REPORT_AI_MODE=single/split 强制。"""
    forced = os.environ.get("WEEKLY_REPORT_AI_MODE", "").strip().lower()
    if forced in ("single", "split"):
        return forced
    try:
        engine = ai_runner._get_engine()
    except Exception:
        engine = ""
    return "split" if engine == "anthropic" else "single"


_TASK_P1 = ("**第一部分 数据报告**：第一行 `# 亚马逊周报 {we}`，然后按 一~七 写。严格遵守第一原则(没有的数据写'无数据')。"
            "约4500字以内，表格优先，不复述数据包原文。")
_TASK_P2 = ("**第二部分 执行建议**：以 `## 第二部分 执行建议` 开头，按 八~十二 写。八 节只能从数据包'5. 候选动作'里选ID，"
            "只输出一个 ```json 代码块(格式见输出格式)，不要写表格；九~十二 的数字必须与数据包和第一部分一致，不要重复第一部分的内容。约3000字以内。")


def _ai_report(ws: dict, week_end: str) -> tuple[str | None, str]:
    """数据包 → ai_runner → 周报_<周>.md。默认一次调用写完两部分；anthropic 通道(输出上限4096 tokens)分两次。
    一次调用时若第二部分缺失(被截断)，只补调一次第二部分。返回 (文件路径, 提示/错误)"""
    if not _HAS_AI:
        return None, "未找到 ai_runner 模块，无法生成AI周报"
    pack = _pack_path(ws, week_end)
    if not pack:
        return None, "没有找到该周的AI数据包，请先生成数据"
    with open(pack, "r", encoding="utf-8") as f:
        data = f.read()
    prev_we, prev = _prev_actions(ws, week_end)
    _log(f"[AI] 上周({prev_we})执行建议：{'已附上，供第十二节复盘' if prev else '没有，第十二节写无'}")
    prev_block = (f"\n\n---\n# 上周({prev_we})AI周报的执行建议清单(供 十二 节逐条评估；无法确认是否执行的写'未知')\n{prev}\n"
                  if prev else "\n\n(没有上周的AI周报，十二 节写'无')\n")
    mode = _ai_mode()
    t1, t2 = _TASK_P1.format(we=week_end), _TASK_P2
    part1 = part2 = None
    if mode == "single":
        _log(f"[AI] 使用数据包 {os.path.basename(pack)}；一次调用写完两部分(数据包只提交一次)")
        text, err = _call_ai(data + prev_block + "\n---\n# 本次任务\n一次输出完整周报，依次写：\n1. " + t1 + "\n2. " + t2, "AI周报")
        if not text:
            return None, err
        m = re.search(r"(?m)^\s*#{1,3}\s*\**第二部分", text)
        if m and re.search(r"八、", text[m.start():]):
            part1, part2 = text[:m.start()].rstrip(), text[m.start():]
        else:
            _log("[AI] 回复里没有完整的第二部分(可能被截断)，补调一次只写第二部分", "warning")
            part1 = text[:m.start()].rstrip() if m else text
    else:
        _log(f"[AI] 使用数据包 {os.path.basename(pack)}；分两次调用(anthropic 通道单次输出上限4096 tokens，写不下完整周报)：①数据报告 ②执行建议")
        part1, err = _call_ai(data + "\n\n---\n# 本次任务\n只输出" + t1 + "不要输出第二部分。", "AI①数据报告")
        if not part1:
            return None, err
    if part2 is None:
        part2, err = _call_ai(data + "\n\n---\n# 已写好的第一部分(数据报告)\n" + part1 + prev_block
                              + "\n---\n# 本次任务\n只输出" + t2, "AI②执行建议")
        if not part2:
            return None, "第一部分已生成，但" + err
    notes = []
    try:
        part2, act_note = _check_actions(ws, week_end, part2)
        if act_note.startswith("⚠️"):
            notes.append(act_note[2:].strip())
    except Exception as e:
        _log(f"[校验] 失败：{e}", "error")
        notes.append(f"执行清单校验出错：{e}")
    try:                                          # 正文一致性检查(本地仓够用/状态混称/方向写反/无出处数字)，结果附在周报末尾
        import pandas as pd
        ACT = _engine_mod("actions")
        Pw = pd.read_csv(os.path.join(ws["out"], f"weekly_parent_{week_end}.csv"), encoding="utf-8-sig")
        lint = ACT.lint_report(part1 + "\n" + part2, Pw)
        if lint:
            part2 = part2.rstrip() + "\n" + ACT.lint_md(lint)
            _log(f"[校验] 正文一致性检查：发现 {len(lint)} 处疑似问题(已附在周报末尾)")
            for x in lint:
                _log(f"[校验]   {x[:160]}")
        _write_state({f"lint_{'test' if ws is WS['测试'] else 'formal'}_{week_end}": len(lint)})
    except Exception as e:
        _log(f"[校验] 正文一致性检查失败：{e}", "warning")
    if not re.search(r"七、", part1):
        notes.append("第一部分没有写到 七、，可能被输出长度截断")
    if not re.search(r"十一、", part2):
        notes.append("第二部分没有写到 十一、，可能被输出长度截断")
    path = _report_path(ws, week_end)
    head = (f"<!-- 生成：{datetime.now():%Y-%m-%d %H:%M}；数据包：{os.path.basename(pack)}；AI调用：{mode}；"
            f"工作区：{'测试' if ws is WS['测试'] else '正式'} -->\n")
    with open(path, "w", encoding="utf-8") as f:
        f.write(head + part1 + "\n\n" + part2 + "\n")
    _log(f"[AI] 周报原稿已保存：{path}" + (f"；注意：{'；'.join(notes)}" if notes else ""))
    return path, ("⚠️ " + "；".join(notes)) if notes else ""


def _overview(ws: dict, week_end: str, report: str, note: str, secs: int, sent: str = "") -> str:
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
        if "断货天数_含待交付" in P.columns:      # 有补货数据：按"已下单的货都算上仍断货"报，只看FBA+在途会夸大(如PO能接上的款)
            risk = P[P["断货天数_含待交付"].fillna(0) > 0].sort_values("首次断货_含待交付_天后")
            if len(risk):
                lines.append("· 断货风险(含工厂待交付仍断货)：" + "、".join(
                    f"{r['款']}(第{int(r['首次断货_含待交付_天后'])}天起{int(r['断货天数_含待交付'])}天"
                    + (f"，空运也补不上{int(r['空运可售前无法避免断货天数'])}天" if r.get("空运可售前无法避免断货天数", 0) > 0 else "") + ")"
                    for _, r in risk.head(5).iterrows()))
            if "尺码_缺货件数_含待交付" in P.columns:   # 亚马逊按子体判断缺货：尺码级才是真实缺货
                sz = P[P["尺码_缺货件数_含待交付"].fillna(0) >= 10].sort_values("尺码_缺货件数_含待交付", ascending=False)
                if len(sz):
                    lines.append("· 尺码缺货(60天内，含待交付仍缺)：" + "、".join(
                        f"{r['款']} 缺{r['尺码_缺货件数_含待交付']:.0f}件" + (f"(本地仓对应尺码不足{r['尺码_本地仓不足件数']:.0f})" if r.get("尺码_本地仓不足件数", 0) >= 5 else "")
                        for _, r in sz.head(5).iterrows()))
            if "冗余_FBA件数" in P.columns:              # 冗余/滞销：清货与停采
                ex = P[P["冗余_FBA件数"].fillna(0) >= 20].sort_values("冗余_FBA件数", ascending=False)
                if len(ex):
                    lines.append(f"· 冗余(FBA超90天销量)共 {ex['冗余_FBA件数'].sum():.0f} 件：" + "、".join(
                        f"{r['款']} {r['冗余_FBA件数']:.0f}" for _, r in ex.head(5).iterrows())
                        + (f"；滞销尺码(满45天、近30天零销量) FBA {P['滞销_FBA件数'].sum():.0f} 件、本地仓 {P['滞销_本地件数'].sum():.0f} 件" if "滞销_FBA件数" in P.columns else ""))
            only = P[(P["断货天数_保守"].fillna(0) > 0) & (P["断货天数_含待交付"].fillna(0) == 0)]
            if len(only):
                lines.append("· 待交付按时到才不断货：" + "、".join(only["款"].astype(str).head(5)) + "(要盯PO交期)")
        elif "断货天数_保守" in P.columns:
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
            lines.append(f"· 数据质量：{n_e} 个错误、{n_w} 个警告(详见周报 十一 节)" + (f"；SP-API 未取全：{'、'.join(sp_bad[:4])}" if sp_bad else ""))
    except Exception:
        pass
    try:
        with open(_actions_path(ws, week_end), "r", encoding="utf-8") as f:
            aj = json.load(f)
        if aj.get("status") == "已校验":
            ACT = _engine_mod("actions")
            ps, bl = aj.get("passed") or [], aj.get("blocked") or []
            lines.append(f"· 执行清单：通过校验 {len(ps)} 条" + (f"，拦截 {len(bl)} 条" if bl else "") + ("，前3条：" if ps else ""))
            for x in ps[:3]:
                lines.append(f"  {x['最终优先级']} {x['id']} {x['款']}：{ACT._action_txt(x)}"[:140])
            report = ""                           # 已用校验结果，不再解析 AI 原文表格
    except FileNotFoundError:
        pass
    except Exception as e:
        lines.append(f"· 执行清单读取失败：{e}")
    try:                                          # 没有校验结果(旧周报/AI 未按格式)时，退回解析 AI 原文表格
        act = ""
        if report:
            with open(report, "r", encoding="utf-8") as f:
                act = _sec(f.read(), "八、", "九、")
        rows = [l for l in act.splitlines() if l.strip().startswith("|") and not re.match(r"^\s*\|[\s:|-]+\|\s*$", l)][1:]
        if rows:
            lines.append(f"· 执行建议 {len(rows)} 条(未经程序校验)，前3条：")
            for l in rows[:3]:
                cells = [c.strip() for c in l.strip().strip("|").split("|")]
                lines.append("  " + " | ".join(c for c in cells[:3] if c)[:140])
    except Exception:
        pass
    n_lint = _read_state().get(f"lint_{'test' if ws is WS['测试'] else 'formal'}_{week_end}")
    if n_lint:
        lines.append(f"· ⚠️ 正文一致性检查：发现 {n_lint} 处疑似矛盾/无出处数字，已列在周报末尾“附：程序一致性检查”，引用前请核对")
    if note:
        lines.append(note)
    lines.append(f"完整周报见附件 {os.path.basename(sent or _report_path(ws, week_end))}；用时 {secs // 60} 分 {secs % 60} 秒")
    return "\n".join(lines)


def _to_pdf(md_path: str) -> str:
    """周报 .md → .html + .pdf(weekly_report/render_pdf.py，用本机 Chrome/Edge 打印)。失败抛异常"""
    return _engine_mod("render_pdf").md_file_to_pdf(md_path)


def _finish(ws, week_end, reply_fn, message_id, chat_id, t0) -> None:
    """AI 周报(.md 保存) → 转 PDF → 发 PDF(失败则发 .md) → 概述"""
    reply_fn(message_id, "🧠 数据已生成，AI 正在撰写周报…")
    path, note = _ai_report(ws, week_end)
    if not path:
        _log(f"❌ AI 周报失败：{note}", "error")
        reply_fn(message_id, f"❌ {note}\n(周宽表与数据包已保存在 {ws['out']})")
        return
    send = path
    t1 = time.time()
    _log("[PDF] 转换中(Markdown → HTML → 浏览器打印 PDF)…")
    try:
        send = _to_pdf(path)
        _log(f"[PDF] 完成：{send}（{os.path.getsize(send) // 1024} KB，{time.time() - t1:.0f} 秒）")
    except Exception as e:
        _log(f"[PDF] 转换失败，改发 Markdown：{e}", "warning")
        note = (note + "\n" if note else "") + f"⚠️ 转 PDF 失败，改发 Markdown：{str(e)[:150]}"
    try:
        _log(f"[发送] 上传并发送 {os.path.basename(send)} → {'群 ' + chat_id if chat_id else '回复消息 ' + message_id}")
        _send_file(send, message_id=message_id, chat_id=chat_id)
        _log("[发送] 成功")
    except Exception as e:
        _log(f"[发送] 失败：{e}", "error")
        note = (note + "\n" if note else "") + f"⚠️ 周报文件发送失败：{e}(文件在 {send})"
    _write_state({f"last_{'test' if ws is WS['测试'] else 'formal'}": {
        "week_end": week_end, "at": datetime.now().strftime("%Y-%m-%d %H:%M"), "report": os.path.basename(path)}})
    reply_fn(message_id, _overview(ws, week_end, path, note, int(time.time() - t0), sent=send))
    _log(f"✅ 完成：{week_end} 周报已发送并附概述，总用时 {time.time() - t0:.0f} 秒")


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
        reply_fn(message_id, f"📂 {week_end} 还没有导入文件，请先把领星导出放进 inbox 并发 /经营周报 导入 {week_end}")
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
        reply_fn(message_id, f"📭 {week_end} 还没有生成数据，请先 /经营周报 生成 {week_end}")
        return
    _finish(ws, week_end, reply_fn, message_id, "", time.time())


def _do_resend(message_id, reply_fn, week_end):
    for ws in (WS["正式"], WS["测试"]):
        p = _report_path(ws, week_end)
        if os.path.exists(p):
            pdf = os.path.splitext(p)[0] + ".pdf"
            if not os.path.exists(pdf) or os.path.getmtime(pdf) < os.path.getmtime(p):
                try:
                    pdf = _to_pdf(p)
                except Exception:
                    pdf = ""
            p = pdf if pdf and os.path.exists(pdf) else p
            try:
                _send_file(p, message_id=message_id)
            except Exception as e:
                reply_fn(message_id, f"❌ 发送失败：{e}(文件在 {p})")
            return
    reply_fn(message_id, f"📭 {week_end} 还没有AI周报，请先 /经营周报 生成 {week_end}")


def _do_window(message_id, reply_fn, week_end):
    code, out = _run([SCRIPT, "window", "--week-end", week_end], 60, "window.log")
    if code != 0:
        reply_fn(message_id, f"❌ 获取窗口失败：\n{_tail(out)}")
        return
    lines = [l for l in out.strip().splitlines() if "放进文件夹" not in l]
    lines.append(f"  · 文件名随意，放进 {WS['正式']['inbox']} 后发 /经营周报 导入 {week_end}")
    reply_fn(message_id, "\n".join(lines))


def _do_import(message_id, reply_fn, week_end):
    ws = WS["正式"]
    files = [f for f in os.listdir(ws["inbox"]) if os.path.isfile(os.path.join(ws["inbox"], f))]
    if not files:
        reply_fn(message_id, f"📭 收件箱是空的：{ws['inbox']}\n请把领星导出(和SP-API .bin)放进去再发 /经营周报 导入")
        return
    man, out = _import(ws, ws["inbox"], week_end, copy=False)
    if not man:
        reply_fn(message_id, f"❌ 识别失败：\n{_tail(out)}")
        return
    moved = man.get("last_import", {}).get("moved", [])
    msg = [f"📥 已归档 {len(moved)} 个文件到 {week_end}：" + "、".join(m["type"] for m in moved), _manifest_text(man),
           "✅ 必需文件齐全，可以发 /经营周报 生成" if man["ok"] else "⚠️ 补齐标红的文件后再导入"]
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
            return "❌ 还没有推送群：请先在目标群发 /经营周报 绑定"
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

logging.info(f"[经营周报] 插件已加载：指令 /经营周报；AI={'可用' if _HAS_AI else '不可用'}；测试周={_latest_test_week() or '无(测试文件夹为空)'}；"
             + _sched_status())
