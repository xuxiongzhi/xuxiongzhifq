# tools/local_test.py
# 本地模拟飞书调用周报插件(不需要网关/店铺后端/飞书/AI)：
#   - 伪造 config / feishu_gateway / ai_runner 三个上层模块
#   - 伪造的 ai_runner 返回固定格式的周报；第二部分从候选动作里选ID(故意混入错误)，检验插件校验；prompt 存到 logs/ 供检查
#   - 发文件改为打印(不请求飞书)
# 真实 AI 内容需在机器人里用 /经营周报 测试 验证。
#
# 用法：
#   python tools/local_test.py "/经营周报 测试" "/经营周报 发送 2026-09-26" "/经营周报"

import json, os, re, sys, threading, time, types

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLUGINS = os.path.join(ROOT, "plugins")
os.environ["WEEKLY_REPORT_NO_SCHEDULER"] = "1"
SENT = []


def _fake_ai(prompt, timeout=600, model=None):
    part = 2 if "只输出**第二部分" in prompt else (0 if "一次输出完整周报" in prompt else 1)
    if part == 0:                                   # 一次调用：两部分拼在一起
        with open(os.path.join(PLUGINS, "weekly_report", "logs", "last_ai_prompt_0.md"), "w", encoding="utf-8") as f:
            f.write(prompt)
        return _fake_ai(prompt.replace("一次输出完整周报", "")) + "\n\n" + _fake_ai(prompt + "只输出**第二部分")
    with open(os.path.join(PLUGINS, "weekly_report", "logs", f"last_ai_prompt_{part}.md"), "w", encoding="utf-8") as f:
        f.write(prompt)
    if part == 1:
        return ("# 亚马逊周报(模拟AI输出)\n\n## 第一部分 数据报告\n## 一、执行摘要\n(模拟)\n## 二、分析口径\n## 三、核心指标总览\n## 四、本期最异常指标\n"
                "## 五、板块分析\n## 六、板块交叉对比\n## 七、最该关注的3个问题与3个机会\n(模拟)")
    # 第二部分：从数据包"5. 候选动作"里挑 ID 输出 JSON，并故意混入几种错误，检验插件的校验能否拦住
    rows = re.findall(r"(?m)^\| ([A-Z]{2}\d{2}) \|.*\| (是|否) \| [^|]*\|$", prompt)
    ok = [i for i, y in rows if y == "是"]
    bad = [i for i, y in rows if y == "否"]
    pick = lambda pre: next((i for i in ok if i.startswith(pre)), None)
    sel = [{"id": pick("KA"), "优先级": "P0", "理由": "(模拟)空运前断货", "验证指标": "下周FBA可售"},
           {"id": pick("CT"), "优先级": "P0", "理由": "(模拟)控速", "验证指标": "日均销量"},
           {"id": pick("KS"), "优先级": "P1", "理由": "(模拟)海运", "验证指标": "发货单"},
           {"id": pick("UP"), "优先级": "P2", "建议值": 9.99, "理由": "(模拟错误)超出允许范围", "验证指标": "ACoS"},
           {"id": pick("TD"), "优先级": "P0", "理由": "(模拟错误)样本不足却给P0", "验证指标": "点击"},
           {"id": "NG99", "优先级": "P2", "理由": "(模拟错误)否定不在候选里的词", "验证指标": "-"}]
    sel += [{"id": i, "优先级": "P1", "理由": "(模拟错误)断货父体加投", "验证指标": "-"} for i in bad[:2]]
    js = json.dumps({"执行清单": [x for x in sel if x["id"]],
                     "人工事项": [{"对象": "tangliuquan-US/ZJPL066", "动作": "核对我方价格29.71高于竞品最低价27.81", "理由": "(模拟)3b"}]},
                    ensure_ascii=False, indent=1)
    return ("## 第二部分 执行建议\n## 八、执行建议清单\n```json\n" + js + "\n```\n"
            "## 九、暂不动作\n## 十、执行前需要人工核对的事项\n## 十一、数据限制与缺口\n## 十二、相对上周的判断修正\n无\n")


def main(cmds):
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", force=True)   # 与店铺后端控制台同格式
    sys.modules["config"] = types.SimpleNamespace(CHAT_ID="oc_test_chat", STORE_NAME="店铺A", GATEWAY_BASE_URL="http://127.0.0.1:8000")
    sys.modules["feishu_gateway"] = types.SimpleNamespace(
        APP_ID="x", APP_SECRET="x", send_to_chat=lambda c, t: print(f"\n[推送到群 {c}] {t}"),
        get_chat_id_by_msg=lambda mid: "oc_test_chat")
    sys.modules["ai_runner"] = types.SimpleNamespace(run_ai=_fake_ai)
    sys.path.insert(0, PLUGINS)
    import importlib.util
    spec = importlib.util.spec_from_file_location("周报_plugin", os.path.join(PLUGINS, "经营周报_plugin.py"))
    plugin = importlib.util.module_from_spec(spec); spec.loader.exec_module(plugin)

    def fake_send_file(path, message_id="", chat_id=""):
        SENT.append(path)
        print(f"\n[发送文件] {os.path.basename(path)}  ({os.path.getsize(path)} 字节) → {'群 ' + chat_id if chat_id else '回复 ' + message_id}")
    plugin._send_file = fake_send_file

    def reply_fn(mid, text):
        print(f"\n[回复 {mid}] {text}")

    for i, cmd in enumerate(cmds, 1):
        print(f"\n{'=' * 70}\n>>> {cmd}")
        before = {t.name for t in threading.enumerate()}
        plugin.handle(f"msg_{i}", cmd, reply_fn, user_id="ou_test")
        time.sleep(0.5)
        for t in threading.enumerate():
            if t.name.startswith("周报-") and t.name not in before:
                t.join()
    return plugin


if __name__ == "__main__":
    main(sys.argv[1:] or ["/经营周报"])
