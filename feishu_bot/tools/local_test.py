# tools/local_test.py
# 本地模拟飞书调用周报插件(不需要网关/店铺后端/飞书/AI)：
#   - 伪造 config / feishu_gateway / ai_runner 三个上层模块
#   - 伪造的 ai_runner 返回一份固定格式的周报(A~E节)，并把收到的 prompt 存到 logs/ 供检查
#   - 发文件改为打印(不请求飞书)
# 真实 AI 内容需在机器人里用 /周报 测试 验证。
#
# 用法：
#   python tools/local_test.py "/周报 测试" "/周报 发送 2026-09-26" "/周报"

import json, os, sys, threading, time, types

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLUGINS = os.path.join(ROOT, "plugins")
os.environ["WEEKLY_REPORT_NO_SCHEDULER"] = "1"
SENT = []


def _fake_ai(prompt, timeout=600, model=None):
    log = os.path.join(PLUGINS, "weekly_report", "logs", "last_ai_prompt.md")
    with open(log, "w", encoding="utf-8") as f:
        f.write(prompt)
    return ("# 亚马逊周报(模拟AI输出)\n\n## A. 本周执行清单\n| 序号 | 对象 | 动作(含数值) | 依据数据 | 预计影响 | 置信度 | 下周验证指标 |\n|---|---|---|---|---|---|---|\n"
            "| 1 | tangliuquan-US/ZJCY076 | 空运补货175件、降竞价10% | 第13天断货 | 避免断货 | 中 | 可售天数 |\n"
            "| 2 | tangliuquan-US/ZJTX073 | 本周海运发59件 | 第39天断货 | 避免断货 | 中 | 发货单 |\n"
            "| 3 | tangliuquan-US/ZJPL065 | 降价清库龄 | 库存262天 | 降仓储费 | 低 | 库存天数 |\n\n"
            f"## B. 暂不动作\n(模拟)\n\n## C. 需要人工核实\n(模拟)\n\n## D. 需要补充的数据\n(模拟)\n\n## E. 相对上周的判断修正\n无\n\n"
            f"<!-- prompt 长度 {len(prompt)} 字符 -->\n")


def main(cmds):
    sys.modules["config"] = types.SimpleNamespace(CHAT_ID="oc_test_chat", STORE_NAME="店铺A", GATEWAY_BASE_URL="http://127.0.0.1:8000")
    sys.modules["feishu_gateway"] = types.SimpleNamespace(
        APP_ID="x", APP_SECRET="x", send_to_chat=lambda c, t: print(f"\n[推送到群 {c}] {t}"),
        get_chat_id_by_msg=lambda mid: "oc_test_chat")
    sys.modules["ai_runner"] = types.SimpleNamespace(run_ai=_fake_ai)
    sys.path.insert(0, PLUGINS)
    import importlib.util
    spec = importlib.util.spec_from_file_location("周报_plugin", os.path.join(PLUGINS, "周报_plugin.py"))
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
    main(sys.argv[1:] or ["/周报"])
