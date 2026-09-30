# tools/local_test.py
# 本地模拟飞书调用周报插件(不需要网关/店铺后端/飞书)：
#   - 伪造店铺后端的 config 模块(GATEWAY_BASE_URL 指向本机假网关)
#   - 起一个假网关，只实现 /gw_proxy/send_file(检查文件存在并记录)
#   - 依次把指令交给插件 handle()，打印 reply_fn 收到的文字/卡片
#
# 用法：
#   python tools/local_test.py "/周报 导入 2026-09-26" "/周报 测试 2026-09-26" "/周报 发送 2026-09-26"

import json, os, sys, threading, time, types
from http.server import BaseHTTPRequestHandler, HTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLUGINS = os.path.join(ROOT, "plugins")
PORT = 18765
SENT = []


class _GW(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        if self.path == "/gw_proxy/send_file":
            ok = os.path.isfile(body.get("file_path", ""))
            SENT.append(body)
            res = {"ok": ok, "error": "" if ok else "文件不存在"}
        else:
            res = {"msg": "ok"}
        data = json.dumps(res).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers(); self.wfile.write(data)

    def log_message(self, *a):
        pass


def main(cmds):
    threading.Thread(target=HTTPServer(("127.0.0.1", PORT), _GW).serve_forever, daemon=True).start()
    sys.modules["config"] = types.SimpleNamespace(GATEWAY_BASE_URL=f"http://127.0.0.1:{PORT}", CHAT_ID="oc_test", STORE_NAME="店铺A")
    sys.path.insert(0, PLUGINS)
    import importlib.util
    spec = importlib.util.spec_from_file_location("周报_plugin", os.path.join(PLUGINS, "周报_plugin.py"))
    plugin = importlib.util.module_from_spec(spec); spec.loader.exec_module(plugin)

    def reply_fn(mid, text_or_card):
        s = str(text_or_card)
        if s.strip().startswith("{") and '"elements"' in s:
            card = json.loads(s)
            print(f"\n[卡片] {card['header']['title']['content']}")
            for e in card["elements"]:
                print("  ────" if e["tag"] == "hr" else "  " + e.get("content", "").replace("\n", "\n  "))
        else:
            print(f"\n[文字] {s}")

    for i, cmd in enumerate(cmds, 1):
        print(f"\n{'=' * 70}\n>>> {cmd}")
        before = {t.name for t in threading.enumerate()}
        handled = plugin.handle(f"msg_{i}", cmd, reply_fn, user_id="ou_test")
        print(f"(handle 返回 {handled})")
        time.sleep(0.5)
        for t in threading.enumerate():                 # 等后台任务结束
            if t.name.startswith("周报-") and t.name not in before:
                t.join()
    if SENT:
        print("\n假网关收到的附件：" + "、".join(os.path.basename(s["file_path"]) for s in SENT))


if __name__ == "__main__":
    main(sys.argv[1:] or ["/周报"])
