# 网关新增 `/gw_proxy/send_file`（需人工合并到 feishu_gateway.py）

店铺后端进程里不能 `import feishu_gateway`（拿不到 APP_SECRET），所以插件发附件要走网关代理。
把下面这段加到 `feishu_gateway.py` 里 `@app.post("/gw_proxy/send_to_user")` 之后即可。
网关与店铺后端在同一台机器上，插件只传文件的绝对路径，由网关读取并上传。

```python
@app.post("/gw_proxy/send_file")
async def gw_proxy_send_file(request: Request):
    """上传本机文件并发到群(chat_id)或回复某条消息(message_id)。返回 {"ok": bool, "error": str}"""
    data = await request.json()
    path = data.get("file_path", "")
    message_id = data.get("message_id", "")
    chat_id = data.get("chat_id", "")
    if not path or not os.path.isfile(path):
        return {"ok": False, "error": f"文件不存在：{path}"}
    if os.path.getsize(path) > 30 * 1024 * 1024:
        return {"ok": False, "error": "文件超过飞书 30MB 上限"}
    try:
        headers = {"Authorization": f"Bearer {_get_token()}"}
        name = os.path.basename(path)
        with open(path, "rb") as f:
            up = requests.post("https://open.feishu.cn/open-apis/im/v1/files", headers=headers,
                               data={"file_type": "stream", "file_name": name},
                               files={"file": (name, f)}, timeout=120).json()
        if up.get("code") != 0:
            return {"ok": False, "error": f"上传失败：{up.get('msg')}"}
        content = json.dumps({"file_key": up["data"]["file_key"]})
        if message_id and not message_id.startswith("scheduler"):
            url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply"
            body = {"msg_type": "file", "content": content}
        else:
            url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
            body = {"receive_id": chat_id or gateway_config.TARGET_CHAT_ID, "msg_type": "file", "content": content}
        res = requests.post(url, headers={**headers, "Content-Type": "application/json; charset=utf-8"},
                            json=body, timeout=30).json()
        return {"ok": res.get("code") == 0, "error": "" if res.get("code") == 0 else res.get("msg", "")}
    except Exception as e:
        logging.error(f"[gateway] send_file 失败: {e}")
        return {"ok": False, "error": str(e)}
```

合并后重启网关即可；插件的 `/周报 发送` 会自动改用这个接口（接口不存在时插件会提示“网关还没有 /gw_proxy/send_file 接口”，不会报错）。
其它插件（库存处理、补货发货同步）里 `import feishu_gateway` 发文件的写法在店铺后端进程里同样会失败，也可以改成调用这个接口。
