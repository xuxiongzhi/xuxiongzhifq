# render_pdf.py
# AI 周报 Markdown → 带样式的 HTML → 用本机 Chrome/Edge 无界面打印成 PDF(版式参考《全店经营速览报告》)。
# 依赖：pip install markdown；浏览器用本机已装的 Chrome(送仓时间插件也在用)或 Edge。
#
# 命令行测试：python render_pdf.py 周报_2026-09-26.md        → 同目录生成 .html 和 .pdf

import os, re, sys, glob, shutil, tempfile, subprocess
from datetime import datetime

BROWSER_CANDIDATES = [
    os.environ.get("CHROME_PATH", ""),
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    os.path.join(os.environ.get("LOCALAPPDATA", ""), r"Google\Chrome\Application\chrome.exe"),
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
] + [shutil.which(n) or "" for n in ("chrome", "google-chrome", "chromium", "chromium-browser", "msedge")] \
  + sorted(glob.glob("/opt/pw-browsers/chromium-*/chrome-linux/chrome"))

CSS = """
@page { size: A4; margin: 14mm 12mm 16mm 12mm; }
body { font-family: "Microsoft YaHei", "PingFang SC", "Noto Sans CJK SC", "WenQuanYi Zen Hei", "Segoe UI", sans-serif;
       font-size: 10.5pt; line-height: 1.55; color: #1f2328; margin: 0; }
h1 { font-size: 19pt; margin: 0 0 4px; padding-bottom: 6px; border-bottom: 3px solid #1f4e8c; color: #12325c; }
h2 { font-size: 13.5pt; margin: 18px 0 6px; padding: 4px 8px; background: #eef3fa; border-left: 4px solid #1f4e8c; color: #12325c;
     break-after: avoid; }
h3 { font-size: 11.5pt; margin: 12px 0 4px; color: #1f4e8c; break-after: avoid; }
p { margin: 4px 0 6px; }
ul, ol { margin: 4px 0 6px 18px; padding: 0; }
li { margin: 2px 0; }
table { border-collapse: collapse; width: 100%; margin: 6px 0 10px; font-size: 9pt; break-inside: auto; }
tr { break-inside: avoid; }
th { background: #1f4e8c; color: #fff; font-weight: 600; padding: 4px 6px; text-align: left; border: 1px solid #1f4e8c; white-space: nowrap; }
td { padding: 3px 6px; border: 1px solid #d0d7de; vertical-align: top; }
tr:nth-child(even) td { background: #f6f8fa; }
code { font-family: Consolas, monospace; font-size: 9pt; background: #f2f2f2; padding: 0 3px; border-radius: 3px; }
blockquote { margin: 6px 0; padding: 4px 10px; border-left: 3px solid #d0d7de; color: #57606a; background: #fafbfc; }
hr { border: 0; border-top: 1px solid #d0d7de; margin: 12px 0; }
.meta { color: #57606a; font-size: 9pt; margin-bottom: 10px; }
.part { margin-top: 22px; font-size: 15pt; color: #fff; background: #12325c; border: 0; padding: 6px 10px; break-before: page; }
.part:first-of-type { break-before: auto; }
"""


def md_to_html(md_text: str, title: str) -> str:
    import markdown
    md_text = re.sub(r"<!--.*?-->", "", md_text, flags=re.S).strip()
    # 表格前必须有空行，否则 markdown 库不识别；AI 输出常常紧贴上一行
    md_text = re.sub(r"(?m)^([^|\n].*)\n(\|)", r"\1\n\n\2", md_text)
    # 表格后紧跟的非表格行(标题/正文)也要空一行，否则会被并进表格
    md_text = re.sub(r"(?m)^(\|.*)\n(?=[^|\n])", r"\1\n\n", md_text)
    body = markdown.markdown(md_text, extensions=["tables", "sane_lists", "nl2br"])
    # "第一部分/第二部分" 标题用醒目分隔(第二部分另起一页)
    body = re.sub(r"<h2>(第[一二三四五六七八九十]+部分[^<]*)</h2>", r'<h2 class="part">\1</h2>', body)
    meta = f'<div class="meta">生成时间 {datetime.now():%Y-%m-%d %H:%M} · 金额单位 USD · 数据来源：领星导出 + SP-API（详见“分析口径”）</div>'
    body = re.sub(r"(</h1>)", r"\1" + meta, body, count=1) if "</h1>" in body else meta + body
    return (f'<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>{title}</title>'
            f"<style>{CSS}</style></head><body>{body}</body></html>")


def find_browser() -> str | None:
    for p in BROWSER_CANDIDATES:
        if p and os.path.isfile(p):
            return p
    return None


def html_to_pdf(html_path: str, pdf_path: str, timeout: int = 120) -> None:
    browser = find_browser()
    if not browser:
        raise RuntimeError("找不到 Chrome/Edge，可设置环境变量 CHROME_PATH 指向 chrome.exe")
    profile = tempfile.mkdtemp(prefix="wr_pdf_")          # 独立临时配置目录，不和正在运行的 Chrome(RPA)抢配置
    try:
        url = "file:///" + os.path.abspath(html_path).replace("\\", "/").lstrip("/")
        cmd = [browser, "--headless=new", "--disable-gpu", "--no-sandbox", "--no-first-run", "--disable-extensions",
               f"--user-data-dir={profile}", "--no-pdf-header-footer", "--print-to-pdf-no-header",
               f"--print-to-pdf={os.path.abspath(pdf_path)}", url]
        if os.path.exists(pdf_path):
            os.remove(pdf_path)
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout)
        if not os.path.exists(pdf_path) or os.path.getsize(pdf_path) < 1000:
            raise RuntimeError(f"浏览器没有生成 PDF(exit {proc.returncode})：{(proc.stderr or proc.stdout or '')[-300:]}")
    finally:
        shutil.rmtree(profile, ignore_errors=True)


def md_file_to_pdf(md_path: str) -> str:
    """周报_xxx.md → 同名 .html 与 .pdf，返回 pdf 路径；失败抛异常"""
    base = os.path.splitext(md_path)[0]
    with open(md_path, "r", encoding="utf-8") as f:
        md_text = f.read()
    title = os.path.basename(base)
    with open(base + ".html", "w", encoding="utf-8") as f:
        f.write(md_to_html(md_text, title))
    html_to_pdf(base + ".html", base + ".pdf")
    return base + ".pdf"


if __name__ == "__main__":
    print(md_file_to_pdf(sys.argv[1]))
