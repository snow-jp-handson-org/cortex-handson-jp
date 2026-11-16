"""docs/usecase_and_data_20261216.html の画像プレースホルダを data URI に置き換える（artifact はリモート画像を読めないため）"""
import base64
from pathlib import Path

DOCS = Path(__file__).resolve().parent.parent / "docs"
html_path = DOCS / "usecase_and_data_20261216.html"
images = {"__DASHBOARD_IMG__": DOCS / "images" / "dashboard_cs_quality.jpg"}

html = html_path.read_text(encoding="utf-8")
for token, img in images.items():
    if token in html:
        uri = "data:image/jpeg;base64," + base64.b64encode(img.read_bytes()).decode()
        html = html.replace(token, uri)
        print(f"【完了】{img.name} を埋め込みました")
html_path.write_text(html, encoding="utf-8")
