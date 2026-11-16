"""Cortex Agent の検証: CoWork のシーン1〜4 を DATA_AGENT_RUN で実行し、ツール選択と Search フィルタを記録する"""
import json
import subprocess
from pathlib import Path

AGENT = "GLACIERSTYLE_DEC_DB.EC_ANALYTICS_SCHEMA.CS_QUALITY_AGENT"
SCENES = [
    ("シーン1", "10月から照明カテゴリの返品率が上がっているのはなぜ？通話記録も調べて"),
    ("シーン2", "LEDデスクライトのちらつきについて、品質管理部は原因を把握していますか？"),
    ("シーン3", "LEDデスクライトを返品したいお客様には、どう案内すればいい？お客様向けの案内と、社内の対応手順を分けて教えて"),
    ("シーン4", "ロット LOT-2609-B を買ったお客様で、通話の解約リスクが高い人は何人？会員ランク別に出して"),
]
OUT = Path(__file__).with_name("agent_results.json")


def run(question):
    body = json.dumps({"messages": [{"role": "user", "content": [{"type": "text", "text": question}]}]}, ensure_ascii=False)
    sql = f"SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN('{AGENT}', $${body}$$) AS r"
    p = subprocess.run(["snow", "sql", "-c", "kshimada_aws_ne1_bce", "--format", "json", "-q", sql],
                       capture_output=True, text=True, timeout=900)
    return json.loads(json.loads(p.stdout[p.stdout.index("["):])[0]["R"])


results = {}
for name, q in SCENES:
    resp = run(q)
    steps, answer = [], ""
    for c in resp.get("content", []):
        if c["type"] == "tool_use":
            tu = c["tool_use"]
            steps.append({"tool": tu["name"], "input": tu.get("input")})
        elif c["type"] == "text":
            answer += c["text"]
    results[name] = {"question": q, "steps": steps, "answer": answer}
    print("=" * 10, name, q)
    for s in steps:
        inp = s["input"] or {}
        print("  ->", s["tool"], json.dumps({k: inp[k] for k in ("query", "filter", "sql") if k in inp}, ensure_ascii=False)[:400])
    print("  回答:", answer[:600].replace("\n", " "))
OUT.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
