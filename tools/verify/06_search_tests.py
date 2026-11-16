"""Cortex Search のフィルタ検証（UC3・シーン1・シーン2）"""
import json, subprocess, sys
SVC = "GLACIERSTYLE_DEC_DB.EC_ANALYTICS_SCHEMA.KNOWLEDGE_SEARCH"
CASES = [
  ("UC3-1 全件", "返品できますか", None),
  ("UC3-2 社外公開", "返品できますか", {"@eq": {"audience": "社外公開"}}),
  ("UC3-3 CS部×社内限定", "返品できますか", {"@and": [{"@eq": {"department": "カスタマーサポート部"}}, {"@eq": {"audience": "社内限定"}}, {"@not": {"@eq": {"doc_type": "通話"}}}]}),
  ("S1 通話×照明×10月以降×ネガ", "明かりがちらつく", {"@and": [{"@eq": {"doc_type": "通話"}}, {"@eq": {"product_category": "照明"}}, {"@gte": {"doc_date": "2026-10-01"}}, {"@eq": {"sentiment": "ネガティブ"}}]}),
  ("S2 品質報告×対象商品", "ちらつきの原因", {"@and": [{"@eq": {"doc_type": "品質報告"}}, {"@contains": {"product_ids": "PRD-LGT-0001"}}]}),
  ("S3 対象ロットの返品対応(社内)", "LEDデスクライトの返品の送料と期限", {"@and": [{"@eq": {"audience": "社内限定"}}, {"@not": {"@eq": {"doc_type": "通話"}}}]}),
]
sqls = []
for name, q, f in CASES:
    body = {"query": q, "columns": ["chunk_text", "title", "doc_type", "audience", "doc_date"], "limit": 3}
    if f: body["filter"] = f
    sqls.append(f"SELECT '{name}' AS case_name, SNOWFLAKE.CORTEX.SEARCH_PREVIEW('{SVC}', $${json.dumps(body, ensure_ascii=False)}$$) AS r")
out = subprocess.run(["snow", "sql", "-c", "kshimada_aws_ne1_bce", "--format", "json", "-q", " UNION ALL ".join(sqls)], capture_output=True, text=True)
rows = json.loads(out.stdout[out.stdout.index("["):])
for row in rows:
    print("=" * 8, row["CASE_NAME"])
    for h in json.loads(row["R"])["results"]:
        print(f"  [{h['doc_type']}/{h['audience']}/{h['doc_date']}] {h['title']} :: {h['chunk_text'][:110].replace(chr(10), ' ')}")
