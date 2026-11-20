"""
補足パート用: 旧 SNS データ（2024年12月）を 2026年のデータに合わせて変換する
- 日付: 2024-12-01〜18 → 2026-10-15〜11-30 に引き伸ばす（10月以降の不具合シグナルと重ねるため）
- 言及商品: 旧商品名 → 新商品マスタ（data/products.csv）の近い商品名に置き換え（本文中の商品名も置換）
- LEDデスクライト 調光式 への言及のうち、10月以降の約半分を「ちらつき」の声に差し替える
- 投稿IDの年も 2026 にする
固定シードで再現可能。元データは git 履歴（main ブランチ）に残る。

実行: python3 tools/convert_sns_logs.py
"""
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(20261216)
DATA = Path(__file__).resolve().parent.parent / "data"

# 旧商品名 -> 新商品名（新しい商品マスタにある名前）
PRODUCT_MAP = {
    "LED調光デスクライト": "LEDデスクライト 調光式",
    "モダンデスクライト": "LEDデスクライト 調光式",
    "スタイリッシュデスクライト": "クリップ式LEDライト",
    "デザインシーリングライト": "ガラスシェードペンダント",
    "北欧風フロアライト": "ウッドフロアライト",
    "吊り下げペンダントライト": "ガラスシェードペンダント",
    "北欧風ペンダントライト": "北欧風ペンダントライト",
    "収納ボックスセット": "ファブリック収納ボックス 3個組",
    "ウォールシェルフ": "壁面ウッドシェルフ",
    "収納付きデスク": "昇降式ワークデスク",
    "コーヒーメーカー": "電気ケトル 0.8L",
    "耐熱ガラスポット": "ダブルウォールグラス",
    "ステンレスキッチンツールセット": "三徳包丁 ステンレス",
    "オーガニックコットンTシャツ": "ガーゼパジャマ",
    "リネンシャツ": "フランネルパジャマ",
    "プレミアムカシミヤセーター": "カシミヤマフラー",
    "メリノウールカーディガン": "ウールストール",
    "ワイヤレスイヤホンPro": "トラベルポーチ 3点セット",
    "スマートスピーカー": "アロマディフューザーセット",
    "北欧インテリア図鑑": "リングノート A5",
}
DEFECT_PRODUCT = "LEDデスクライト 調光式"
DEFECT_VOICES = [
    "GlacierStyleで買ったLEDデスクライト 調光式、使って10分くらいでちらつき始める…目が疲れる",
    "LEDデスクライト 調光式、明るさ最大にするとチカチカする。初期不良かな？",
    "先月届いたLEDデスクライト 調光式、夜に急に暗くなったり明るくなったりする。問い合わせ中",
    "LEDデスクライト 調光式のちらつき、同じ症状の人いる？サポートに電話したけど解決してない",
]
OLD_START, OLD_END = datetime(2024, 12, 1), datetime(2024, 12, 19)
NEW_START, NEW_END = datetime(2026, 10, 15), datetime(2026, 12, 1)


def shift(ts: str) -> str:
    dt = datetime.fromisoformat(ts)
    naive = dt.replace(tzinfo=None)
    ratio = (naive - OLD_START) / (OLD_END - OLD_START)
    new = NEW_START + (NEW_END - NEW_START) * ratio
    return new.replace(microsecond=0, second=0).isoformat() + ts[19:]  # 元のタイムゾーン表記を残す


def main():
    products = {r["product_name"] for r in csv.DictReader(open(DATA / "products.csv", encoding="utf-8"))}
    assert set(PRODUCT_MAP.values()) <= products, set(PRODUCT_MAP.values()) - products

    path = DATA / "sns_logs.json"
    posts = json.loads(path.read_text(encoding="utf-8"))
    defect_posts = 0
    for p in posts:
        p["posted_at"] = shift(p["posted_at"])
        p["post_id"] = p["post_id"].replace("-2024-", "-2026-")
        old = p.get("mentioned_products") or []
        p["mentioned_products"] = [PRODUCT_MAP.get(x, x) for x in old]
        for o in old:
            if o in PRODUCT_MAP:
                p["content"] = p["content"].replace(o, PRODUCT_MAP[o])
        if DEFECT_PRODUCT in p["mentioned_products"] and random.random() < 0.5:
            p["content"] = random.choice(DEFECT_VOICES)
            p["hashtags"] = ["グレイシャースタイル", "デスクライト"]
            defect_posts += 1
    posts.sort(key=lambda x: x["posted_at"])
    path.write_text(json.dumps(posts, ensure_ascii=False, indent=2), encoding="utf-8")

    unknown = {m for p in posts for m in p["mentioned_products"]} - products
    print(f"【完了】SNS {len(posts)} 件を変換しました（期間 {posts[0]['posted_at'][:10]}〜{posts[-1]['posted_at'][:10]}、ちらつきの声 {defect_posts} 件）")
    assert not unknown, f"新商品マスタにない商品名: {unknown}"


if __name__ == "__main__":
    main()
