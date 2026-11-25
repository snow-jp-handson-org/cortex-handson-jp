"""
12/16 ハンズオン用サンプルデータ生成スクリプト
- 構造化: customers.csv / products.csv / orders.csv
- 通話: tools/call_scripts.csv（音声の台本）、voice_logs_metadata.json の既存10件の日付更新
  台本を変えたら tools/generate_voice.py → tools/build_voice_metadata.py の順に実行する
- 期間: 2026-06-01 〜 2026-11-30
固定シードで再現可能。ユースケース（docs/usecase_and_data_20261216.html）のシグナルを仕込み、最後に assert で確認する。

実行: uv run --with pandas python tools/generate_data.py
"""
import csv
import json
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

SEED = 20261216
random.seed(SEED)
ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

START = datetime(2026, 6, 1)
END = datetime(2026, 11, 30, 23, 59)

# 不具合シグナルの対象（ユースケースの正解）
DEFECT_PRODUCT_ID = "PRD-LGT-0001"
DEFECT_LOT = "LOT-2609-B"
DEFECT_START = datetime(2026, 10, 1)

# ---------------------------------------------------------------------------
# 商品マスタ（約60件）
# ---------------------------------------------------------------------------
PRODUCT_SPECS = [
    # (prefix, L1, L2, [(L3, 商品名, 価格)])
    ("LGT", "インテリア", "照明", [
        ("デスクライト", "LEDデスクライト 調光式", 8980),
        ("デスクライト", "クリップ式LEDライト", 3980),
        ("ペンダントライト", "北欧風ペンダントライト", 14800),
        ("ペンダントライト", "ガラスシェードペンダント", 19800),
        ("フロアライト", "ウッドフロアライト", 16800),
        ("フロアライト", "アーチ型フロアライト", 24800),
        ("間接照明", "LEDテープライト 3m", 2980),
        ("間接照明", "キャンドル型LEDライト", 1980),
    ]),
    ("FRN", "インテリア", "家具", [
        ("テーブル", "オーク材ローテーブル", 29800),
        ("テーブル", "折りたたみサイドテーブル", 6980),
        ("チェア", "ラタンラウンジチェア", 34800),
        ("チェア", "スタッキングチェア", 7980),
        ("ソファ", "2人掛けファブリックソファ", 59800),
        ("ソファ", "フロアクッションソファ", 19800),
        ("デスク", "昇降式ワークデスク", 44800),
    ]),
    ("STR", "インテリア", "収納", [
        ("ラック", "スチールオープンラック", 12800),
        ("ラック", "壁面ウッドシェルフ", 8980),
        ("ボックス", "ファブリック収納ボックス 3個組", 3480),
        ("ボックス", "スタッキングコンテナ", 2480),
        ("ワゴン", "キッチンワゴン 3段", 6980),
        ("チェスト", "桐チェスト 4段", 39800),
    ]),
    ("KIT", "キッチン", "調理器具", [
        ("鍋", "ホーロー両手鍋 20cm", 9800),
        ("鍋", "鋳物ココット 18cm", 16800),
        ("フライパン", "鉄フライパン 26cm", 5980),
        ("フライパン", "セラミックフライパン 24cm", 4980),
        ("包丁", "三徳包丁 ステンレス", 6980),
        ("家電", "電気ケトル 0.8L", 7980),
        ("家電", "ハンドブレンダー", 9980),
    ]),
    ("TBW", "キッチン", "食器", [
        ("プレート", "波佐見焼プレート 2枚組", 4400),
        ("プレート", "リム皿 ホワイト", 1980),
        ("カップ", "ダブルウォールグラス", 2980),
        ("カップ", "マグカップ 北欧柄", 1980),
        ("カトラリー", "カトラリー 4点セット", 3980),
        ("ボウル", "木製サラダボウル", 3480),
    ]),
    ("STN", "ライフスタイル雑貨", "文具", [
        ("ノート", "リングノート A5", 880),
        ("ペン", "真鍮ボールペン", 4980),
        ("手帳", "2027年 手帳 B6", 2480),
        ("デスク小物", "レザーデスクマット", 5980),
        ("デスク小物", "ウッドペントレイ", 1980),
    ]),
    ("TRV", "ライフスタイル雑貨", "トラベル", [
        ("バッグ", "トラベルトートバッグ", 8980),
        ("バッグ", "軽量リュック 20L", 12800),
        ("ポーチ", "トラベルポーチ 3点セット", 2980),
        ("小物", "ネックピロー", 2480),
    ]),
    ("GFT", "ライフスタイル雑貨", "ギフト", [
        ("ギフトセット", "アロマディフューザーセット", 6980),
        ("ギフトセット", "タオルギフト 今治", 5500),
        ("キャンドル", "ソイキャンドル", 2980),
    ]),
    ("APL", "アパレル", "ファッション小物", [
        ("マフラー", "カシミヤマフラー", 14800),
        ("マフラー", "ウールストール", 8980),
        ("手袋", "レザーグローブ", 9800),
        ("帽子", "ニットキャップ", 3980),
        ("靴下", "オーガニックコットンソックス 3足組", 2480),
    ]),
    ("HOM", "アパレル", "ルームウェア", [
        ("パジャマ", "ガーゼパジャマ", 8980),
        ("パジャマ", "フランネルパジャマ", 9800),
        ("スリッパ", "ルームシューズ", 2980),
        ("ブランケット", "ニットブランケット", 6980),
    ]),
]


def build_products():
    rows = []
    for prefix, l1, l2, items in PRODUCT_SPECS:
        for i, (l3, name, price) in enumerate(items, start=1):
            rows.append({
                "product_id": f"PRD-{prefix}-{i:04d}",
                "product_name": name,
                "category_l1": l1,
                "category_l2": l2,
                "category_l3": l3,
                "brand": "GlacierStyle Original",
                "list_price": price,
                "cost_price": round(price * random.uniform(0.35, 0.55)),
                "launch_date": (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 1000))).date().isoformat(),
            })
    return rows


# ---------------------------------------------------------------------------
# 顧客（300件）
# ---------------------------------------------------------------------------
LAST = ["佐藤", "鈴木", "高橋", "田中", "伊藤", "渡辺", "山本", "中村", "小林", "加藤",
        "吉田", "山田", "佐々木", "山口", "松本", "井上", "木村", "林", "斎藤", "清水"]
FIRST_F = ["美咲", "陽菜", "結衣", "さくら", "葵", "由美子", "恵子", "優子", "真由美", "彩"]
FIRST_M = ["翔太", "大輔", "健太", "拓也", "直樹", "誠", "浩二", "隆", "悠斗", "蓮"]
PREFS = [("東京都", "渋谷区"), ("東京都", "世田谷区"), ("神奈川県", "横浜市"), ("埼玉県", "さいたま市"),
         ("千葉県", "船橋市"), ("大阪府", "大阪市"), ("愛知県", "名古屋市"), ("福岡県", "福岡市"),
         ("北海道", "札幌市"), ("兵庫県", "神戸市")]
TIERS = ["standard"] * 5 + ["silver"] * 3 + ["gold"] * 2 + ["platinum"]


def build_customers(n=300):
    rows = []
    for i in range(1, n + 1):
        gender = random.choice(["female", "male"])
        first = random.choice(FIRST_F if gender == "female" else FIRST_M)
        pref, city = random.choice(PREFS)
        rows.append({
            "customer_id": f"CUS-{i:06d}",
            "last_name": random.choice(LAST),
            "first_name": first,
            "gender": gender,
            "birth_date": (datetime(1965, 1, 1) + timedelta(days=random.randint(0, 12000))).date().isoformat(),
            "email": f"customer{i}@example.com",
            "phone": f"0{random.choice([70, 80, 90])}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}",
            "prefecture": pref,
            "city": city,
            "registration_date": (datetime(2019, 1, 1) + timedelta(days=random.randint(0, 2700))).date().isoformat(),
            "membership_tier": random.choice(TIERS),
        })
    return rows


# ---------------------------------------------------------------------------
# 注文（約3,000件）
# ---------------------------------------------------------------------------
MONTH_WEIGHT = {6: 0.9, 7: 1.0, 8: 0.9, 9: 0.95, 10: 1.0, 11: 1.45}  # 11月はセール


def rand_datetime_in_month(m):
    start = datetime(2026, m, 1)
    end = datetime(2026, m + 1, 1) if m < 12 else datetime(2027, 1, 1)
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds()) - 1))


def build_orders(products, customers, base_per_month=470):
    rows = []
    seq = 0
    lighting = [p for p in products if p["category_l2"] == "照明"]
    for m, w in MONTH_WEIGHT.items():
        n = int(base_per_month * w)
        for _ in range(n):
            # 照明は人気カテゴリ。不具合商品は10月以降に売れ行きが落ちる
            # 照明は人気カテゴリ。不具合商品は照明の売れ筋（9月まで照明の約半分）で、10月以降は売れ行きが落ちる
            if random.random() < 0.22:
                defect_share = 0.5 if m <= 9 else 0.3
                if random.random() < defect_share:
                    p = next(x for x in lighting if x["product_id"] == DEFECT_PRODUCT_ID)
                else:
                    p = random.choice([x for x in lighting if x["product_id"] != DEFECT_PRODUCT_ID])
            else:
                p = random.choice(products)
            seq += 1
            qty = random.choices([1, 2, 3], weights=[80, 15, 5])[0]
            unit = p["list_price"]
            discount = round(unit * qty * (0.15 if m == 11 and random.random() < 0.6 else 0))
            subtotal = unit * qty - discount
            # 返品率: 通常 3%。不具合商品は10月以降 40%
            ret_rate = 0.03
            if p["product_id"] == DEFECT_PRODUCT_ID and m >= 10:
                ret_rate = 0.40
            status = "returned" if random.random() < ret_rate else ("cancelled" if random.random() < 0.02 else "completed")
            rows.append({
                "order_id": f"ORD-2026-{seq:06d}",
                "order_datetime": rand_datetime_in_month(m).strftime("%Y-%m-%d %H:%M:%S"),
                "customer_id": random.choice(customers)["customer_id"],
                "product_id": p["product_id"],
                "quantity": qty,
                "unit_price": unit,
                "discount_amount": discount,
                "total_amount": round(subtotal * 1.1),
                "order_channel": random.choices(["web", "app", "store"], weights=[60, 30, 10])[0],
                "order_status": status,
                "lot_no": (DEFECT_LOT if p["product_id"] == DEFECT_PRODUCT_ID and m >= 9 else ""),
            })
    rows.sort(key=lambda r: r["order_datetime"])
    return rows


# ---------------------------------------------------------------------------
# 通話の文字起こし（約100件）
# ---------------------------------------------------------------------------
AGENTS = ["AGT-00312", "AGT-00214", "AGT-00614", "AGT-00456", "AGT-00521", "AGT-00133"]
OPENING = "お電話ありがとうございます。GlacierStyleカスタマーサポートの{agent}でございます。"


def talk(lines):
    return "\n".join(f"{who}: {text}" for who, text in lines)


def call_defect(c, p, order, repeat=False):
    extra = ("前にも電話したんですが、まだ解決していません。正直、もう他のお店で買おうかと思っています。"
             if repeat else "")
    return "商品不具合", "ネガティブ", talk([
        ("オペレーター", OPENING.format(agent="木下")),
        ("顧客", f"{c['last_name']}と申します。注文番号は{order['order_id']}です。先月買った{p['product_name']}なんですが、"
                 f"使い始めて数日で明かりがちらつくようになりました。{extra}"),
        ("オペレーター", "ご不便をおかけして申し訳ございません。ご本人確認のため、お電話番号をお願いできますか。"),
        ("顧客", f"{c['phone']}です。"),
        ("オペレーター", "ありがとうございます。ちらつきは、調光を変えたときに起きますか、それとも常にでしょうか。"),
        ("顧客", random.choice(["明るさを最大にすると点滅します。", "電源を入れて10分くらいすると、ずっとちらついています。",
                                "夜に使っていると急に暗くなったり明るくなったりします。"])),
        ("オペレーター", "確認いたします。交換の手配をご案内いたしますので、返送用の伝票をお送りします。"),
        ("顧客", "返品の送料はこちらが払うんですか。"),
        ("オペレーター", "不良品の場合は当社負担でございます。"),
    ])


def call_delivery(c, p, order):
    return "配送遅延", random.choice(["ネガティブ", "中立"]), talk([
        ("オペレーター", OPENING.format(agent="佐野")),
        ("顧客", f"{c['last_name']}です。セールで注文した{p['product_name']}がまだ届かないんです。注文番号は{order['order_id']}です。"),
        ("オペレーター", "お待たせして申し訳ございません。現在セール期間のご注文が集中しており、発送まで通常より2〜3日多くいただいております。"),
        ("顧客", random.choice(["週末に使いたかったので困ります。", "いつ届くか分からないと不安です。", "分かりました。いつごろになりますか。"])),
        ("オペレーター", "明日の発送を予定しております。発送後にメールで追跡番号をお送りします。"),
    ])


def call_return(c, p, order):
    return "返品・交換", "中立", talk([
        ("オペレーター", OPENING.format(agent="森")),
        ("顧客", f"{c['last_name']}と申します。{p['product_name']}を買ったんですが、思っていたサイズと違ったので返品できますか。"),
        ("オペレーター", "お客様都合の場合、商品到着後7日以内・未使用であれば返品を承っております。返送料はお客様負担となります。"),
        ("顧客", "まだ開けていないので大丈夫です。お願いします。"),
    ])


def call_general(c, p, order):
    kind, text = random.choice([
        ("商品問い合わせ", f"{p['product_name']}のサイズと素材を教えてください。"),
        ("注文キャンセル", f"注文番号{order['order_id']}をキャンセルしたいです。"),
        ("支払い", "コンビニ払いの期限を過ぎてしまったのですが、どうなりますか。"),
        ("会員・ポイント", "ゴールド会員の特典について教えてください。"),
    ])
    sentiment = random.choice(["中立", "中立", "ポジティブ"])
    return kind, sentiment, talk([
        ("オペレーター", OPENING.format(agent="石井")),
        ("顧客", f"{c['last_name']}です。{text}"),
        ("オペレーター", "確認いたします。少々お待ちください。"),
        ("オペレーター", "お待たせいたしました。ご案内いたします。"),
        ("顧客", "ありがとうございます。助かりました。"),
    ])


def build_calls(products, customers, orders):
    pmap = {p["product_id"]: p for p in products}
    cmap = {c["customer_id"]: c for c in customers}
    by_month = defaultdict(list)
    for o in orders:
        by_month[int(o["order_datetime"][5:7])].append(o)
    defect_orders = [o for o in orders if o["product_id"] == DEFECT_PRODUCT_ID and o["order_datetime"] >= "2026-09-01"]

    plan = []  # (month, kind)
    # 不具合: 10月以降 23件（うち繰り返しの顧客3名×2回目）
    plan += [(10, "defect")] * 9 + [(11, "defect")] * 14
    # 配送遅延: 11月が最多
    plan += [(6, "delivery")] * 1 + [(7, "delivery")] * 2 + [(8, "delivery")] * 1 + [(9, "delivery")] * 2 + [(10, "delivery")] * 2 + [(11, "delivery")] * 12
    # 返品（お客様都合）と一般
    for m in range(6, 12):
        plan += [(m, "return")] * 2 + [(m, "general")] * 5

    repeaters = random.sample(defect_orders, 3)
    calls = []
    for idx, (m, kind) in enumerate(plan):
        if kind == "defect":
            order = defect_orders[idx % len(defect_orders)]
        else:
            order = random.choice(by_month[m])
        c, p = cmap[order["customer_id"]], pmap[order["product_id"]]
        if kind == "defect":
            p = pmap[DEFECT_PRODUCT_ID]
            inq, sent, text = call_defect(c, p, order)
        elif kind == "delivery":
            inq, sent, text = call_delivery(c, p, order)
        elif kind == "return":
            inq, sent, text = call_return(c, p, order)
        else:
            inq, sent, text = call_general(c, p, order)
        dt = rand_datetime_in_month(m)
        if dt < datetime.strptime(order["order_datetime"], "%Y-%m-%d %H:%M:%S"):
            dt = datetime.strptime(order["order_datetime"], "%Y-%m-%d %H:%M:%S") + timedelta(days=random.randint(2, 10))
        calls.append({"customer_id": c["customer_id"], "product_id": p["product_id"], "order_id": order["order_id"],
                      "call_datetime": min(dt, END), "transcript_text": text,
                      "_inquiry": inq, "_sentiment": sent})
    # 繰り返し不満の顧客（解約リスクの正解）
    for order in repeaters:
        c, p = cmap[order["customer_id"]], pmap[DEFECT_PRODUCT_ID]
        _, _, text = call_defect(c, p, order, repeat=True)
        calls.append({"customer_id": c["customer_id"], "product_id": p["product_id"], "order_id": order["order_id"],
                      "call_datetime": datetime(2026, 11, random.randint(15, 28), random.randint(10, 18), random.randint(0, 59)),
                      "transcript_text": text, "_inquiry": "商品不具合", "_sentiment": "ネガティブ"})
    calls.sort(key=lambda x: x["call_datetime"])
    for i, c in enumerate(calls, start=1):
        c["call_id"] = f"CALL-{c['call_datetime']:%Y%m%d}-{i:04d}"
        c["agent_id"] = random.choice(AGENTS)
        c["call_duration_sec"] = random.randint(90, 600)
    return calls, [o["customer_id"] for o in repeaters]


def update_voice_metadata(customers):
    """既存 mp3 10件のメタデータを 2026年11月に寄せる（音声内容は変更しない）

    台本から生成した88件（audio_file が CALL-...mp3）は build_voice_metadata.py が作るので触らない
    """
    path = DATA / "voice_logs" / "voice_logs_metadata.json"
    meta = json.loads(path.read_text(encoding="utf-8"))
    originals = [m for m in meta if m["audio_file"].startswith("CALL_")]
    for i, m in enumerate(sorted(originals, key=lambda x: x["audio_file"])):
        old = datetime.fromisoformat(m["call_start_time"])
        new = old.replace(year=2026, month=11)
        dur = timedelta(seconds=m["call_duration_sec"])
        m["call_start_time"] = new.isoformat()
        m["call_end_time"] = (new + dur).isoformat(timespec="seconds")
        m["call_id"] = f"CALL-{new:%Y%m%d}-V{i + 1:03d}"
        if m.get("customer_id"):
            m["customer_id"] = customers[i * 7]["customer_id"]
    path.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return meta


def write_csv(path, rows, fields=None):
    fields = fields or [k for k in rows[0].keys() if not k.startswith("_")]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def main():
    products = build_products()
    customers = build_customers()
    orders = build_orders(products, customers)
    calls, repeaters = build_calls(products, customers, orders)
    update_voice_metadata(customers)

    write_csv(DATA / "products.csv", products)
    write_csv(DATA / "customers.csv", customers)
    write_csv(DATA / "orders.csv", orders)
    for c in calls:
        c["call_datetime"] = c["call_datetime"].strftime("%Y-%m-%d %H:%M:%S")
    write_csv(ROOT / "tools" / "call_scripts.csv", calls,
              ["call_id", "call_datetime", "customer_id", "order_id", "product_id", "agent_id", "call_duration_sec", "transcript_text"])
    # 正解ラベル（検証用。参加者には配布しない）
    write_csv(ROOT / "tools" / "expected_call_labels.csv", calls, ["call_id", "_inquiry", "_sentiment"])

    # ---- シグナルの確認 ----
    pmap = {p["product_id"]: p for p in products}

    def ret_rate(filter_fn):
        xs = [o for o in orders if filter_fn(o)]
        return sum(o["order_status"] == "returned" for o in xs) / max(len(xs), 1)

    light_before = ret_rate(lambda o: pmap[o["product_id"]]["category_l2"] == "照明" and o["order_datetime"] < "2026-10-01")
    light_after = ret_rate(lambda o: pmap[o["product_id"]]["category_l2"] == "照明" and o["order_datetime"] >= "2026-10-01")
    delivery_by_month = Counter(c["call_datetime"][:7] for c in calls if c["_inquiry"] == "配送遅延")
    defect_calls = sum(1 for c in calls if c["_inquiry"] == "商品不具合" and c["call_datetime"] >= "2026-10-01")

    print(f"商品 {len(products)} / 顧客 {len(customers)} / 注文 {len(orders)} / 通話 {len(calls)}")
    print(f"照明の返品率 6-9月 {light_before:.1%} → 10-11月 {light_after:.1%}")
    print(f"配送遅延の通話 月別 {dict(sorted(delivery_by_month.items()))}")
    print(f"10月以降の不具合通話 {defect_calls} 件（繰り返し顧客 {repeaters}）")

    assert light_after >= light_before * 2, "照明の返品率が2倍以上になっていない"
    assert delivery_by_month.most_common(1)[0][0] == "2026-11", "配送遅延の最多月が11月でない"
    assert all(START <= datetime.strptime(o["order_datetime"], "%Y-%m-%d %H:%M:%S") <= END for o in orders)
    print("【完了】シグナルの確認に成功しました")


if __name__ == "__main__":
    main()
