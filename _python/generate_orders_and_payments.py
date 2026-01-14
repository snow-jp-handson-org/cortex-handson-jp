"""
EC取引データ（fact_orders）とクレジット決済情報（fact_payments）のサンプルデータ生成スクリプト
既存のcustomers.csvとproducts.csvのデータと整合性を保ちます
"""

import random
from datetime import datetime, timedelta
import csv
import hashlib

# パラメータ設定
NUM_ORDERS = 500  # 注文数

# キャンペーンID
CAMPAIGN_IDS = [
    'CMP-2024-WINTER01',
    'CMP-2024-SPRING01',
    'CMP-2024-SUMMER01',
    'CMP-2024-AUTUMN01',
    None  # キャンペーンなし
]

# 支払方法
PAYMENT_METHODS = {
    'credit_card': 0.70,
    'cod': 0.20,
    'convenience': 0.10,
}

# 注文チャネル
ORDER_CHANNELS = {
    'web': 0.60,
    'app': 0.30,
    'store': 0.10,
}

# 注文ステータス
ORDER_STATUSES = {
    'completed': 0.90,
    'cancelled': 0.07,
    'returned': 0.03,
}

# クレジットカードブランド
CARD_BRANDS = ['visa', 'mastercard', 'jcb', 'amex']

# 決済ステータス
PAYMENT_STATUSES = {
    'approved': 0.95,
    'declined': 0.03,
    'refunded': 0.02,
}

def load_customers(filename='customers.csv'):
    """顧客データの読み込み"""
    customers = []
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            customers.append(row)
    return customers

def load_products(filename='products.csv'):
    """商品データの読み込み"""
    products = []
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # 在庫があり、activeな商品のみを対象
            if row['product_status'] == 'active' and int(row['stock_quantity']) > 0:
                products.append(row)
    return products

def generate_order_id(order_number):
    """注文IDの生成"""
    return f"ORD-2024-{order_number:08d}"

def generate_payment_id(order_id):
    """決済IDの生成（注文IDベース）"""
    order_num = order_id.split('-')[-1]
    payment_num = int(order_num) + 80000000  # オフセットを追加
    return f"PAY-2024-{payment_num}"

def generate_shipping_address_id(customer_id):
    """配送先IDの生成"""
    customer_num = customer_id.split('-')[-1]
    address_variant = random.randint(1, 5)
    return f"ADDR-{customer_num}-{address_variant:02d}"

def generate_authorization_code():
    """オーソリコードの生成"""
    return f"AUTH{random.randint(100000, 999999)}"

def generate_device_fingerprint():
    """デバイスフィンガープリントの生成"""
    random_str = f"{random.random()}{datetime.now().isoformat()}"
    hash_obj = hashlib.md5(random_str.encode())
    return f"FP-{hash_obj.hexdigest()[:12]}"

def generate_ip_address():
    """IPアドレスの生成"""
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"

def calculate_amounts(unit_price, quantity, has_campaign):
    """金額の計算"""
    subtotal = unit_price * quantity
    
    # 割引額（キャンペーンがある場合）
    if has_campaign:
        discount_rate = random.uniform(0.05, 0.20)
        discount_amount = round(subtotal * discount_rate, 2)
    else:
        discount_amount = 0.00
    
    # 消費税額（10%）
    tax_amount = round((subtotal - discount_amount) * 0.10, 2)
    
    # 合計金額
    total_amount = round(subtotal - discount_amount + tax_amount, 2)
    
    return discount_amount, tax_amount, total_amount

def generate_orders_and_payments(num_orders, customers, products):
    """注文データと決済データの生成"""
    orders = []
    payments = []
    
    # 2024年のデータを生成
    start_date = datetime(2024, 1, 1, 0, 0, 0)
    end_date = datetime(2024, 12, 31, 23, 59, 59)
    
    for i in range(1, num_orders + 1):
        # 注文日時をランダムに生成
        days_between = (end_date - start_date).days
        random_days = random.randint(0, days_between)
        random_seconds = random.randint(0, 86399)
        order_datetime = start_date + timedelta(days=random_days, seconds=random_seconds)
        
        # 顧客を選択
        customer = random.choice(customers)
        customer_id = customer['customer_id']
        
        # 商品を選択
        product = random.choice(products)
        product_id = product['product_id']
        
        # 購入数量（1-10個）
        quantity = random.randint(1, 10)
        
        # 単価（商品の現在価格を使用）
        unit_price = float(product['current_price'])
        
        # キャンペーン
        campaign_id = random.choice(CAMPAIGN_IDS)
        has_campaign = campaign_id is not None
        
        # 金額計算
        discount_amount, tax_amount, total_amount = calculate_amounts(
            unit_price, quantity, has_campaign
        )
        
        # 支払方法
        payment_method = random.choices(
            list(PAYMENT_METHODS.keys()),
            weights=list(PAYMENT_METHODS.values())
        )[0]
        
        # 配送先ID
        shipping_address_id = generate_shipping_address_id(customer_id)
        
        # 注文チャネル
        order_channel = random.choices(
            list(ORDER_CHANNELS.keys()),
            weights=list(ORDER_CHANNELS.values())
        )[0]
        
        # 注文ステータス
        order_status = random.choices(
            list(ORDER_STATUSES.keys()),
            weights=list(ORDER_STATUSES.values())
        )[0]
        
        # 注文ID
        order_id = generate_order_id(i)
        
        # 注文データ
        order = {
            'order_id': order_id,
            'order_datetime': order_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            'customer_id': customer_id,
            'product_id': product_id,
            'quantity': quantity,
            'unit_price': unit_price,
            'discount_amount': discount_amount,
            'tax_amount': tax_amount,
            'total_amount': total_amount,
            'payment_method': payment_method,
            'shipping_address_id': shipping_address_id,
            'order_channel': order_channel,
            'campaign_id': campaign_id if campaign_id else '',
            'order_status': order_status
        }
        orders.append(order)
        
        # クレジットカード決済の場合は決済データを生成
        if payment_method == 'credit_card':
            # 決済日時（注文から数秒後）
            payment_datetime = order_datetime + timedelta(seconds=random.randint(1, 60))
            
            # カードブランド
            card_brand = random.choice(CARD_BRANDS)
            
            # カード番号下4桁
            card_last4 = f"{random.randint(0, 9999):04d}"
            
            # 決済ステータス（注文ステータスと整合性を保つ）
            if order_status == 'completed':
                payment_status = random.choices(
                    ['approved', 'approved', 'approved', 'approved', 'approved'],
                    weights=[1, 1, 1, 1, 1]
                )[0]
            elif order_status == 'cancelled':
                payment_status = random.choice(['declined', 'approved'])
            elif order_status == 'returned':
                payment_status = 'refunded'
            else:
                payment_status = 'approved'
            
            # 不正スコア（0-100、低いほど正常）
            if payment_status == 'declined':
                fraud_score = round(random.uniform(60.0, 95.0), 2)
            elif payment_status == 'approved':
                fraud_score = round(random.uniform(0.0, 30.0), 2)
            else:  # refunded
                fraud_score = round(random.uniform(10.0, 40.0), 2)
            
            # デバイスフィンガープリント
            device_fingerprint = generate_device_fingerprint()
            
            # IPアドレス
            ip_address = generate_ip_address()
            
            # 請求国（ほとんどが日本）
            billing_country = 'JP' if random.random() < 0.95 else random.choice(['US', 'CN', 'KR', 'TW'])
            
            # オーソリコード（承認された場合のみ）
            authorization_code = generate_authorization_code() if payment_status == 'approved' else ''
            
            # 決済ID
            payment_id = generate_payment_id(order_id)
            
            # 決済データ
            payment = {
                'payment_id': payment_id,
                'order_id': order_id,
                'payment_datetime': payment_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                'card_brand': card_brand,
                'card_last4': card_last4,
                'payment_amount': total_amount,
                'authorization_code': authorization_code,
                'payment_status': payment_status,
                'fraud_score': fraud_score,
                'device_fingerprint': device_fingerprint,
                'ip_address': ip_address,
                'billing_country': billing_country
            }
            payments.append(payment)
        
        # 進捗表示
        if i % 100 == 0:
            print(f"  処理中... {i}/{num_orders} 件完了")
    
    # 注文データをタイムスタンプ順にソート
    orders.sort(key=lambda x: x['order_datetime'])
    
    return orders, payments

def save_orders_to_csv(orders, filename='orders.csv'):
    """注文データをCSVファイルに保存"""
    fieldnames = [
        'order_id', 'order_datetime', 'customer_id', 'product_id', 'quantity',
        'unit_price', 'discount_amount', 'tax_amount', 'total_amount',
        'payment_method', 'shipping_address_id', 'order_channel', 'campaign_id',
        'order_status'
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(orders)
    
    print(f"✅ {len(orders)}件の注文データを {filename} に保存しました")

def save_payments_to_csv(payments, filename='payments.csv'):
    """決済データをCSVファイルに保存"""
    fieldnames = [
        'payment_id', 'order_id', 'payment_datetime', 'card_brand', 'card_last4',
        'payment_amount', 'authorization_code', 'payment_status', 'fraud_score',
        'device_fingerprint', 'ip_address', 'billing_country'
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(payments)
    
    print(f"✅ {len(payments)}件の決済データを {filename} に保存しました")

def generate_orders_sql(orders, filename='insert_orders.sql'):
    """注文データのSQL INSERT文を生成"""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("-- EC取引データテーブルの作成\n")
        f.write("CREATE OR REPLACE TABLE fact_orders (\n")
        f.write("    order_id VARCHAR PRIMARY KEY,\n")
        f.write("    order_datetime TIMESTAMP,\n")
        f.write("    customer_id VARCHAR,\n")
        f.write("    product_id VARCHAR,\n")
        f.write("    quantity INTEGER,\n")
        f.write("    unit_price DECIMAL(10,2),\n")
        f.write("    discount_amount DECIMAL(10,2),\n")
        f.write("    tax_amount DECIMAL(10,2),\n")
        f.write("    total_amount DECIMAL(10,2),\n")
        f.write("    payment_method VARCHAR,\n")
        f.write("    shipping_address_id VARCHAR,\n")
        f.write("    order_channel VARCHAR,\n")
        f.write("    campaign_id VARCHAR,\n")
        f.write("    order_status VARCHAR\n")
        f.write(");\n\n")
        
        f.write("-- サンプルデータの挿入\n")
        f.write("INSERT INTO fact_orders VALUES\n")
        
        for i, order in enumerate(orders):
            campaign_id = f"'{order['campaign_id']}'" if order['campaign_id'] else 'NULL'
            
            line = (
                f"('{order['order_id']}', '{order['order_datetime']}', '{order['customer_id']}', "
                f"'{order['product_id']}', {order['quantity']}, {order['unit_price']}, "
                f"{order['discount_amount']}, {order['tax_amount']}, {order['total_amount']}, "
                f"'{order['payment_method']}', '{order['shipping_address_id']}', "
                f"'{order['order_channel']}', {campaign_id}, '{order['order_status']}')"
            )
            
            if i < len(orders) - 1:
                f.write(line + ",\n")
            else:
                f.write(line + ";\n")
        
        f.write("\n-- データ件数の確認\n")
        f.write("SELECT COUNT(*) AS total_orders FROM fact_orders;\n\n")
        f.write("-- 支払方法別の集計\n")
        f.write("SELECT payment_method, COUNT(*) AS order_count, SUM(total_amount) AS total_revenue ")
        f.write("FROM fact_orders GROUP BY payment_method ORDER BY total_revenue DESC;\n\n")
        f.write("-- 注文ステータス別の集計\n")
        f.write("SELECT order_status, COUNT(*) AS order_count FROM fact_orders GROUP BY order_status;\n\n")
        f.write("-- サンプルデータの確認\n")
        f.write("SELECT * FROM fact_orders LIMIT 10;\n")
    
    print(f"✅ SQL INSERT文を {filename} に保存しました")

def generate_payments_sql(payments, filename='insert_payments.sql'):
    """決済データのSQL INSERT文を生成"""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("-- クレジット決済情報テーブルの作成\n")
        f.write("CREATE OR REPLACE TABLE fact_payments (\n")
        f.write("    payment_id VARCHAR PRIMARY KEY,\n")
        f.write("    order_id VARCHAR,\n")
        f.write("    payment_datetime TIMESTAMP,\n")
        f.write("    card_brand VARCHAR,\n")
        f.write("    card_last4 VARCHAR,\n")
        f.write("    payment_amount DECIMAL(10,2),\n")
        f.write("    authorization_code VARCHAR,\n")
        f.write("    payment_status VARCHAR,\n")
        f.write("    fraud_score DECIMAL(5,2),\n")
        f.write("    device_fingerprint VARCHAR,\n")
        f.write("    ip_address VARCHAR,\n")
        f.write("    billing_country VARCHAR\n")
        f.write(");\n\n")
        
        f.write("-- サンプルデータの挿入\n")
        f.write("INSERT INTO fact_payments VALUES\n")
        
        for i, payment in enumerate(payments):
            auth_code = f"'{payment['authorization_code']}'" if payment['authorization_code'] else 'NULL'
            
            line = (
                f"('{payment['payment_id']}', '{payment['order_id']}', '{payment['payment_datetime']}', "
                f"'{payment['card_brand']}', '{payment['card_last4']}', {payment['payment_amount']}, "
                f"{auth_code}, '{payment['payment_status']}', {payment['fraud_score']}, "
                f"'{payment['device_fingerprint']}', '{payment['ip_address']}', '{payment['billing_country']}')"
            )
            
            if i < len(payments) - 1:
                f.write(line + ",\n")
            else:
                f.write(line + ";\n")
        
        f.write("\n-- データ件数の確認\n")
        f.write("SELECT COUNT(*) AS total_payments FROM fact_payments;\n\n")
        f.write("-- 決済ステータス別の集計\n")
        f.write("SELECT payment_status, COUNT(*) AS payment_count, SUM(payment_amount) AS total_amount ")
        f.write("FROM fact_payments GROUP BY payment_status ORDER BY payment_count DESC;\n\n")
        f.write("-- カードブランド別の集計\n")
        f.write("SELECT card_brand, COUNT(*) AS payment_count FROM fact_payments GROUP BY card_brand ORDER BY payment_count DESC;\n\n")
        f.write("-- サンプルデータの確認\n")
        f.write("SELECT * FROM fact_payments LIMIT 10;\n")
    
    print(f"✅ SQL INSERT文を {filename} に保存しました")

if __name__ == '__main__':
    print(f"🚀 注文データと決済データを生成中...")
    print(f"   既存のcustomers.csvとproducts.csvを読み込みます\n")
    
    # 既存データの読み込み
    customers = load_customers('customers.csv')
    products = load_products('products.csv')
    
    print(f"📊 読み込んだデータ:")
    print(f"  顧客数: {len(customers)}人")
    print(f"  商品数: {len(products)}商品（在庫あり・activeのみ）\n")
    
    # 注文データと決済データの生成
    orders, payments = generate_orders_and_payments(NUM_ORDERS, customers, products)
    
    # CSVファイルに保存
    save_orders_to_csv(orders, 'orders.csv')
    save_payments_to_csv(payments, 'payments.csv')
    
    # SQL INSERT文を生成
    generate_orders_sql(orders, 'insert_orders.sql')
    generate_payments_sql(payments, 'insert_payments.sql')
    
    print(f"\n📊 生成されたデータ統計:")
    print(f"  総注文数: {len(orders):,}件")
    print(f"  総決済数: {len(payments):,}件（クレジットカード決済のみ）")
    
    # 支払方法別の集計
    payment_method_counts = {}
    for order in orders:
        pm = order['payment_method']
        payment_method_counts[pm] = payment_method_counts.get(pm, 0) + 1
    
    print("\n💳 支払方法別:")
    for method, count in sorted(payment_method_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(orders)) * 100
        print(f"  {method}: {count:,}件 ({percentage:.1f}%)")
    
    # 注文ステータス別の集計
    status_counts = {}
    for order in orders:
        status = order['order_status']
        status_counts[status] = status_counts.get(status, 0) + 1
    
    print("\n📦 注文ステータス別:")
    for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(orders)) * 100
        print(f"  {status}: {count:,}件 ({percentage:.1f}%)")
    
    # 決済ステータス別の集計
    if payments:
        payment_status_counts = {}
        for payment in payments:
            ps = payment['payment_status']
            payment_status_counts[ps] = payment_status_counts.get(ps, 0) + 1
        
        print("\n✅ 決済ステータス別:")
        for status, count in sorted(payment_status_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(payments)) * 100
            print(f"  {status}: {count:,}件 ({percentage:.1f}%)")
    
    # 総売上金額
    total_revenue = sum(order['total_amount'] for order in orders if order['order_status'] == 'completed')
    print(f"\n💰 総売上金額（完了注文のみ）: ¥{total_revenue:,.2f}")
    
    print("\n📦 サンプル注文（最初の3件）:")
    for i, order in enumerate(orders[:3]):
        print(f"\n注文 {i+1}:")
        print(f"  注文ID: {order['order_id']}")
        print(f"  注文日時: {order['order_datetime']}")
        print(f"  顧客ID: {order['customer_id']}")
        print(f"  商品ID: {order['product_id']}")
        print(f"  数量: {order['quantity']}個")
        print(f"  単価: ¥{order['unit_price']:,.2f}")
        print(f"  割引: ¥{order['discount_amount']:,.2f}")
        print(f"  消費税: ¥{order['tax_amount']:,.2f}")
        print(f"  合計: ¥{order['total_amount']:,.2f}")
        print(f"  支払方法: {order['payment_method']}")
        print(f"  注文チャネル: {order['order_channel']}")
        print(f"  ステータス: {order['order_status']}")
        
        # 対応する決済情報があれば表示
        payment = next((p for p in payments if p['order_id'] == order['order_id']), None)
        if payment:
            print(f"  [決済情報]")
            print(f"  決済ID: {payment['payment_id']}")
            print(f"  カードブランド: {payment['card_brand']}")
            print(f"  カード下4桁: {payment['card_last4']}")
            print(f"  決済ステータス: {payment['payment_status']}")
            print(f"  不正スコア: {payment['fraud_score']}")
    
    print("\n✨ 完了！")

