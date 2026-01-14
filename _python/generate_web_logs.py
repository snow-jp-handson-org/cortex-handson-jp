"""
ECアクセスログ（fact_web_logs）のサンプルデータ生成スクリプト
"""

import random
from datetime import datetime, timedelta
import csv
import json
import hashlib

# パラメータ設定
NUM_SESSIONS = 2000  # セッション数（各セッションで複数のイベントが発生）
EVENTS_PER_SESSION_MIN = 1
EVENTS_PER_SESSION_MAX = 15

# 既存データの参照（実際のデータと整合性を保つ）
CUSTOMER_IDS = [f"CUS-{i:08d}" for i in range(1, 101)]  # 100人の顧客
PRODUCT_CATEGORIES = {
    'INT': list(range(1, 145)),  # インテリア商品
    'ELT': list(range(1, 145)),  # 家電
    'FSH': list(range(1, 145)),  # ファッション
    'BKS': list(range(1, 145)),  # 書籍
}

# イベント種別の定義
EVENT_TYPES = {
    'page_view': 0.50,      # 50% - ページ閲覧
    'click': 0.25,          # 25% - クリック
    'add_to_cart': 0.15,    # 15% - カート追加
    'purchase': 0.10,       # 10% - 購入
}

# ページカテゴリ
PAGE_CATEGORIES = {
    'home': {'url': '/', 'has_product': False},
    'category': {'url': '/category/', 'has_product': False},
    'product_detail': {'url': '/products/', 'has_product': True},
    'cart': {'url': '/cart', 'has_product': False},
    'checkout': {'url': '/checkout', 'has_product': False},
    'search': {'url': '/search', 'has_product': False},
    'account': {'url': '/account', 'has_product': False},
}

# 流入元（UTM parameters）
UTM_SOURCES = ['google', 'yahoo', 'instagram', 'facebook', 'twitter', 'email', 'direct', 'line']
UTM_MEDIUMS = {
    'google': ['cpc', 'organic', 'display'],
    'yahoo': ['cpc', 'organic'],
    'instagram': ['social', 'paid'],
    'facebook': ['social', 'paid'],
    'twitter': ['social', 'paid'],
    'email': ['newsletter', 'campaign'],
    'direct': ['none'],
    'line': ['social', 'messaging'],
}

# キャンペーン名
UTM_CAMPAIGNS = [
    'winter_sale_2024', 'spring_sale_2024', 'summer_sale_2024', 'autumn_sale_2024',
    'new_year_campaign', 'valentine_special', 'golden_week_sale', 'black_friday',
    'cyber_monday', 'birthday_campaign', 'member_special', 'clearance_sale',
    None, None, None  # キャンペーンなしも含む
]

# デバイス種別
DEVICE_TYPES = {
    'mobile': 0.60,    # 60% - モバイル
    'desktop': 0.30,   # 30% - デスクトップ
    'tablet': 0.10,    # 10% - タブレット
}

# ブラウザ（デバイスごとに異なる）
BROWSERS = {
    'mobile': ['chrome', 'safari', 'samsung_internet', 'firefox'],
    'desktop': ['chrome', 'edge', 'firefox', 'safari'],
    'tablet': ['safari', 'chrome', 'samsung_internet'],
}

# OS（デバイスごとに異なる）
OS_TYPES = {
    'mobile': ['ios', 'android'],
    'desktop': ['windows', 'macos', 'linux'],
    'tablet': ['ios', 'android'],
}

# リファラーURL
REFERRER_URLS = [
    'https://www.google.com/',
    'https://www.google.co.jp/search',
    'https://search.yahoo.co.jp/',
    'https://www.instagram.com/',
    'https://www.facebook.com/',
    'https://twitter.com/',
    'https://line.me/',
    None,  # 直接アクセス
]

def generate_session_id():
    """セッションIDの生成"""
    random_str = f"{random.random()}{datetime.now().isoformat()}"
    hash_obj = hashlib.md5(random_str.encode())
    return f"SESS-{hash_obj.hexdigest()[:16]}"

def generate_log_id(timestamp):
    """ログIDの生成（LOG-YYYYMMDD-nnnnnnnnnn形式）"""
    date_str = timestamp.strftime('%Y%m%d')
    random_num = random.randint(100000000, 999999999)
    return f"LOG-{date_str}-{random_num}"

def get_random_product_id():
    """ランダムな商品IDを取得"""
    category = random.choice(list(PRODUCT_CATEGORIES.keys()))
    product_num = random.choice(PRODUCT_CATEGORIES[category])
    return f"PRD-{category}-{product_num:05d}"

def generate_page_url(page_category, product_id=None):
    """ページURLの生成"""
    base_url = PAGE_CATEGORIES[page_category]['url']
    
    if page_category == 'product_detail' and product_id:
        return f"{base_url}{product_id}"
    elif page_category == 'category':
        categories = ['インテリア', '家電', 'ファッション', '書籍']
        return f"{base_url}{random.choice(categories)}"
    elif page_category == 'search':
        search_terms = ['デスクライト', '掃除機', 'シャツ', 'ビジネス書', 'LED', 'ワイヤレス']
        return f"{base_url}?q={random.choice(search_terms)}"
    else:
        return base_url

def calculate_time_on_page(page_category, event_type):
    """ページ滞在時間の計算（秒）"""
    base_times = {
        'home': (10, 60),
        'category': (20, 120),
        'product_detail': (30, 300),
        'cart': (15, 90),
        'checkout': (30, 180),
        'search': (10, 60),
        'account': (20, 120),
    }
    
    min_time, max_time = base_times.get(page_category, (5, 60))
    
    # イベント種別によって調整
    if event_type == 'purchase':
        min_time = max(min_time, 30)
    elif event_type == 'click':
        max_time = min(max_time, 30)
    
    return random.randint(min_time, max_time)

def generate_session_events(session_start_time, is_logged_in, customer_id=None):
    """1セッション内のイベント群を生成"""
    events = []
    current_time = session_start_time
    session_id = generate_session_id()
    
    # セッション内のイベント数
    num_events = random.randint(EVENTS_PER_SESSION_MIN, EVENTS_PER_SESSION_MAX)
    
    # デバイス・ブラウザ・OSはセッション内で一貫
    device_type = random.choices(
        list(DEVICE_TYPES.keys()),
        weights=list(DEVICE_TYPES.values())
    )[0]
    browser = random.choice(BROWSERS[device_type])
    os = random.choice(OS_TYPES[device_type])
    
    # 流入元情報（セッション開始時に決定）
    utm_source = random.choice(UTM_SOURCES)
    utm_medium = random.choice(UTM_MEDIUMS[utm_source])
    utm_campaign = random.choice(UTM_CAMPAIGNS)
    referrer_url = random.choice(REFERRER_URLS) if utm_source != 'direct' else None
    
    # セッションの流れをシミュレート
    session_flow = []
    
    # 最初は必ずホームページまたはカテゴリページ
    if random.random() < 0.6:
        session_flow.append('home')
    else:
        session_flow.append('category')
    
    # セッションの残りをランダムに生成（ただし、現実的な遷移を考慮）
    for _ in range(num_events - 1):
        last_page = session_flow[-1]
        
        if last_page == 'home':
            next_page = random.choices(
                ['category', 'product_detail', 'search', 'account'],
                weights=[0.4, 0.3, 0.2, 0.1]
            )[0]
        elif last_page == 'category':
            next_page = random.choices(
                ['product_detail', 'category', 'search', 'home'],
                weights=[0.5, 0.2, 0.2, 0.1]
            )[0]
        elif last_page == 'product_detail':
            next_page = random.choices(
                ['product_detail', 'cart', 'category', 'home'],
                weights=[0.3, 0.3, 0.2, 0.2]
            )[0]
        elif last_page == 'cart':
            next_page = random.choices(
                ['checkout', 'product_detail', 'category', 'cart'],
                weights=[0.4, 0.3, 0.2, 0.1]
            )[0]
        elif last_page == 'checkout':
            # チェックアウト後は終了または完了ページ
            if random.random() < 0.7:
                break
            next_page = 'checkout'
        elif last_page == 'search':
            next_page = random.choices(
                ['product_detail', 'category', 'search'],
                weights=[0.5, 0.3, 0.2]
            )[0]
        else:  # account
            next_page = random.choices(
                ['home', 'category', 'product_detail'],
                weights=[0.4, 0.3, 0.3]
            )[0]
        
        session_flow.append(next_page)
    
    # イベントを生成
    for page_category in session_flow:
        # イベント種別を決定
        if page_category == 'checkout' and random.random() < 0.7:
            event_type = 'purchase'
        elif page_category == 'product_detail' and random.random() < 0.3:
            event_type = 'add_to_cart'
        elif random.random() < 0.2:
            event_type = 'click'
        else:
            event_type = 'page_view'
        
        # 商品IDの決定（商品関連ページの場合）
        product_id = None
        if PAGE_CATEGORIES[page_category]['has_product']:
            product_id = get_random_product_id()
        
        # ページURLの生成
        page_url = generate_page_url(page_category, product_id)
        
        # 滞在時間
        time_on_page = calculate_time_on_page(page_category, event_type)
        
        # ログIDの生成
        log_id = generate_log_id(current_time)
        
        event = {
            'log_id': log_id,
            'session_id': session_id,
            'customer_id': customer_id if is_logged_in else None,
            'event_timestamp': current_time.strftime('%Y-%m-%d %H:%M:%S'),
            'event_type': event_type,
            'page_url': page_url,
            'page_category': page_category,
            'referrer_url': referrer_url,
            'utm_source': utm_source,
            'utm_medium': utm_medium,
            'utm_campaign': utm_campaign,
            'device_type': device_type,
            'browser': browser,
            'os': os,
            'time_on_page': time_on_page,
            'product_id': product_id if product_id else None
        }
        
        events.append(event)
        
        # 次のイベントまでの時間を進める
        current_time += timedelta(seconds=time_on_page + random.randint(1, 10))
    
    return events

def generate_web_logs(num_sessions):
    """Webアクセスログの生成"""
    all_events = []
    
    # 2024年のデータを生成
    start_date = datetime(2024, 1, 1, 0, 0, 0)
    end_date = datetime(2024, 12, 31, 23, 59, 59)
    
    for i in range(num_sessions):
        # セッション開始時刻をランダムに生成
        days_between = (end_date - start_date).days
        random_days = random.randint(0, days_between)
        random_seconds = random.randint(0, 86399)  # 1日の秒数
        session_start = start_date + timedelta(days=random_days, seconds=random_seconds)
        
        # ログイン状態の決定（40%の確率でログイン）
        is_logged_in = random.random() < 0.40
        customer_id = random.choice(CUSTOMER_IDS) if is_logged_in else None
        
        # セッションのイベントを生成
        session_events = generate_session_events(session_start, is_logged_in, customer_id)
        all_events.extend(session_events)
        
        # 進捗表示
        if (i + 1) % 200 == 0:
            print(f"  処理中... {i + 1}/{num_sessions} セッション完了")
    
    # イベントをタイムスタンプ順にソート
    all_events.sort(key=lambda x: x['event_timestamp'])
    
    return all_events

def save_to_csv(events, filename='web_logs.csv'):
    """CSVファイルへの保存"""
    fieldnames = [
        'log_id', 'session_id', 'customer_id', 'event_timestamp', 'event_type',
        'page_url', 'page_category', 'referrer_url', 'utm_source', 'utm_medium',
        'utm_campaign', 'device_type', 'browser', 'os', 'time_on_page', 'product_id'
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)
    
    print(f"✅ {len(events)}件のアクセスログを {filename} に保存しました")

def save_to_json(events, filename='web_logs.json'):
    """JSON配列形式でファイルに保存"""
    # Noneを空文字列に変換
    cleaned_events = []
    for event in events:
        cleaned_event = {}
        for key, value in event.items():
            cleaned_event[key] = value if value is not None else ""
        cleaned_events.append(cleaned_event)
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(cleaned_events, f, ensure_ascii=False, indent=2)
    
    print(f"✅ {len(events)}件のアクセスログを {filename} に保存しました（JSON配列形式）")

def save_to_jsonlines(events, filename='web_logs.jsonl'):
    """JSONLines形式でファイルに保存（1行1JSON）"""
    with open(filename, 'w', encoding='utf-8') as f:
        for event in events:
            # Noneを空文字列に変換
            cleaned_event = {}
            for key, value in event.items():
                cleaned_event[key] = value if value is not None else ""
            f.write(json.dumps(cleaned_event, ensure_ascii=False) + '\n')
    
    print(f"✅ {len(events)}件のアクセスログを {filename} に保存しました（JSONLines形式）")

def generate_sql_insert(events, filename='insert_web_logs.sql', batch_size=1000):
    """SQL INSERT文の生成（大量データのためバッチ処理）"""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("-- ECアクセスログテーブルの作成\n")
        f.write("CREATE OR REPLACE TABLE fact_web_logs (\n")
        f.write("    log_id VARCHAR PRIMARY KEY,\n")
        f.write("    session_id VARCHAR,\n")
        f.write("    customer_id VARCHAR,\n")
        f.write("    event_timestamp TIMESTAMP,\n")
        f.write("    event_type VARCHAR,\n")
        f.write("    page_url VARCHAR,\n")
        f.write("    page_category VARCHAR,\n")
        f.write("    referrer_url VARCHAR,\n")
        f.write("    utm_source VARCHAR,\n")
        f.write("    utm_medium VARCHAR,\n")
        f.write("    utm_campaign VARCHAR,\n")
        f.write("    device_type VARCHAR,\n")
        f.write("    browser VARCHAR,\n")
        f.write("    os VARCHAR,\n")
        f.write("    time_on_page INTEGER,\n")
        f.write("    product_id VARCHAR\n")
        f.write(");\n\n")
        
        # バッチごとにINSERT文を生成
        total_batches = (len(events) + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(events))
            batch_events = events[start_idx:end_idx]
            
            f.write(f"-- バッチ {batch_num + 1}/{total_batches} ({start_idx + 1}〜{end_idx}件目)\n")
            f.write("INSERT INTO fact_web_logs VALUES\n")
            
            for i, event in enumerate(batch_events):
                customer_id = f"'{event['customer_id']}'" if event['customer_id'] else 'NULL'
                referrer_url = f"'{event['referrer_url']}'" if event['referrer_url'] else 'NULL'
                utm_campaign = f"'{event['utm_campaign']}'" if event['utm_campaign'] else 'NULL'
                product_id = f"'{event['product_id']}'" if event['product_id'] else 'NULL'
                
                # SQLインジェクション対策：シングルクォートをエスケープ
                page_url = event['page_url'].replace("'", "''")
                
                line = (
                    f"('{event['log_id']}', '{event['session_id']}', {customer_id}, "
                    f"'{event['event_timestamp']}', '{event['event_type']}', '{page_url}', "
                    f"'{event['page_category']}', {referrer_url}, '{event['utm_source']}', "
                    f"'{event['utm_medium']}', {utm_campaign}, '{event['device_type']}', "
                    f"'{event['browser']}', '{event['os']}', {event['time_on_page']}, {product_id})"
                )
                
                if i < len(batch_events) - 1:
                    f.write(line + ",\n")
                else:
                    f.write(line + ";\n\n")
        
        f.write("-- データ件数の確認\n")
        f.write("SELECT COUNT(*) AS total_logs FROM fact_web_logs;\n\n")
        f.write("-- イベント種別ごとの集計\n")
        f.write("SELECT event_type, COUNT(*) AS event_count FROM fact_web_logs GROUP BY event_type ORDER BY event_count DESC;\n\n")
        f.write("-- デバイス種別ごとの集計\n")
        f.write("SELECT device_type, COUNT(*) AS log_count FROM fact_web_logs GROUP BY device_type ORDER BY log_count DESC;\n\n")
        f.write("-- サンプルデータの確認\n")
        f.write("SELECT * FROM fact_web_logs LIMIT 10;\n")
    
    print(f"✅ SQL INSERT文を {filename} に保存しました")

if __name__ == '__main__':
    print(f"🚀 {NUM_SESSIONS}セッションのWebアクセスログを生成中...")
    print(f"   各セッションで {EVENTS_PER_SESSION_MIN}〜{EVENTS_PER_SESSION_MAX} イベントが発生します")
    print()
    
    events = generate_web_logs(NUM_SESSIONS)
    
    # CSVファイルに保存
    save_to_csv(events, 'web_logs.csv')
    
    # JSON配列形式で保存
    save_to_json(events, 'web_logs.json')
    
    # JSONLines形式で保存
    save_to_jsonlines(events, 'web_logs.jsonl')
    
    # SQL INSERT文を生成
    generate_sql_insert(events, 'insert_web_logs.sql')
    
    print(f"\n📊 生成されたアクセスログ統計:")
    print(f"  総イベント数: {len(events):,}件")
    print(f"  総セッション数: {NUM_SESSIONS:,}セッション")
    print(f"  平均イベント数/セッション: {len(events)/NUM_SESSIONS:.1f}件")
    
    # イベント種別ごとの集計
    event_type_counts = {}
    for event in events:
        et = event['event_type']
        event_type_counts[et] = event_type_counts.get(et, 0) + 1
    
    print("\n📈 イベント種別:")
    for event_type, count in sorted(event_type_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(events)) * 100
        print(f"  {event_type}: {count:,}件 ({percentage:.1f}%)")
    
    # デバイス種別ごとの集計
    device_counts = {}
    for event in events:
        device = event['device_type']
        device_counts[device] = device_counts.get(device, 0) + 1
    
    print("\n📱 デバイス種別:")
    for device, count in sorted(device_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(events)) * 100
        print(f"  {device}: {count:,}件 ({percentage:.1f}%)")
    
    # ログインユーザーの割合
    logged_in_count = sum(1 for e in events if e['customer_id'])
    print(f"\n👤 ログイン状態:")
    print(f"  ログイン済み: {logged_in_count:,}件 ({(logged_in_count/len(events))*100:.1f}%)")
    print(f"  未ログイン: {len(events)-logged_in_count:,}件 ({((len(events)-logged_in_count)/len(events))*100:.1f}%)")
    
    print("\n💡 サンプルセッション（最初の5イベント）:")
    for i, event in enumerate(events[:5]):
        print(f"\n{i+1}. {event['event_timestamp']}")
        print(f"   セッション: {event['session_id']}")
        print(f"   顧客ID: {event['customer_id'] or '未ログイン'}")
        print(f"   イベント: {event['event_type']}")
        print(f"   ページ: {event['page_category']} ({event['page_url']})")
        print(f"   デバイス: {event['device_type']} / {event['browser']} / {event['os']}")
        print(f"   流入: {event['utm_source']} / {event['utm_medium']}")
        if event['product_id']:
            print(f"   商品ID: {event['product_id']}")
    
    print("\n✨ 完了！")

