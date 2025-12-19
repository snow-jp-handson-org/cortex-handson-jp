"""
商品マスタ（dim_products）のサンプルデータ生成スクリプト
"""

import random
from datetime import datetime, timedelta
import csv

# パラメータ設定
NUM_PRODUCTS_PER_CATEGORY = 50  # 各カテゴリごとの商品数

# カテゴリ定義（fact_ordersと整合性を取る）
CATEGORIES = {
    'INT': {  # インテリア
        'category_l1': 'インテリア',
        'subcategories': [
            {'category_l2': '照明', 'category_l3': ['デスクライト', 'フロアライト', 'シーリングライト', 'ペンダントライト']},
            {'category_l2': '家具', 'category_l3': ['デスク', 'チェア', '収納棚', 'テーブル']},
            {'category_l2': '装飾品', 'category_l3': ['時計', '花瓶', 'フレーム', 'オブジェ']},
        ]
    },
    'ELT': {  # 家電
        'category_l1': '家電',
        'subcategories': [
            {'category_l2': '生活家電', 'category_l3': ['掃除機', '空気清浄機', '加湿器', '除湿器']},
            {'category_l2': 'キッチン家電', 'category_l3': ['電子レンジ', '炊飯器', 'トースター', 'コーヒーメーカー']},
            {'category_l2': '美容家電', 'category_l3': ['ドライヤー', 'ヘアアイロン', '美顔器', 'シェーバー']},
        ]
    },
    'FSH': {  # ファッション
        'category_l1': 'ファッション',
        'subcategories': [
            {'category_l2': 'メンズ', 'category_l3': ['シャツ', 'パンツ', 'ジャケット', 'アウター']},
            {'category_l2': 'レディース', 'category_l3': ['ブラウス', 'スカート', 'ワンピース', 'コート']},
            {'category_l2': 'アクセサリー', 'category_l3': ['バッグ', '財布', 'ベルト', '帽子']},
        ]
    },
    'BKS': {  # 書籍
        'category_l1': '書籍',
        'subcategories': [
            {'category_l2': 'ビジネス', 'category_l3': ['経営', 'マーケティング', '自己啓発', 'キャリア']},
            {'category_l2': '技術書', 'category_l3': ['プログラミング', 'データサイエンス', 'AI・機械学習', 'インフラ']},
            {'category_l2': '文芸', 'category_l3': ['小説', 'エッセイ', '詩集', '戯曲']},
        ]
    }
}

# ブランド名
BRANDS = {
    'INT': ['GlacierStyle', 'ModernLiving', 'UrbanDecor', 'NordicHome', 'MinimalSpace'],
    'ELT': ['TechnoLife', 'SmartHome', 'EcoAppliance', 'PowerPlus', 'FutureHome'],
    'FSH': ['StyleCraft', 'UrbanWear', 'ElegantStyle', 'CasualMode', 'PremiumFashion'],
    'BKS': ['KnowledgePress', 'TechPublish', 'BusinessBooks', 'AcademicPress', 'ModernReads']
}

# 商品名のテンプレート
PRODUCT_TEMPLATES = {
    'INT': {
        'デスクライト': ['モダンデスクライト', 'LED調光デスクライト', 'スタイリッシュデスクライト', 'コンパクトデスクライト'],
        'フロアライト': ['スタンディングフロアライト', 'モダンフロアライト', '間接照明フロアライト', '北欧風フロアライト'],
        'シーリングライト': ['LEDシーリングライト', 'リモコン付きシーリングライト', '調光調色シーリングライト', 'デザインシーリングライト'],
        'ペンダントライト': ['吊り下げペンダントライト', 'ガラスペンダントライト', '北欧風ペンダントライト', 'モダンペンダントライト'],
        'デスク': ['シンプルデスク', 'L字型デスク', 'スタンディングデスク', '収納付きデスク'],
        'チェア': ['オフィスチェア', 'エルゴノミクスチェア', 'ゲーミングチェア', 'ダイニングチェア'],
        '収納棚': ['オープンシェルフ', 'ブックシェルフ', 'ディスプレイラック', 'スタッキングシェルフ'],
        'テーブル': ['コーヒーテーブル', 'サイドテーブル', 'ダイニングテーブル', '折りたたみテーブル'],
        '時計': ['壁掛け時計', '置き時計', 'デジタル時計', '振り子時計'],
        '花瓶': ['陶器花瓶', 'ガラス花瓶', 'モダン花瓶', 'アンティーク花瓶'],
        'フレーム': ['写真フレーム', 'アートフレーム', 'ウォールフレーム', 'デジタルフォトフレーム'],
        'オブジェ': ['インテリアオブジェ', '陶器オブジェ', 'モダンアート', 'デコレーションピース'],
    },
    'ELT': {
        '掃除機': ['コードレス掃除機', 'ロボット掃除機', 'スティック掃除機', 'サイクロン掃除機'],
        '空気清浄機': ['HEPAフィルター空気清浄機', '加湿機能付き空気清浄機', '静音空気清浄機', 'コンパクト空気清浄機'],
        '加湿器': ['超音波加湿器', 'スチーム加湿器', 'ハイブリッド加湿器', 'アロマ対応加湿器'],
        '除湿器': ['コンプレッサー除湿器', 'デシカント除湿器', '衣類乾燥除湿器', 'コンパクト除湿器'],
        '電子レンジ': ['オーブンレンジ', 'スチームオーブンレンジ', 'フラット電子レンジ', 'コンパクト電子レンジ'],
        '炊飯器': ['IH炊飯器', '圧力IH炊飯器', 'マイコン炊飯器', '多機能炊飯器'],
        'トースター': ['ポップアップトースター', 'オーブントースター', 'スチームトースター', 'コンパクトトースター'],
        'コーヒーメーカー': ['ドリップコーヒーメーカー', 'エスプレッソマシン', '全自動コーヒーメーカー', 'カプセル式コーヒーメーカー'],
        'ドライヤー': ['イオンドライヤー', '大風量ドライヤー', '速乾ドライヤー', 'コンパクトドライヤー'],
        'ヘアアイロン': ['ストレートアイロン', 'カールアイロン', '2WAYヘアアイロン', 'コードレスヘアアイロン'],
        '美顔器': ['イオン美顔器', 'RF美顔器', 'LED美顔器', 'ウォーターピーリング美顔器'],
        'シェーバー': ['電気シェーバー', '回転式シェーバー', '往復式シェーバー', '防水シェーバー'],
    },
    'FSH': {
        'シャツ': ['ビジネスシャツ', 'カジュアルシャツ', 'オックスフォードシャツ', 'リネンシャツ'],
        'パンツ': ['スラックス', 'チノパン', 'ジーンズ', 'カーゴパンツ'],
        'ジャケット': ['テーラードジャケット', 'ブレザー', 'カーディガン', 'ボンバージャケット'],
        'アウター': ['コート', 'ダウンジャケット', 'トレンチコート', 'パーカー'],
        'ブラウス': ['シフォンブラウス', 'レースブラウス', 'リボンブラウス', 'ストライプブラウス'],
        'スカート': ['フレアスカート', 'タイトスカート', 'プリーツスカート', 'マキシスカート'],
        'ワンピース': ['シャツワンピース', 'ニットワンピース', 'フォーマルワンピース', 'カジュアルワンピース'],
        'コート': ['トレンチコート', 'チェスターコート', 'ダッフルコート', 'ロングコート'],
        'バッグ': ['トートバッグ', 'ショルダーバッグ', 'リュックサック', 'ハンドバッグ'],
        '財布': ['長財布', '二つ折り財布', 'コインケース', 'カードケース'],
        'ベルト': ['レザーベルト', 'メッシュベルト', 'カジュアルベルト', 'ビジネスベルト'],
        '帽子': ['キャップ', 'ハット', 'ニット帽', 'ベレー帽'],
    },
    'BKS': {
        '経営': ['経営戦略入門', 'リーダーシップの教科書', '組織マネジメント実践', 'イノベーション経営'],
        'マーケティング': ['デジタルマーケティング入門', 'ブランド戦略', 'コンテンツマーケティング', 'SNSマーケティング実践'],
        '自己啓発': ['時間管理術', '思考法トレーニング', 'コミュニケーション力向上', '習慣化メソッド'],
        'キャリア': ['キャリア設計の教科書', '転職成功ガイド', 'スキルアップ戦略', 'リモートワーク入門'],
        'プログラミング': ['Python入門', 'JavaScript完全ガイド', 'Web開発実践', 'アルゴリズム入門'],
        'データサイエンス': ['データ分析入門', '統計学基礎', 'SQL実践ガイド', 'データ可視化テクニック'],
        'AI・機械学習': ['機械学習入門', 'ディープラーニング実践', 'AI活用ガイド', '自然言語処理入門'],
        'インフラ': ['クラウド入門', 'Docker実践ガイド', 'ネットワーク基礎', 'セキュリティ入門'],
        '小説': ['ミステリー小説', 'SF小説', '恋愛小説', '歴史小説'],
        'エッセイ': ['日常エッセイ', '旅行エッセイ', '人生エッセイ', '食エッセイ'],
        '詩集': ['現代詩集', '抒情詩集', '散文詩集', '短歌集'],
        '戯曲': ['現代戯曲集', 'クラシック戯曲', '一人芝居台本', '短編戯曲集'],
    }
}

# 商品名（英語）のテンプレート
PRODUCT_TEMPLATES_EN = {
    'INT': {
        'デスクライト': ['Modern Desk Light', 'LED Dimmable Desk Lamp', 'Stylish Desk Light', 'Compact Desk Lamp'],
        'フロアライト': ['Standing Floor Lamp', 'Modern Floor Light', 'Ambient Floor Lamp', 'Nordic Floor Light'],
        'シーリングライト': ['LED Ceiling Light', 'Remote Control Ceiling Light', 'Dimmable Ceiling Light', 'Design Ceiling Light'],
        'ペンダントライト': ['Hanging Pendant Light', 'Glass Pendant Lamp', 'Nordic Pendant Light', 'Modern Pendant Light'],
        'デスク': ['Simple Desk', 'L-Shaped Desk', 'Standing Desk', 'Desk with Storage'],
        'チェア': ['Office Chair', 'Ergonomic Chair', 'Gaming Chair', 'Dining Chair'],
        '収納棚': ['Open Shelf', 'Bookshelf', 'Display Rack', 'Stacking Shelf'],
        'テーブル': ['Coffee Table', 'Side Table', 'Dining Table', 'Folding Table'],
        '時計': ['Wall Clock', 'Desk Clock', 'Digital Clock', 'Pendulum Clock'],
        '花瓶': ['Ceramic Vase', 'Glass Vase', 'Modern Vase', 'Antique Vase'],
        'フレーム': ['Photo Frame', 'Art Frame', 'Wall Frame', 'Digital Photo Frame'],
        'オブジェ': ['Interior Object', 'Ceramic Object', 'Modern Art', 'Decoration Piece'],
    },
    'ELT': {
        '掃除機': ['Cordless Vacuum', 'Robot Vacuum', 'Stick Vacuum', 'Cyclone Vacuum'],
        '空気清浄機': ['HEPA Air Purifier', 'Humidifying Air Purifier', 'Silent Air Purifier', 'Compact Air Purifier'],
        '加湿器': ['Ultrasonic Humidifier', 'Steam Humidifier', 'Hybrid Humidifier', 'Aroma Humidifier'],
        '除湿器': ['Compressor Dehumidifier', 'Desiccant Dehumidifier', 'Clothes Dryer Dehumidifier', 'Compact Dehumidifier'],
        '電子レンジ': ['Microwave Oven', 'Steam Oven', 'Flat Microwave', 'Compact Microwave'],
        '炊飯器': ['IH Rice Cooker', 'Pressure IH Rice Cooker', 'Microcomputer Rice Cooker', 'Multi-Function Rice Cooker'],
        'トースター': ['Pop-up Toaster', 'Oven Toaster', 'Steam Toaster', 'Compact Toaster'],
        'コーヒーメーカー': ['Drip Coffee Maker', 'Espresso Machine', 'Automatic Coffee Maker', 'Capsule Coffee Maker'],
        'ドライヤー': ['Ion Hair Dryer', 'High Power Dryer', 'Quick Dry Dryer', 'Compact Dryer'],
        'ヘアアイロン': ['Straight Iron', 'Curling Iron', '2-Way Hair Iron', 'Cordless Hair Iron'],
        '美顔器': ['Ion Beauty Device', 'RF Beauty Device', 'LED Beauty Device', 'Water Peeling Device'],
        'シェーバー': ['Electric Shaver', 'Rotary Shaver', 'Foil Shaver', 'Waterproof Shaver'],
    },
    'FSH': {
        'シャツ': ['Business Shirt', 'Casual Shirt', 'Oxford Shirt', 'Linen Shirt'],
        'パンツ': ['Slacks', 'Chino Pants', 'Jeans', 'Cargo Pants'],
        'ジャケット': ['Tailored Jacket', 'Blazer', 'Cardigan', 'Bomber Jacket'],
        'アウター': ['Coat', 'Down Jacket', 'Trench Coat', 'Parka'],
        'ブラウス': ['Chiffon Blouse', 'Lace Blouse', 'Ribbon Blouse', 'Stripe Blouse'],
        'スカート': ['Flare Skirt', 'Tight Skirt', 'Pleated Skirt', 'Maxi Skirt'],
        'ワンピース': ['Shirt Dress', 'Knit Dress', 'Formal Dress', 'Casual Dress'],
        'コート': ['Trench Coat', 'Chester Coat', 'Duffle Coat', 'Long Coat'],
        'バッグ': ['Tote Bag', 'Shoulder Bag', 'Backpack', 'Handbag'],
        '財布': ['Long Wallet', 'Bifold Wallet', 'Coin Case', 'Card Case'],
        'ベルト': ['Leather Belt', 'Mesh Belt', 'Casual Belt', 'Business Belt'],
        '帽子': ['Cap', 'Hat', 'Knit Cap', 'Beret'],
    },
    'BKS': {
        '経営': ['Business Strategy Basics', 'Leadership Handbook', 'Organization Management', 'Innovation Management'],
        'マーケティング': ['Digital Marketing Guide', 'Brand Strategy', 'Content Marketing', 'SNS Marketing Practice'],
        '自己啓発': ['Time Management', 'Thinking Training', 'Communication Skills', 'Habit Formation Method'],
        'キャリア': ['Career Design Guide', 'Job Change Success Guide', 'Skill Up Strategy', 'Remote Work Guide'],
        'プログラミング': ['Python Basics', 'JavaScript Complete Guide', 'Web Development Practice', 'Algorithm Basics'],
        'データサイエンス': ['Data Analysis Basics', 'Statistics Fundamentals', 'SQL Practice Guide', 'Data Visualization'],
        'AI・機械学習': ['Machine Learning Basics', 'Deep Learning Practice', 'AI Application Guide', 'NLP Basics'],
        'インフラ': ['Cloud Computing Basics', 'Docker Practice Guide', 'Network Fundamentals', 'Security Basics'],
        '小説': ['Mystery Novel', 'Science Fiction', 'Romance Novel', 'Historical Fiction'],
        'エッセイ': ['Daily Essay', 'Travel Essay', 'Life Essay', 'Food Essay'],
        '詩集': ['Modern Poetry', 'Lyric Poetry', 'Prose Poetry', 'Tanka Collection'],
        '戯曲': ['Modern Play', 'Classic Drama', 'Monologue Script', 'Short Play Collection'],
    }
}

def generate_product_description(category_code, category_l3, product_name):
    """商品説明の生成"""
    descriptions = {
        'INT': f"スタイリッシュなデザインと機能性を両立した{category_l3}。{product_name}は、モダンな空間にぴったりのインテリアアイテムです。高品質な素材を使用し、長くご愛用いただけます。",
        'ELT': f"最新技術を搭載した{category_l3}。{product_name}は、快適な生活をサポートする高性能家電です。省エネ設計で環境にも優しく、使いやすさにもこだわりました。",
        'FSH': f"上質な素材とデザインにこだわった{category_l3}。{product_name}は、様々なシーンで活躍するファッションアイテムです。着心地の良さと耐久性を兼ね備えています。",
        'BKS': f"実践的な知識とノウハウが詰まった{category_l3}の書籍。{product_name}は、初心者から上級者まで幅広く学べる内容となっています。豊富な事例と分かりやすい解説で、すぐに実践できます。",
    }
    return descriptions.get(category_code, "高品質な商品です。")

def generate_dimensions(category_code, category_l3):
    """サイズの生成"""
    if category_code == 'INT':
        if 'ライト' in category_l3:
            return f"W{random.randint(10,30)}×D{random.randint(10,30)}×H{random.randint(20,60)}cm"
        elif category_l3 in ['デスク', 'テーブル']:
            return f"W{random.randint(80,180)}×D{random.randint(50,90)}×H{random.randint(70,75)}cm"
        elif category_l3 == 'チェア':
            return f"W{random.randint(40,70)}×D{random.randint(40,70)}×H{random.randint(80,120)}cm"
        else:
            return f"W{random.randint(20,50)}×D{random.randint(15,40)}×H{random.randint(20,80)}cm"
    elif category_code == 'ELT':
        return f"W{random.randint(20,50)}×D{random.randint(20,40)}×H{random.randint(15,80)}cm"
    elif category_code == 'FSH':
        sizes = ['S', 'M', 'L', 'XL', 'Free']
        return random.choice(sizes)
    elif category_code == 'BKS':
        formats = ['A5判', 'B5判', 'A4判', '新書判', '文庫判']
        return random.choice(formats)
    return "標準サイズ"

def generate_weight(category_code, category_l3):
    """重量の生成（グラム）"""
    if category_code == 'INT':
        if 'ライト' in category_l3:
            return random.randint(300, 2000)
        elif category_l3 in ['デスク', 'テーブル']:
            return random.randint(15000, 40000)
        elif category_l3 == 'チェア':
            return random.randint(8000, 15000)
        else:
            return random.randint(500, 5000)
    elif category_code == 'ELT':
        if category_l3 in ['掃除機', '電子レンジ', '炊飯器']:
            return random.randint(3000, 8000)
        else:
            return random.randint(500, 3000)
    elif category_code == 'FSH':
        return random.randint(100, 1500)
    elif category_code == 'BKS':
        return random.randint(200, 800)
    return 1000

def generate_launch_date():
    """発売日の生成（2020年〜2024年）"""
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 12, 31)
    days_between = (end_date - start_date).days
    random_days = random.randint(0, days_between)
    launch_date = start_date + timedelta(days=random_days)
    return launch_date.strftime('%Y-%m-%d')

def generate_products():
    """商品データの生成"""
    products = []
    product_counter = {}
    
    for category_code, category_data in CATEGORIES.items():
        product_counter[category_code] = 1
        category_l1 = category_data['category_l1']
        
        for subcategory in category_data['subcategories']:
            category_l2 = subcategory['category_l2']
            
            for category_l3 in subcategory['category_l3']:
                # 各小カテゴリごとに商品を生成
                num_products = NUM_PRODUCTS_PER_CATEGORY // len(subcategory['category_l3'])
                
                for _ in range(num_products):
                    product_id = f"PRD-{category_code}-{product_counter[category_code]:05d}"
                    product_counter[category_code] += 1
                    
                    # 商品名の生成
                    product_name_base = random.choice(PRODUCT_TEMPLATES[category_code][category_l3])
                    brand = random.choice(BRANDS[category_code])
                    product_name = f"{product_name_base}"
                    
                    # 英語商品名
                    product_name_en = random.choice(PRODUCT_TEMPLATES_EN[category_code][category_l3])
                    
                    # 仕入先ID（1-50の仕入先）
                    supplier_id = f"SUP-{random.randint(1, 50):05d}"
                    
                    # 価格設定
                    if category_code == 'INT':
                        cost_price = round(random.uniform(1000, 30000), 2)
                    elif category_code == 'ELT':
                        cost_price = round(random.uniform(3000, 80000), 2)
                    elif category_code == 'FSH':
                        cost_price = round(random.uniform(500, 15000), 2)
                    elif category_code == 'BKS':
                        cost_price = round(random.uniform(500, 3000), 2)
                    else:
                        cost_price = round(random.uniform(1000, 10000), 2)
                    
                    list_price = round(cost_price * random.uniform(2.0, 3.5), 2)
                    current_price = round(list_price * random.uniform(0.7, 1.0), 2)
                    
                    # 在庫数
                    stock_quantity = random.randint(0, 500)
                    
                    # 商品ステータス
                    if stock_quantity == 0:
                        product_status = random.choice(['out_of_stock', 'discontinued'])
                    else:
                        product_status = random.choice(['active', 'active', 'active', 'discontinued'])
                    
                    # その他の属性
                    launch_date = generate_launch_date()
                    description = generate_product_description(category_code, category_l3, product_name)
                    weight_g = generate_weight(category_code, category_l3)
                    dimensions = generate_dimensions(category_code, category_l3)
                    
                    product = {
                        'product_id': product_id,
                        'product_name': product_name,
                        'product_name_en': product_name_en,
                        'category_l1': category_l1,
                        'category_l2': category_l2,
                        'category_l3': category_l3,
                        'brand': f"{brand} Original",
                        'supplier_id': supplier_id,
                        'cost_price': cost_price,
                        'list_price': list_price,
                        'current_price': current_price,
                        'stock_quantity': stock_quantity,
                        'product_status': product_status,
                        'launch_date': launch_date,
                        'description': description,
                        'weight_g': weight_g,
                        'dimensions': dimensions
                    }
                    
                    products.append(product)
    
    return products

def save_to_csv(products, filename='products.csv'):
    """CSVファイルへの保存"""
    fieldnames = [
        'product_id', 'product_name', 'product_name_en', 'category_l1', 'category_l2',
        'category_l3', 'brand', 'supplier_id', 'cost_price', 'list_price',
        'current_price', 'stock_quantity', 'product_status', 'launch_date',
        'description', 'weight_g', 'dimensions'
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(products)
    
    print(f"✅ {len(products)}件の商品データを {filename} に保存しました")

def generate_sql_insert(products, filename='insert_products.sql'):
    """SQL INSERT文の生成"""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("-- 商品マスタテーブルの作成\n")
        f.write("CREATE OR REPLACE TABLE dim_products (\n")
        f.write("    product_id VARCHAR PRIMARY KEY,\n")
        f.write("    product_name VARCHAR,\n")
        f.write("    product_name_en VARCHAR,\n")
        f.write("    category_l1 VARCHAR,\n")
        f.write("    category_l2 VARCHAR,\n")
        f.write("    category_l3 VARCHAR,\n")
        f.write("    brand VARCHAR,\n")
        f.write("    supplier_id VARCHAR,\n")
        f.write("    cost_price DECIMAL(10,2),\n")
        f.write("    list_price DECIMAL(10,2),\n")
        f.write("    current_price DECIMAL(10,2),\n")
        f.write("    stock_quantity INTEGER,\n")
        f.write("    product_status VARCHAR,\n")
        f.write("    launch_date DATE,\n")
        f.write("    description TEXT,\n")
        f.write("    weight_g INTEGER,\n")
        f.write("    dimensions VARCHAR\n")
        f.write(");\n\n")
        
        f.write("-- サンプルデータの挿入\n")
        f.write("INSERT INTO dim_products VALUES\n")
        
        for i, product in enumerate(products):
            # SQLインジェクション対策：シングルクォートをエスケープ
            description = product['description'].replace("'", "''")
            product_name = product['product_name'].replace("'", "''")
            product_name_en = product['product_name_en'].replace("'", "''")
            
            line = (
                f"('{product['product_id']}', '{product_name}', '{product_name_en}', "
                f"'{product['category_l1']}', '{product['category_l2']}', '{product['category_l3']}', "
                f"'{product['brand']}', '{product['supplier_id']}', {product['cost_price']}, "
                f"{product['list_price']}, {product['current_price']}, {product['stock_quantity']}, "
                f"'{product['product_status']}', '{product['launch_date']}', '{description}', "
                f"{product['weight_g']}, '{product['dimensions']}')"
            )
            
            if i < len(products) - 1:
                f.write(line + ",\n")
            else:
                f.write(line + ";\n")
        
        f.write("\n-- データ件数の確認\n")
        f.write("SELECT COUNT(*) AS total_products FROM dim_products;\n\n")
        f.write("-- カテゴリ別の商品数\n")
        f.write("SELECT category_l1, COUNT(*) AS product_count FROM dim_products GROUP BY category_l1 ORDER BY product_count DESC;\n\n")
        f.write("-- サンプルデータの確認\n")
        f.write("SELECT * FROM dim_products LIMIT 10;\n")
    
    print(f"✅ SQL INSERT文を {filename} に保存しました")

if __name__ == '__main__':
    print(f"🚀 商品データを生成中...")
    products = generate_products()
    
    # CSVファイルに保存
    save_to_csv(products, 'products.csv')
    
    # SQL INSERT文を生成
    generate_sql_insert(products, 'insert_products.sql')
    
    print(f"\n📊 生成された商品データ統計:")
    print(f"  総商品数: {len(products)}件")
    
    # カテゴリ別の集計
    category_counts = {}
    for product in products:
        cat = product['category_l1']
        category_counts[cat] = category_counts.get(cat, 0) + 1
    
    for category, count in category_counts.items():
        print(f"  {category}: {count}件")
    
    print("\n📦 サンプル商品（各カテゴリから1件ずつ）:")
    displayed_categories = set()
    for product in products:
        if product['category_l1'] not in displayed_categories:
            print(f"\n【{product['category_l1']}】")
            print(f"  商品ID: {product['product_id']}")
            print(f"  商品名: {product['product_name']}")
            print(f"  英語名: {product['product_name_en']}")
            print(f"  カテゴリ: {product['category_l1']} > {product['category_l2']} > {product['category_l3']}")
            print(f"  ブランド: {product['brand']}")
            print(f"  価格: ¥{product['current_price']:,.2f} (定価: ¥{product['list_price']:,.2f})")
            print(f"  在庫: {product['stock_quantity']}個")
            print(f"  ステータス: {product['product_status']}")
            displayed_categories.add(product['category_l1'])
    
    print("\n✨ 完了！")

