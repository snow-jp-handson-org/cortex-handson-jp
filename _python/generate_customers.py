"""
顧客データ（dim_customers）のサンプルデータ生成スクリプト
"""

import random
from datetime import datetime, timedelta
import csv

# パラメータ設定
NUM_CUSTOMERS = 100

# 日本人の姓・名のリスト
LAST_NAMES = [
    '佐藤', '鈴木', '高橋', '田中', '渡辺', '伊藤', '山本', '中村', '小林', '加藤',
    '吉田', '山田', '佐々木', '山口', '松本', '井上', '木村', '林', '清水', '山崎',
    '森', '池田', '橋本', '阿部', '石川', '山下', '中島', '石井', '小川', '前田',
    '藤田', '岡田', '後藤', '長谷川', '村上', '近藤', '石田', '遠藤', '青木', '坂本',
    '斎藤', '西村', '福田', '太田', '三浦', '藤井', '岡本', '松田', '中川', '中野'
]

FIRST_NAMES_MALE = [
    '太郎', '健太', '翔太', '大輔', '拓也', '健', '誠', '翔', '修', '裕太',
    '雄太', '貴之', '直樹', '隆', '勇気', '浩', '淳', '渉', '亮', '達也',
    '悠太', '陽介', '大樹', '大地', '勇', '航', '蓮', '颯太', '悠斗', '陸'
]

FIRST_NAMES_FEMALE = [
    '花子', '美咲', 'さくら', '優子', '愛', '結衣', '陽菜', '葵', '凛', '美羽',
    '桜', '芽依', '莉子', '美月', '彩', '真由美', '由美子', '恵子', '明美', '千尋',
    '琴音', '七海', '楓', '心春', '美優', '詩織', '舞', '菜々子', '麻衣', '里奈'
]

# 都道府県・市区町村のデータ
PREFECTURES = [
    {'prefecture': '東京都', 'cities': ['渋谷区', '新宿区', '港区', '世田谷区', '目黒区', '品川区', '中央区', '千代田区', '練馬区', '杉並区']},
    {'prefecture': '神奈川県', 'cities': ['横浜市', '川崎市', '相模原市', '藤沢市', '横須賀市', '平塚市', '鎌倉市', '茅ヶ崎市', '厚木市', '大和市']},
    {'prefecture': '大阪府', 'cities': ['大阪市', '堺市', '豊中市', '吹田市', '高槻市', '枚方市', '茨木市', '八尾市', '寝屋川市', '東大阪市']},
    {'prefecture': '愛知県', 'cities': ['名古屋市', '豊田市', '岡崎市', '一宮市', '豊橋市', '春日井市', '安城市', '豊川市', '西尾市', '刈谷市']},
    {'prefecture': '埼玉県', 'cities': ['さいたま市', '川口市', '川越市', '所沢市', '越谷市', '草加市', '春日部市', '熊谷市', '上尾市', '深谷市']},
    {'prefecture': '千葉県', 'cities': ['千葉市', '船橋市', '松戸市', '市川市', '柏市', '市原市', '八千代市', '流山市', '浦安市', '佐倉市']},
    {'prefecture': '兵庫県', 'cities': ['神戸市', '姫路市', '西宮市', '尼崎市', '明石市', '加古川市', '宝塚市', '伊丹市', '川西市', '三田市']},
    {'prefecture': '福岡県', 'cities': ['福岡市', '北九州市', '久留米市', '飯塚市', '大牟田市', '春日市', '筑紫野市', '大野城市', '宗像市', '太宰府市']},
    {'prefecture': '北海道', 'cities': ['札幌市', '旭川市', '函館市', '釧路市', '苫小牧市', '帯広市', '小樽市', '北見市', '江別市', '千歳市']},
    {'prefecture': '京都府', 'cities': ['京都市', '宇治市', '亀岡市', '城陽市', '長岡京市', '八幡市', '京田辺市', '木津川市', '向日市', '福知山市']},
]

# 町名・番地の生成用
STREET_NAMES = ['中央', '本町', '栄町', '幸町', '緑町', '桜', '旭町', '花園', '若葉', '新町']

def generate_postal_code():
    """郵便番号の生成（nnn-nnnn形式）"""
    return f"{random.randint(100, 999):03d}-{random.randint(0, 9999):04d}"

def generate_phone():
    """電話番号の生成（090-nnnn-nnnn形式）"""
    prefix = random.choice(['090', '080', '070'])
    return f"{prefix}-{random.randint(1000, 9999):04d}-{random.randint(1000, 9999):04d}"

def generate_email(last_name, first_name, customer_id):
    """メールアドレスの生成"""
    # ローマ字風のメールアドレス
    domains = ['example.com', 'email.com', 'mail.com', 'test.jp', 'sample.co.jp']
    # シンプルに顧客IDベースのメールアドレス
    username = f"customer{customer_id}"
    return f"{username}@{random.choice(domains)}"

def generate_address():
    """住所の生成"""
    street = random.choice(STREET_NAMES)
    chome = random.randint(1, 5)
    ban = random.randint(1, 30)
    go = random.randint(1, 20)
    return f"{street}{chome}-{ban}-{go}"

def generate_birth_date():
    """生年月日の生成（20-70歳）"""
    today = datetime.now()
    age = random.randint(20, 70)
    birth_year = today.year - age
    birth_date = datetime(
        birth_year,
        random.randint(1, 12),
        random.randint(1, 28)
    )
    return birth_date.strftime('%Y-%m-%d')

def generate_registration_date():
    """会員登録日の生成（2018年1月1日〜2024年12月31日）"""
    start_date = datetime(2018, 1, 1)
    end_date = datetime(2024, 12, 31)
    days_between = (end_date - start_date).days
    random_days = random.randint(0, days_between)
    registration_date = start_date + timedelta(days=random_days)
    return registration_date.strftime('%Y-%m-%d')

def generate_last_order_date(registration_date):
    """最終注文日の生成（会員登録日〜2024年12月31日）"""
    reg_date = datetime.strptime(registration_date, '%Y-%m-%d')
    end_date = datetime(2024, 12, 31)
    
    # 10%の確率で注文履歴なし
    if random.random() < 0.10:
        return None
    
    days_between = (end_date - reg_date).days
    if days_between <= 0:
        return registration_date
    
    random_days = random.randint(0, days_between)
    last_order = reg_date + timedelta(days=random_days)
    return last_order.strftime('%Y-%m-%d')

def generate_customers(num_customers):
    """顧客データの生成"""
    customers = []
    
    for i in range(1, num_customers + 1):
        customer_id = f"CUS-{i:08d}"
        
        # 性別の決定
        gender = random.choice(['male', 'female', 'male', 'female', 'other'])  # otherは少なめ
        
        # 名前の生成
        last_name = random.choice(LAST_NAMES)
        if gender == 'male':
            first_name = random.choice(FIRST_NAMES_MALE)
        elif gender == 'female':
            first_name = random.choice(FIRST_NAMES_FEMALE)
        else:
            first_name = random.choice(FIRST_NAMES_MALE + FIRST_NAMES_FEMALE)
        
        # 住所の生成
        location = random.choice(PREFECTURES)
        prefecture = location['prefecture']
        city = random.choice(location['cities'])
        address = generate_address()
        postal_code = generate_postal_code()
        
        # その他の属性
        email = generate_email(last_name, first_name, i)
        phone = generate_phone()
        birth_date = generate_birth_date()
        registration_date = generate_registration_date()
        
        # 会員ランク（登録日が古いほど上位ランクになりやすい）
        reg_date = datetime.strptime(registration_date, '%Y-%m-%d')
        days_since_reg = (datetime.now() - reg_date).days
        
        if days_since_reg > 1800:  # 5年以上
            membership_tier = random.choice(['gold', 'platinum', 'platinum', 'silver'])
        elif days_since_reg > 730:  # 2年以上
            membership_tier = random.choice(['silver', 'gold', 'silver', 'standard'])
        else:
            membership_tier = random.choice(['standard', 'silver', 'standard', 'standard'])
        
        # 購入履歴（会員ランクに応じて変動）
        if membership_tier == 'platinum':
            total_orders = random.randint(50, 150)
            total_spent = round(random.uniform(500000, 2000000), 2)
        elif membership_tier == 'gold':
            total_orders = random.randint(20, 80)
            total_spent = round(random.uniform(200000, 800000), 2)
        elif membership_tier == 'silver':
            total_orders = random.randint(10, 40)
            total_spent = round(random.uniform(80000, 400000), 2)
        else:  # standard
            total_orders = random.randint(1, 20)
            total_spent = round(random.uniform(5000, 150000), 2)
        
        # 10%の確率で注文履歴なし
        if random.random() < 0.10:
            total_orders = 0
            total_spent = 0.00
        
        last_order_date = generate_last_order_date(registration_date) if total_orders > 0 else None
        
        # メール配信許諾（70%が許諾）
        email_opt_in = random.random() < 0.70
        
        # アプリインストール（50%がインストール済み）
        app_installed = random.random() < 0.50
        
        customer = {
            'customer_id': customer_id,
            'email': email,
            'phone': phone,
            'last_name': last_name,
            'first_name': first_name,
            'gender': gender,
            'birth_date': birth_date,
            'postal_code': postal_code,
            'prefecture': prefecture,
            'city': city,
            'address': address,
            'registration_date': registration_date,
            'membership_tier': membership_tier,
            'total_orders': total_orders,
            'total_spent': total_spent,
            'last_order_date': last_order_date if last_order_date else '',
            'email_opt_in': email_opt_in,
            'app_installed': app_installed
        }
        
        customers.append(customer)
    
    return customers

def save_to_csv(customers, filename='customers.csv'):
    """CSVファイルへの保存"""
    fieldnames = [
        'customer_id', 'email', 'phone', 'last_name', 'first_name', 'gender',
        'birth_date', 'postal_code', 'prefecture', 'city', 'address',
        'registration_date', 'membership_tier', 'total_orders', 'total_spent',
        'last_order_date', 'email_opt_in', 'app_installed'
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(customers)
    
    print(f"✅ {len(customers)}件の顧客データを {filename} に保存しました")

def generate_sql_insert(customers, filename='insert_customers.sql'):
    """SQL INSERT文の生成"""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("-- 顧客データテーブルの作成\n")
        f.write("CREATE OR REPLACE TABLE dim_customers (\n")
        f.write("    customer_id VARCHAR PRIMARY KEY,\n")
        f.write("    email VARCHAR,\n")
        f.write("    phone VARCHAR,\n")
        f.write("    last_name VARCHAR,\n")
        f.write("    first_name VARCHAR,\n")
        f.write("    gender VARCHAR,\n")
        f.write("    birth_date DATE,\n")
        f.write("    postal_code VARCHAR,\n")
        f.write("    prefecture VARCHAR,\n")
        f.write("    city VARCHAR,\n")
        f.write("    address VARCHAR,\n")
        f.write("    registration_date DATE,\n")
        f.write("    membership_tier VARCHAR,\n")
        f.write("    total_orders INTEGER,\n")
        f.write("    total_spent DECIMAL(12,2),\n")
        f.write("    last_order_date DATE,\n")
        f.write("    email_opt_in BOOLEAN,\n")
        f.write("    app_installed BOOLEAN\n")
        f.write(");\n\n")
        
        f.write("-- サンプルデータの挿入\n")
        f.write("INSERT INTO dim_customers VALUES\n")
        
        for i, customer in enumerate(customers):
            last_order = f"'{customer['last_order_date']}'" if customer['last_order_date'] else 'NULL'
            email_opt = 'TRUE' if customer['email_opt_in'] else 'FALSE'
            app_inst = 'TRUE' if customer['app_installed'] else 'FALSE'
            
            line = (
                f"('{customer['customer_id']}', '{customer['email']}', '{customer['phone']}', "
                f"'{customer['last_name']}', '{customer['first_name']}', '{customer['gender']}', "
                f"'{customer['birth_date']}', '{customer['postal_code']}', '{customer['prefecture']}', "
                f"'{customer['city']}', '{customer['address']}', '{customer['registration_date']}', "
                f"'{customer['membership_tier']}', {customer['total_orders']}, {customer['total_spent']}, "
                f"{last_order}, {email_opt}, {app_inst})"
            )
            
            if i < len(customers) - 1:
                f.write(line + ",\n")
            else:
                f.write(line + ";\n")
        
        f.write("\n-- データ件数の確認\n")
        f.write("SELECT COUNT(*) AS total_customers FROM dim_customers;\n\n")
        f.write("-- サンプルデータの確認\n")
        f.write("SELECT * FROM dim_customers LIMIT 10;\n")
    
    print(f"✅ SQL INSERT文を {filename} に保存しました")

if __name__ == '__main__':
    print(f"🚀 {NUM_CUSTOMERS}件の顧客データを生成中...")
    customers = generate_customers(NUM_CUSTOMERS)
    
    # CSVファイルに保存
    save_to_csv(customers, 'customers.csv')
    
    # SQL INSERT文を生成
    generate_sql_insert(customers, 'insert_customers.sql')
    
    print("\n📊 サンプルデータ（最初の3件）:")
    for i, customer in enumerate(customers[:3]):
        print(f"\n顧客 {i+1}:")
        print(f"  ID: {customer['customer_id']}")
        print(f"  名前: {customer['last_name']} {customer['first_name']}")
        print(f"  性別: {customer['gender']}")
        print(f"  メール: {customer['email']}")
        print(f"  電話: {customer['phone']}")
        print(f"  住所: 〒{customer['postal_code']} {customer['prefecture']}{customer['city']}{customer['address']}")
        print(f"  会員ランク: {customer['membership_tier']}")
        print(f"  累計注文数: {customer['total_orders']}件")
        print(f"  累計購入金額: ¥{customer['total_spent']:,.2f}")
    
    print("\n✨ 完了！")

