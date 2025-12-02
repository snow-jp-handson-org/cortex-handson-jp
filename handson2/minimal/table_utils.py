# =========================================================
# Snowflake Cortex Handson シナリオ#2
# テーブルユーティリティ - フォールバック機能
# =========================================================
# 概要: Part1の成果物テーブルが存在しない場合、自動的にフォールバックテーブルを参照
# =========================================================

from snowflake.snowpark.context import get_active_session

# フォールバックテーブルのマッピング
# キー: 元のテーブル名, 値: フォールバックテーブル名
FALLBACK_TABLE_MAPPING = {
    "PRODUCT_MASTER": "PRODUCT_MASTER_FALLBACK",
    "PRODUCT_MASTER_EMBED": "PRODUCT_MASTER_EMBED_FALLBACK",
    "EC_DATA_WITH_PRODUCT_MASTER": "EC_DATA_WITH_PRODUCT_MASTER_FALLBACK",
    "RETAIL_DATA_WITH_PRODUCT_MASTER": "RETAIL_DATA_WITH_PRODUCT_MASTER_FALLBACK",
}


def _get_session():
    """Snowflakeセッションを取得"""
    return get_active_session()


def _table_exists(session, table_name: str) -> bool:
    """テーブルの存在確認（内部用）"""
    try:
        session.sql(f"SELECT 1 FROM {table_name} LIMIT 1").collect()
        return True
    except:
        return False


def resolve_table_name(table_name: str, session=None) -> str:
    """
    テーブル名を解決する。実テーブルが存在すればそれを返し、
    存在しない場合はフォールバックテーブル名を返す。
    
    Args:
        table_name: 元のテーブル名
        session: Snowflakeセッション（省略可）
    
    Returns:
        解決されたテーブル名
    
    Example:
        >>> actual_table = resolve_table_name("EC_DATA_WITH_PRODUCT_MASTER")
        # Part1完了済み: "EC_DATA_WITH_PRODUCT_MASTER"
        # Part1未完了: "EC_DATA_WITH_PRODUCT_MASTER_FALLBACK"
    """
    if session is None:
        session = _get_session()
    
    # 元のテーブルが存在するか確認
    if _table_exists(session, table_name):
        return table_name
    
    # フォールバックテーブルがマッピングに存在するか確認
    if table_name in FALLBACK_TABLE_MAPPING:
        fallback_table = FALLBACK_TABLE_MAPPING[table_name]
        if _table_exists(session, fallback_table):
            return fallback_table
    
    # どちらも存在しない場合は元のテーブル名を返す（エラーハンドリングは呼び出し元で）
    return table_name


def check_table_with_fallback(table_name: str, session=None) -> dict:
    """
    テーブルの存在確認とフォールバック情報を返す
    
    Args:
        table_name: 確認するテーブル名
        session: Snowflakeセッション（省略可）
    
    Returns:
        dict: {
            "exists": bool,           # テーブルが利用可能か
            "actual_table": str,      # 実際に使用するテーブル名
            "is_fallback": bool,      # フォールバックを使用しているか
            "original_exists": bool   # 元のテーブルが存在するか
        }
    """
    if session is None:
        session = _get_session()
    
    original_exists = _table_exists(session, table_name)
    
    if original_exists:
        return {
            "exists": True,
            "actual_table": table_name,
            "is_fallback": False,
            "original_exists": True
        }
    
    # フォールバックを確認
    if table_name in FALLBACK_TABLE_MAPPING:
        fallback_table = FALLBACK_TABLE_MAPPING[table_name]
        fallback_exists = _table_exists(session, fallback_table)
        
        if fallback_exists:
            return {
                "exists": True,
                "actual_table": fallback_table,
                "is_fallback": True,
                "original_exists": False
            }
    
    return {
        "exists": False,
        "actual_table": table_name,
        "is_fallback": False,
        "original_exists": False
    }


def get_table_count_with_fallback(table_name: str, session=None) -> tuple:
    """
    テーブルのレコード数を取得（フォールバック対応）
    
    Args:
        table_name: テーブル名
        session: Snowflakeセッション（省略可）
    
    Returns:
        tuple: (count, actual_table_name, is_fallback)
    """
    if session is None:
        session = _get_session()
    
    info = check_table_with_fallback(table_name, session)
    
    if not info["exists"]:
        return (0, table_name, False)
    
    try:
        result = session.sql(f"SELECT COUNT(*) as count FROM {info['actual_table']}").collect()
        count = result[0]['COUNT']
    except:
        count = 0
    
    return (count, info["actual_table"], info["is_fallback"])


def get_data_status_message(table_name: str, session=None) -> str:
    """
    テーブルのステータスメッセージを生成
    
    Args:
        table_name: テーブル名
        session: Snowflakeセッション（省略可）
    
    Returns:
        str: ステータスメッセージ
    """
    info = check_table_with_fallback(table_name, session)
    
    if not info["exists"]:
        return f"❌ {table_name}: テーブルが見つかりません"
    
    if info["is_fallback"]:
        return f"⚠️ {table_name}: フォールバックデータを使用中（Part1スキップモード）"
    
    return f"✅ {table_name}: 利用可能"

