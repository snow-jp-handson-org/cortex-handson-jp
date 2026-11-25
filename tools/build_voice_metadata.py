"""通話音声のメタデータ（data/voice_logs/voice_logs_metadata.json）を作る。

- 台本 88件: tools/call_scripts.csv の属性と、generate_voice.py が出力した実際の音声の長さから作る
- 既存の音声 10件（CALL_001〜010.mp3）: 今のメタデータを引き継ぎ、注文・商品は持たない（null）
- 問い合わせ種別（旧 category）は AI_EXTRACT で取り出す項目なので持たせない
使い方: python3 tools/build_voice_metadata.py --durations /tmp/voice_all/durations.csv
"""
import argparse
import csv
import json
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
META = ROOT / 'data' / 'voice_logs' / 'voice_logs_metadata.json'
FIELDS = ['call_id', 'audio_file', 'call_start_time', 'call_end_time', 'call_duration_sec',
          'call_type', 'agent_id', 'customer_id', 'customer_phone', 'order_id', 'product_id']


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--durations', required=True)
    args = ap.parse_args()

    durations = {r['call_id']: float(r['duration_sec']) for r in csv.DictReader(open(args.durations))}
    phones = {r['customer_id']: r['phone'] for r in csv.DictReader(open(ROOT / 'data' / 'customers.csv'))}

    records = []
    for r in csv.DictReader(open(ROOT / 'tools' / 'call_scripts.csv')):
        start = datetime.fromisoformat(r['call_datetime'])
        dur = durations[r['call_id']]
        records.append({
            'call_id': r['call_id'],
            'audio_file': f"{r['call_id']}.mp3",
            'call_start_time': start.isoformat(),
            'call_end_time': (start + timedelta(seconds=round(dur))).isoformat(),
            'call_duration_sec': dur,
            'call_type': 'inbound',
            'agent_id': r['agent_id'],
            'customer_id': r['customer_id'],
            'customer_phone': phones[r['customer_id']],
            'order_id': r['order_id'] or None,
            'product_id': r['product_id'] or None,
        })

    # 既存の10件は audio_file が CALL_xxx.mp3 のものだけを引き継ぐ（再実行しても二重にならない）
    for old in json.load(open(META)):
        if old['audio_file'].startswith('CALL_'):
            records.append({k: old.get(k) for k in FIELDS})

    assert len(records) == 98 and len({x['call_id'] for x in records}) == 98
    records.sort(key=lambda x: x['call_start_time'])
    META.write_text(json.dumps(records, ensure_ascii=False, indent=2) + '\n')
    print(f'{len(records)} 件を書き出しました: {META}')


if __name__ == '__main__':
    main()
