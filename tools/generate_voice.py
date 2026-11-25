"""通話の台本（tools/call_scripts.csv）から mp3 を生成する。

macOS の say で話者ごとに声を変えて読み上げ、ffmpeg で1本の mp3 にまとめる。
使い方: python3 tools/generate_voice.py [--ids CALL-...,CALL-...] [--out DIR]
"""
import argparse
import csv
import re
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OPERATOR_VOICE = 'Kyoko'
# 顧客の声は性別ごとに customer_id で決定的に選ぶ（オペレーターと同じ Kyoko は使わない）
CUSTOMER_VOICES = {
    'female': ['Flo (日本語（日本）)', 'Sandy (日本語（日本）)', 'Shelley (日本語（日本）)'],
    'male': ['Eddy (日本語（日本）)', 'Reed (日本語（日本）)', 'Rocko (日本語（日本）)'],
}
PAUSE_SEC = 0.4  # 発話の間に入れる無音
# say が読み間違える表記の読み替え（台本の原文は変えず、読み上げにだけ使う）
READINGS = [
    (r'GlacierStyle', 'グレイシャースタイル'),
    (r'LED', 'エルイーディー'),
    (r'ORD-(\d{4})-(\d{6})', r'オーディーアール、\1の、\2'),
    (r'(\d+)cm', r'\1センチ'),
    (r'(\d+)m(?![A-Za-z])', r'\1メートル'),
    (r'(\d+)〜(\d+)', r'\1から\2'),
    (r'A5', 'エーご'),
    (r'B6', 'ビーろく'),
]


def reading(text: str) -> str:
    for pattern, repl in READINGS:
        text = re.sub(pattern, repl, text)
    return text


def customer_voice(customer_id: str, gender: str) -> str:
    voices = CUSTOMER_VOICES.get(gender, CUSTOMER_VOICES['female'])
    return voices[int(customer_id.split('-')[-1]) % len(voices)]


def utterances(transcript: str):
    """「オペレーター: ...」「顧客: ...」の行を (話者, 発話) に分ける。"""
    for line in transcript.splitlines():
        speaker, _, text = line.partition(':')
        if text.strip():
            yield speaker.strip(), text.strip()


def synthesize(row: dict, gender: str, out_path: Path) -> float:
    cust_voice = customer_voice(row['customer_id'], gender)
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        silence = tmp / 'silence.wav'
        subprocess.run(['ffmpeg', '-loglevel', 'error', '-f', 'lavfi', '-i',
                        'anullsrc=r=22050:cl=mono', '-t', str(PAUSE_SEC), silence], check=True)
        parts = []
        for i, (speaker, text) in enumerate(utterances(row['transcript_text'])):
            aiff = tmp / f'{i:03d}.aiff'
            voice = OPERATOR_VOICE if speaker == 'オペレーター' else cust_voice
            subprocess.run(['say', '-v', voice, '-o', aiff, reading(text)], check=True)
            wav = tmp / f'{i:03d}.wav'
            subprocess.run(['ffmpeg', '-loglevel', 'error', '-i', aiff, '-ar', '22050',
                            '-ac', '1', wav], check=True)
            parts += [wav, silence]
        concat = tmp / 'list.txt'
        concat.write_text(''.join(f"file '{p}'\n" for p in parts))
        subprocess.run(['ffmpeg', '-loglevel', 'error', '-y', '-f', 'concat', '-safe', '0',
                        '-i', concat, '-ac', '1', '-b:a', '32k', out_path], check=True)
    dur = subprocess.run(['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                          '-of', 'csv=p=0', out_path], capture_output=True, text=True, check=True)
    return round(float(dur.stdout), 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--ids', help='生成する call_id（カンマ区切り）。省略時は全件')
    ap.add_argument('--out', default=str(ROOT / 'data' / 'voice_logs'))
    args = ap.parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    genders = {r['customer_id']: r['gender'] for r in csv.DictReader(open(ROOT / 'data' / 'customers.csv'))}
    rows = list(csv.DictReader(open(ROOT / 'tools' / 'call_scripts.csv')))
    if args.ids:
        wanted = set(args.ids.split(','))
        rows = [r for r in rows if r['call_id'] in wanted]
        assert len(rows) == len(wanted), '存在しない call_id があります'

    durations = {}
    for r in rows:
        durations[r['call_id']] = synthesize(r, genders[r['customer_id']], out_dir / f"{r['call_id']}.mp3")
        print(f"{r['call_id']}\t{durations[r['call_id']]}s", flush=True)
    # メタデータ JSON の通話時間に使うため、実際の音声の長さを保存
    with open(out_dir / 'durations.csv', 'w', newline='') as f:
        csv.writer(f).writerows([('call_id', 'duration_sec'), *durations.items()])


if __name__ == '__main__':
    main()
