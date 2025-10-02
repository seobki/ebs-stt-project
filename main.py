# main.py
import argparse
import sys
import config
import stt_engine
from pathlib import Path
from utils.ffmpeg_utils import convert_to_wav
from utils.path_utils import ensure_parent_dir, shard_filepath
from oracle import fetch_contents_by_year_range
from pg_db import init_db, upsert_record, upsert_segments
from utils.logger_utils import get_logger

logger = get_logger("stt_app")

def process_one(rec: dict, whisper):
    input_file = Path(config.BASE_DAS) / rec["PROXY_PATH"]
    cid = rec["CONTENT_ID"]
    out_wav = shard_filepath(config.BASE_STT_WAV, cid, ".wav")
    out_json = shard_filepath(config.BASE_STT_JSON, cid, ".json")
    ensure_parent_dir(out_wav); ensure_parent_dir(out_json)

    # 1) ffmpeg: mp4 → wav
    out_wav = convert_to_wav(str(input_file), str(out_wav))

    # 2) STT
    stt_results = whisper.stt_whisper(out_wav)

    # 3) JSON 저장
    out_json = stt_engine.save_to_json(stt_results, out_json)

    # 4) Postgres upsert
    upsert_record(results=rec, wav_path=str(out_wav), json_path=str(out_json))
    upsert_segments(content_id=str(cid), stt_segments=stt_results)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-start", type=int, required=True, help="처리 시작 연도 (예: 2017)")
    ap.add_argument("--year-end", type=int, required=True, help="처리 종료 연도 (예: 2025)")
    ap.add_argument("--model-size", type=str, default="small", help="Whisper 모델 크기")
    args = ap.parse_args()

    if args.year_start > args.year_end:
        logger.error("❌ year-start 는 year-end 보다 클 수 없습니다.")
        sys.exit(2)

    # DB 스키마 준비
    init_db()

    # Oracle에서 범위 전체 fetchall
    rows = fetch_contents_by_year_range(args.year_start, args.year_end)
    if not rows:
        logger.error("❌ 조회 결과가 없습니다.")
        sys.exit(1)

    # Whisper 모델 1회 로드
    whisper = stt_engine.STTProcessor(args.model_size, device="cuda", compute_type="float32")

    total = len(rows)
    logger.info(f"📦 총 처리 건수: {total} (연도: {args.year_start}~{args.year_end})")

    for i, rec in enumerate(rows, start=1):
        cid = rec.get("CONTENT_ID")
        try:
            logger.info(f"[{i}/{total}] 처리 시작: CONTENT_ID={cid}")
            process_one(rec, whisper)
            logger.info(f"[{i}/{total}] 처리 완료: CONTENT_ID={cid}")
        except Exception as e:
            logger.error(f"[{i}/{total}] ⚠️ 처리 실패: CONTENT_ID={cid}, err={e}")

if __name__ == "__main__":
    main()
