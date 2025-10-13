# main.py
from __future__ import annotations
import argparse
import sys
from pathlib import Path

import config
import stt_engine
from utils.ffmpeg_utils import convert_to_wav
from utils.path_utils import ensure_parent_dir, shard_filepath
from oracle import fetch_contents_by_year_range
from pg_db import init_db, upsert_record, upsert_segments, has_record
from utils.logger_utils import get_logger
from utils.audio_vad import speech_ratio_wav  # webrtcvad 기반

logger = get_logger("stt_app")

# ---- config.py에서 실행 파라미터 로드 ----
VAD_SKIP_THRESHOLD   = getattr(config, "VAD_SKIP_THRESHOLD", 0.02)   # 2% 미만 → 스킵
VAD_FILTER_THRESHOLD = getattr(config, "VAD_FILTER_THRESHOLD", 0.10) # 2~10% → 전처리 후 STT
VOICE_ENHANCE_AF     = getattr(
    config,
    "VOICE_ENHANCE_AF",
    "highpass=f=200,lowpass=f=3800,dynaudnorm=f=150:g=15,afftdn=nr=20"
)
DEFAULT_DEVICE       = getattr(config, "DEVICE", "cuda")
DEFAULT_COMPUTE_TYPE = getattr(config, "COMPUTE_TYPE", "float32")
# ----------------------------------------


def _convert(in_path: str, out_path: str, extra_af: str | None = None) -> str:
    """
    convert_to_wav 래퍼: extra_af(추가 오디오 필터 체인) 지원 안 하면 자동 폴백.
    - 기대 결과: 16kHz mono s16 PCM WAV
    """
    if extra_af:
        try:
            return convert_to_wav(in_path, out_path, extra_af=extra_af)  # 새 시그니처
        except TypeError:
            logger.warning("convert_to_wav가 extra_af 미지원 → 기본 변환으로 폴백합니다.")
    return convert_to_wav(in_path, out_path)  # 기존 시그니처


def process_one(rec: dict, whisper, skip_existing: bool = False):
    """단건 처리: ffmpeg → (VAD/전처리) → STT → JSON 저장 → Postgres upsert"""
    cid = str(rec["CONTENT_ID"])
    input_file = Path(config.BASE_DAS) / rec["PROXY_PATH"]
    out_wav = shard_filepath(config.BASE_STT_WAV, cid, ".wav")
    out_json = shard_filepath(config.BASE_STT_JSON, cid, ".json")

    ensure_parent_dir(out_wav)
    ensure_parent_dir(out_json)

    # 이미 처리된 건 스킵 (JSON 파일 또는 DB 레코드 존재 시)
    if skip_existing and (Path(out_json).exists() or has_record(cid)):
        logger.info(f"⏭️  스킵: 이미 처리됨 content_id={cid}")
        return

    if not input_file.exists():
        logger.error(f"❌ 원본 없음: {input_file} (content_id={cid})")
        return

    # 1) 기본 변환(16kHz mono s16)
    try:
        out_wav = _convert(str(input_file), str(out_wav))
    except Exception as e:
        logger.error(f"❌ ffmpeg 변환 실패: content_id={cid}, err={e}")
        return

    # 2) 음성 비율 측정 (webrtcvad)
    try:
        ratio = speech_ratio_wav(str(out_wav), aggressiveness=2)  # 0(느슨)~3(엄격)
    except Exception as e:
        logger.error(f"❌ VAD 측정 실패: content_id={cid}, err={e}")
        ratio = 0.0

    # 2-1) 스킵 기준
    if ratio < VAD_SKIP_THRESHOLD:
        logger.info(f"⏭️  스킵(음성 비율 {ratio:.3%} < {VAD_SKIP_THRESHOLD:.0%}): content_id={cid}")
        return

    # 2-2) 저음성: 전처리 필터 적용 후 재변환
    if ratio < VAD_FILTER_THRESHOLD:
        logger.info(f"🎛️  저음성({ratio:.3%}) → 전처리 필터 적용: content_id={cid}")
        try:
            out_wav = _convert(str(input_file), str(out_wav), extra_af=VOICE_ENHANCE_AF)
        except Exception as e:
            logger.error(f"❌ 전처리 재변환 실패: content_id={cid}, err={e}")
            return

    # 3) STT (stt_engine이 vad_filter 파라미터를 지원한다면 아래 주석 해제하여 사용)
    try:
        # stt_results = whisper.stt_whisper(
        #     out_wav,
        #     vad_filter=True,
        #     vad_parameters=dict(
        #         threshold=0.5,
        #         min_speech_duration_ms=250,
        #         max_speech_duration_s=20,
        #         min_silence_duration_ms=200,
        #     ),
        # )
        stt_results = whisper.stt_whisper(out_wav)
    except Exception as e:
        logger.error(f"❌ STT 실패: content_id={cid}, err={e}")
        return

    if not stt_results:
        logger.info(f"⏭️  STT 결과 비어있음 → 음성 없음 처리: content_id={cid}")
        return

    # 4) 저장
    try:
        _ = stt_engine.save_to_json(stt_results, out_json)
    except Exception as e:
        logger.error(f"❌ JSON 저장 실패: content_id={cid}, err={e}")
        return

    try:
        upsert_record(results=rec, wav_path=str(out_wav), json_path=str(out_json))
        upsert_segments(content_id=cid, stt_segments=stt_results)
    except Exception as e:
        logger.error(f"❌ DB upsert 실패: content_id={cid}, err={e}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-start", type=int, required=True, help="처리 시작 연도 (예: 2017)")
    ap.add_argument("--year-end",   type=int, required=True, help="처리 종료 연도 (예: 2025)")
    ap.add_argument("--model-size", type=str, default="small", help="Whisper 모델 크기")
    ap.add_argument("--skip-existing", action="store_true", help="이미 처리된 항목 건너뛰기")

    # 기본값을 config에서 읽어 사용
    ap.add_argument("--device",       type=str, default=DEFAULT_DEVICE)
    ap.add_argument("--compute-type", type=str, default=DEFAULT_COMPUTE_TYPE)

    args = ap.parse_args()

    if args.year_start > args.year_end:
        logger.error("❌ year-start 는 year-end 보다 클 수 없습니다.")
        sys.exit(2)

    # DB 스키마 준비
    init_db()

    # Oracle에서 범위 전체 조회
    rows = fetch_contents_by_year_range(args.year_start, args.year_end)
    if not rows:
        logger.error("❌ 조회 결과가 없습니다.")
        sys.exit(1)

    # Whisper 모델 1회 로드
    logger.info(f"🔧 Whisper init: model={args.model_size}, device={args.device}, type={args.compute_type}")
    whisper = stt_engine.STTProcessor(args.model_size, device=args.device, compute_type=args.compute_type)

    total = len(rows)
    logger.info(f"📦 총 처리 건수: {total} (연도: {args.year_start}~{args.year_end})")

    try:
        for i, rec in enumerate(rows, start=1):
            cid = rec.get("CONTENT_ID")
            try:
                logger.info(f"[{i}/{total}] 처리 시작: CONTENT_ID={cid}")
                process_one(rec, whisper, skip_existing=args.skip_existing)
                logger.info(f"[{i}/{total}] 처리 완료: CONTENT_ID={cid}")
            except Exception as e:
                logger.error(f"[{i}/{total}] ⚠️ 처리 실패: CONTENT_ID={cid}, err={e}")
    except KeyboardInterrupt:
        logger.warning("🛑 사용자 중지: 진행 중단")
        sys.exit(130)


if __name__ == "__main__":
    main()
