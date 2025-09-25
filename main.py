import config
import sys
import stt_engine
from utils.ffmpeg_utils import convert_to_wav
from utils.path_utils import ensure_parent_dir, shard_filepath
from pathlib import Path
from oracle import fetch_content_by_id
from sqlite_db import init_db, upsert_record

# 사용자 입력으로 CONTENT_ID 받기
user_input = input("조회할 c.CONTENT_ID를 입력하세요: ").strip().strip("'\"")
if not user_input:
    print("❌ CONTENT_ID가 비어 있습니다. 종료합니다.")
    sys.exit(1)

# 숫자만 허용하려면 아래 주석 해제 (필요 시)
if not user_input.isdigit():
    print("❌ CONTENT_ID는 숫자만 입력하세요. 종료합니다.")
    sys.exit(1)

# Oracle 연결
results = fetch_content_by_id(user_input)  # 내부에서 int 캐스팅 처리됨(리팩 버전 기준)
if not results:
    print("❌ 해당 CONTENT_ID에 대한 결과가 없습니다.")
    sys.exit(1)

print(f'프록시 경로 : {results["PROXY_PATH"]}')  # 기존 출력 유지

# 경로 설정 및 디렉토리 생성
input_file = Path(config.BASE_DAS) / results["PROXY_PATH"]

cid = results["CONTENT_ID"]  # 안전하게 8자리 보정
output_file_wav = shard_filepath(config.BASE_STT_WAV, cid, ".wav")
output_file_json = shard_filepath(config.BASE_STT_JSON, cid, ".json")

ensure_parent_dir(output_file_wav)
ensure_parent_dir(output_file_json)

# ffmpeg 실행
output_file_wav = convert_to_wav(str(input_file), str(output_file_wav))

# Whisper 모델 로드 및 음성 인식
model_size = input("사용할 Whisper 모델 크기(small, medium, large 등)를 입력하세요 (기본: small): ").strip()

whisper_model = stt_engine.STTProcessor(model_size, device="cuda", compute_type="float32")
stt_results = whisper_model.stt_whisper(output_file_wav)

# STT 결과 JSON 저장
output_file_json = stt_engine.save_to_json(stt_results, output_file_json)

# DB 준비(최초 1회 호출해도 되고, 매 실행시 호출해도 비용 거의 없음)
init_db()

# UPSERT 저장
upsert_record(
    results=results,
    wav_path=str(output_file_wav),
    json_path=str(output_file_json),
)

if __name__ == "__main__":
    pass  # 메인 스크립트로 직접 실행될 때만 동작