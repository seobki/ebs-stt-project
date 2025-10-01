import subprocess
from datetime import datetime, timedelta
from utils.logger_utils import get_logger

logger = get_logger("stt_app")

def convert_to_wav(input_path: str, output_path: str):
    # mp4/mxf 등 영상을 wav(16kHz, mono, pcm_s16le)로 변환
    if not input_path:
        raise ValueError(f"❌ ffmpeg input 값 없음! : {input_path}")

    try:
        start_time = datetime.now()
        
        ffmpeg_command = [
            "ffmpeg",
            "-y",  # 기존 파일 덮어쓰기
            "-i", input_path,
            "-ar", "16000",
            "-ac", "1",
            "-c:a", "pcm_s16le",
            output_path,
        ]
        subprocess.run(ffmpeg_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        elapsed_time = datetime.now() - start_time
        elapsed_str = str(timedelta(seconds=int(elapsed_time.total_seconds()))).zfill(8)
        
        logger.info(f"wav 파일 저장 성공! : {output_path}, 소요시간 : {elapsed_str}")
        return output_path
        
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ ffmpeg 변환 실패: {e}")
        raise