import subprocess
from datetime import datetime, timedelta
from utils.logger_utils import get_logger

logger = get_logger("stt_app")

def convert_to_wav(input_path: str, output_path: str, extra_af: str | None = None):
    # mp4/mxf 등 영상을 wav(16kHz, mono, pcm_s16le)로 변환
    if not input_path:
        raise ValueError(f"❌ ffmpeg input 값 없음! : {input_path}")

    try:
        start_time = datetime.now()
        
        base_filters = ["aresample=16000", "aformat=sample_fmts=s16:channel_layouts=mono"]
        filters = ([extra_af] if extra_af else []) + base_filters
        af_chain = ",".join(filters)
        
        ffmpeg_command = [
            "ffmpeg", "-y",
            "-i", input_path,
            "-vn",
            "-af", af_chain,
            "-ar", "16000",
            "-ac", "1",
            "-sample_fmt", "s16",
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