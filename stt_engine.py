import json
from datetime import datetime, timedelta
from faster_whisper import WhisperModel
from utils.logger_utils import get_logger

logger = get_logger("stt_app")

# 모델 로드 및 음성 인식

class STTProcessor:
    
    VALID_MODELS = ["tiny", "base", "small", "medium", "large"]

    def __init__(self, model_size="", device="cuda", compute_type="float32"):
        if not model_size:
            model_size = "small"
        elif model_size not in self.VALID_MODELS:
            logger.info(f"❌ 지원하지 않는 모델 크기입니다: {model_size}. 'small'으로 설정합니다.")
            model_size = "small"

        self.model_size = model_size
        logger.info(f"Whisper 모델 로드 중... 크기: {model_size}, 디바이스: {device}, 타입: {compute_type}")
        
        self.model = WhisperModel(model_size, device=device, compute_type=compute_type)
    
    def stt_whisper(self, output_file_wav):
    
        logger.info(f"STT 시작 : {output_file_wav}")
        start_time = datetime.now()
        
        try:
            segments, info = self.model.transcribe(output_file_wav, beam_size=3)
            logger.info(f"언어 감지 : {info.language}, (확신도: {info.language_probability:.2f})")
        
            stt_results = []
            for segment in segments:
                start_str = str(timedelta(seconds=int(segment.start))).zfill(8)
                end_str   = str(timedelta(seconds=int(segment.end))).zfill(8)

                print(f"[{start_str} --> {end_str}] {segment.text}")
                stt_results.append({
                    "start": start_str,
                    "end": end_str,
                    "text": segment.text
                })
            
            end_time = datetime.now()    
            elapsed_time = datetime.now() - start_time
            elapsed_str = str(timedelta(seconds=int(elapsed_time.total_seconds()))).zfill(8)
            
            logger.info(f"STT 종료 : {elapsed_str} (시작시간 : {start_time.strftime('%H:%M:%S')}, 종료시간 : {end_time.strftime('%H:%M:%S')})")
            
            return stt_results
        
        except Exception as e:
            logger.error(f"❌ STT 실패: {e}")
            return []

def save_to_json(stt_results, output_file_json):
    
    # STT 결과 JSON 저장
    try:
        with open(output_file_json, "w", encoding="utf-8") as json_file:
            json.dump(stt_results, json_file, ensure_ascii=False, indent=4)
        logger.info(f"STT 결과 JSON 파일 저장 성공! : {output_file_json}")
        return output_file_json
    except Exception as e:
        logger.error(f"❌ JSON 파일 저장 실패: {e}")
        return None

# 함수형 예전 버전 (참고용)    
# model = WhisperModel("small", device="cuda", compute_type="float32")

# def stt_whisper(output_file_wav):
    
#     logger.info(f"STT 시작 : {output_file_wav}")
#     start_time = datetime.now()
    
#     try:
#         segments, info = model.transcribe(output_file_wav, beam_size=3)
#         logger.info(f"언어 감지 : {info.language}, (확신도: {info.language_probability:.2f})")
    
#         stt_results = []
#         for segment in segments:
#             start_str = str(timedelta(seconds=int(segment.start))).zfill(8)
#             end_str   = str(timedelta(seconds=int(segment.end))).zfill(8)

#             logger.info(f"[{start_str} --> {end_str}] {segment.text}")
#             stt_results.append({
#                 "start": start_str,
#                 "end": end_str,
#                 "text": segment.text
#             })
            
#         elapsed_time = datetime.now() - start_time
#         elapsed_str = str(timedelta(seconds=int(elapsed_time.total_seconds()))).zfill(8)
        
#         logger.info(f"STT 종료 : {elapsed_str} (시작시간 : {start_time.strftime('%H:%M:%S')}, 종료시간 : {datetime.now().strftime('%H:%M:%S')})")
        
#         return stt_results
    
#     except Exception as e:
#         logger.info(f"❌ STT 실패: {e}")
#         return []

