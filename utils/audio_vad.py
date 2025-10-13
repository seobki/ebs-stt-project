import wave
import webrtcvad

def speech_ratio_wav(path: str, aggressiveness: int = 2, frame_ms: int = 20) -> float:
    """
    16kHz mono 16-bit PCM WAV 전제.
    aggressiveness: 0(느슨)~3(엄격) — 높을수록 '음성' 인식이 보수적.
    frame_ms: 10/20/30 중 하나.
    """
    vad = webrtcvad.Vad(aggressiveness)
    with wave.open(path, "rb") as wf:
        sr = wf.getframerate()
        ch = wf.getnchannels()
        sw = wf.getsampwidth()
        if sr not in (8000, 16000, 32000) or ch != 1 or sw != 2:
            raise ValueError(f"VAD requires 8/16/32kHz mono s16 PCM. got sr={sr}, ch={ch}, sw={sw}")
        bytes_per_frame = int(sr * (frame_ms / 1000.0) * 2)
        voiced = total = 0
        while True:
            buf = wf.readframes(bytes_per_frame // 2)
            if len(buf) < bytes_per_frame:
                break
            total += 1
            if vad.is_speech(buf, sr):
                voiced += 1
    return (voiced / total) if total else 0.0
