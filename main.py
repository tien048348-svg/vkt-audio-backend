import asyncio
import os
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from engine_v2 import process_full_job

app = FastAPI(title="NarraVoice Studio API")

# Cấu hình CORS để Frontend (Vercel/Localhost) gọi được Backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class RenderRequest(BaseModel):
    script: str
    voice: str
    rate: int
    pitch: int
    env: str
    volume: int = 0
    reverb: int = 0
    echo: int = 0
    bass: int = 0
    custom_dict: dict = {}

OUTPUT_DIR = r"E:\HMKT\VKT_ECOSYSTEM_CORE\VKT_NARRAVOICE_WEB\backend\jobs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Giả lập Database trạng thái
JOB_STATUS = {}

async def run_render_task(job_id: str, req: RenderRequest):
    JOB_STATUS[job_id] = "processing"
    try:
        zip_path = await process_full_job(
            script=req.script,
            voice=req.voice,
            rate=req.rate,
            pitch=req.pitch,
            volume=req.volume,
            reverb=req.reverb,
            echo=req.echo,
            bass=req.bass,
            output_dir=OUTPUT_DIR,
            job_id=job_id,
            custom_dict=req.custom_dict
        )
        JOB_STATUS[job_id] = "done"
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"Lỗi: {e}")
        
        # TẠO HỒ SƠ BẮT LỖI (DIAGNOSTIC DUMP) THEO CHUẨN V3.4
        diag_data = {
            "job_id": job_id,
            "inputs": {"voice": req.voice, "rate": req.rate, "style": req.env},
            "error_message": str(e),
            "traceback": error_trace
        }
        import json
        with open(os.path.join(OUTPUT_DIR, f"{job_id}.diag.json"), "w", encoding="utf-8") as f:
            json.dump(diag_data, f, ensure_ascii=False, indent=2)
            
        JOB_STATUS[job_id] = "error"

@app.get("/api/diagnostic/{job_id}")
async def download_diagnostic(job_id: str):
    diag_path = os.path.join(OUTPUT_DIR, f"{job_id}.diag.json")
    if os.path.exists(diag_path):
        return FileResponse(diag_path, filename=f"Error_Log_{job_id}.json", media_type="application/json")
    return JSONResponse({"error": "No diagnostic file found"}, status_code=404)

@app.post("/api/render")
async def start_render(req: RenderRequest, background_tasks: BackgroundTasks):
    import uuid
    job_id = f"nvj_{uuid.uuid4().hex[:8]}"
    background_tasks.add_task(run_render_task, job_id, req)
    return JSONResponse({"job_id": job_id, "message": "Job started"})

@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    return JSONResponse({"job_id": job_id, "status": JOB_STATUS.get(job_id, "not_found")})

@app.get("/api/download/{job_id}")
async def download_zip(job_id: str):
    zip_path = os.path.join(OUTPUT_DIR, f"{job_id}.zip")
    if os.path.exists(zip_path):
        return FileResponse(zip_path, filename=f"NarraVoice_{job_id}.zip", media_type="application/zip")
    return JSONResponse({"error": "File not found"}, status_code=404)

# Cache danh sách giọng đọc để tải siêu tốc
VOICE_CACHE = {}

def get_friendly_locale(locale: str) -> str:
    lang_map = {
        "vi": "🇻🇳 Tiếng Việt", "en": "🌐 Tiếng Anh", "ja": "🇯🇵 Tiếng Nhật", "ko": "🇰🇷 Tiếng Hàn", "zh": "🇨🇳 Tiếng Trung",
        "es": "🇪🇸 Tiếng Tây Ban Nha", "fr": "🇫🇷 Tiếng Pháp", "de": "🇩🇪 Tiếng Đức", "it": "🇮🇹 Tiếng Ý", "ru": "🇷🇺 Tiếng Nga",
        "pt": "🇵🇹 Tiếng Bồ Đào Nha", "ar": "🇸🇦 Tiếng Ả Rập", "th": "🇹🇭 Tiếng Thái", "id": "🇮🇩 Tiếng Indonesia", "ms": "🇲🇾 Tiếng Mã Lai",
        "nl": "🇳🇱 Tiếng Hà Lan", "tr": "🇹🇷 Tiếng Thổ Nhĩ Kỳ", "pl": "🇵🇱 Tiếng Ba Lan", "sv": "🇸🇪 Tiếng Thụy Điển", "da": "🇩🇰 Tiếng Đan Mạch",
        "fi": "🇫🇮 Tiếng Phần Lan", "el": "🇬🇷 Tiếng Hy Lạp", "hi": "🇮🇳 Tiếng Hindi", "bn": "🇧🇩 Tiếng Bengal", "ta": "🇮🇳 Tiếng Tamil",
        "te": "🇮🇳 Tiếng Telugu", "ur": "🇵🇰 Tiếng Urdu", "fa": "🇮🇷 Tiếng Ba Tư", "he": "🇮🇱 Tiếng Do Thái", "cs": "🇨🇿 Tiếng Séc",
        "hu": "🇭🇺 Tiếng Hungary", "ro": "🇷🇴 Tiếng Romania", "uk": "🇺🇦 Tiếng Ukraina", "bg": "🇧🇬 Tiếng Bulgaria", "sk": "🇸🇰 Tiếng Slovak",
        "hr": "🇭🇷 Tiếng Croatia", "sr": "🇷🇸 Tiếng Serbia", "sl": "🇸🇮 Tiếng Slovenia", "lt": "🇱🇹 Tiếng Litva", "lv": "🇱🇻 Tiếng Latvia",
        "et": "🇪🇪 Tiếng Estonia", "ca": "🇪🇸 Tiếng Catalan", "eu": "🇪🇸 Tiếng Basque", "gl": "🇪🇸 Tiếng Galicia", "cy": "🏴󠁧󠁢󠁷󠁬󠁳󠁿 Tiếng Wales",
        "ga": "🇮🇪 Tiếng Ireland", "mt": "🇲🇹 Tiếng Malta", "is": "🇮🇸 Tiếng Iceland", "af": "🇿🇦 Tiếng Afrikaans", "sw": "🇰🇪 Tiếng Swahili",
        "zu": "🇿🇦 Tiếng Zulu", "sq": "🇦🇱 Tiếng Albania", "mk": "🇲🇰 Tiếng Macedonia", "ka": "🇬🇪 Tiếng Gruzia", "hy": "🇦🇲 Tiếng Armenia",
        "az": "🇦🇿 Tiếng Azerbaijan", "kk": "🇰🇿 Tiếng Kazakhstan", "uz": "🇺🇿 Tiếng Uzbekistan", "km": "🇰🇭 Tiếng Khmer", "lo": "🇱🇦 Tiếng Lào",
        "my": "🇲🇲 Tiếng Myanmar", "ne": "🇳🇵 Tiếng Nepal", "si": "🇱🇰 Tiếng Sinhala", "gu": "🇮🇳 Tiếng Gujarati", "mr": "🇮🇳 Tiếng Marathi",
        "kn": "🇮🇳 Tiếng Kannada", "ml": "🇮🇳 Tiếng Malayalam", "su": "🇮🇩 Tiếng Sunda", "jv": "🇮🇩 Tiếng Java", "tl": "🇵🇭 Tiếng Tagalog"
    }
    REGION_MAP = {
        "US": "Mỹ", "GB": "Vương Quốc Anh", "AU": "Úc", "CA": "Canada", "HK": "Hồng Kông",
        "IE": "Ireland", "IN": "Ấn Độ", "NZ": "New Zealand", "SG": "Singapore", "PH": "Philippines",
        "ZA": "Nam Phi", "KE": "Kenya", "NG": "Nigeria", "TZ": "Tanzania", "CN": "Trung Quốc",
        "TW": "Đài Loan", "MO": "Ma Cao", "JP": "Nhật Bản", "KR": "Hàn Quốc", "VN": "Việt Nam",
        "ES": "Tây Ban Nha", "MX": "Mexico", "AR": "Argentina", "CO": "Colombia", "PE": "Peru",
        "CL": "Chile", "VE": "Venezuela", "EC": "Ecuador", "GT": "Guatemala", "CU": "Cuba",
        "BO": "Bolivia", "DO": "Cộng hòa Dominica", "HN": "Honduras", "PY": "Paraguay", "SV": "El Salvador",
        "NI": "Nicaragua", "CR": "Costa Rica", "PR": "Puerto Rico", "PA": "Panama", "UY": "Uruguay",
        "FR": "Pháp", "CA": "Canada", "CH": "Thụy Sĩ", "BE": "Bỉ", "DE": "Đức", "AT": "Áo",
        "IT": "Ý", "RU": "Nga", "PT": "Bồ Đào Nha", "BR": "Brazil", "SA": "Ả Rập Xê Út", "AE": "UAE",
        "EG": "Ai Cập", "TH": "Thái Lan", "ID": "Indonesia", "MY": "Malaysia", "NL": "Hà Lan",
        "TR": "Thổ Nhĩ Kỳ", "PL": "Ba Lan", "SE": "Thụy Điển", "DK": "Đan Mạch", "FI": "Phần Lan",
        "GR": "Hy Lạp", "BD": "Bangladesh", "PK": "Pakistan", "IR": "Iran", "IL": "Israel",
        "CZ": "Séc", "HU": "Hungary", "RO": "Romania", "UA": "Ukraina", "BG": "Bulgaria", "SK": "Slovakia",
        "HR": "Croatia", "RS": "Serbia", "SI": "Slovenia", "LT": "Litva", "LV": "Latvia", "EE": "Estonia",
        "MT": "Malta", "IS": "Iceland", "AL": "Albania", "MK": "Macedonia", "GE": "Gruzia", "AM": "Armenia",
        "AZ": "Azerbaijan", "KZ": "Kazakhstan", "UZ": "Uzbekistan", "KH": "Campuchia", "LA": "Lào",
        "MM": "Myanmar", "NP": "Nepal", "LK": "Sri Lanka"
    }

    parts = locale.split("-")
    lang_code = parts[0]
    region_code = parts[1] if len(parts) > 1 else ""
    
    base_name = lang_map.get(lang_code, f"Ngôn ngữ ({lang_code})")
    
    # Nếu có mã vùng, dịch mã vùng sang tên quốc gia tiếng Việt
    if region_code:
        vietnamese_region = REGION_MAP.get(region_code, region_code)
        return f"{base_name} ({vietnamese_region})"
    
    return base_name

@app.get("/api/voices")
async def get_voices():
    global VOICE_CACHE
    if VOICE_CACHE:
        return VOICE_CACHE
        
    import edge_tts
    voices = await edge_tts.list_voices()
    grouped = {}
    
    for v in voices:
        raw_locale = v.get("Locale", "Unknown")
        market = get_friendly_locale(raw_locale)
        
        if market not in grouped:
            grouped[market] = []
            
        name = f"{v['ShortName'].split('-')[-1].replace('Neural', '')} ({'Nữ' if v.get('Gender') == 'Female' else 'Nam'})"
        
        grouped[market].append({
            "id": v["ShortName"], 
            "name": name
        })
        
    # Sắp xếp ưu tiên Tiếng Việt, Tiếng Anh lên đầu
    priority = ["🇻🇳 Tiếng Việt (Việt Nam)", "🌐 Tiếng Anh (Mỹ)", "🌐 Tiếng Anh (Vương Quốc Anh)"]
    sorted_grouped = {}
    for p in priority:
        if p in grouped:
            sorted_grouped[p] = grouped.pop(p)
            
    for k in sorted(grouped.keys()):
        sorted_grouped[k] = grouped[k]
        
    VOICE_CACHE = sorted_grouped
    return sorted_grouped

class PreviewRequest(BaseModel):
    voice: str
    text: str = ""
    rate: int = 0
    pitch: int = 0
    volume: int = 0
    reverb: int = 0
    echo: int = 0
    bass: int = 0
    custom_dict: dict = {}

@app.post("/api/preview")
async def preview_voice(req: PreviewRequest):
    voice = req.voice
    text = req.text if req.text else "VKT xin chào bạn. Hôm nay, chúng ta cùng lắng nghe một giọng nói rõ ràng, tự nhiên và giàu cảm xúc. Trong buổi sớm yên bình, gió khẽ lay hàng cây, còn phía xa, một câu chuyện mới đang bắt đầu."
    
    import edge_tts
    import uuid
    import subprocess
    from engine_v2 import apply_pronunciation_filter
    
    uid = uuid.uuid4().hex[:6]
    raw_path = os.path.join(OUTPUT_DIR, f"raw_{uid}.mp3")
    final_path = os.path.join(OUTPUT_DIR, f"preview_{uid}.mp3")
    
    # Lọc phát âm trước khi gọi Edge-TTS
    text_filtered = apply_pronunciation_filter(text, voice, req.custom_dict)
    
    # 1. Edge-TTS (Tốc độ, Cao độ, Âm lượng)
    rate_str = f"+{req.rate}%" if req.rate >= 0 else f"{req.rate}%"
    pitch_str = f"+{req.pitch}Hz" if req.pitch >= 0 else f"{req.pitch}Hz"
    vol_str = f"+{req.volume}%" if req.volume >= 0 else f"{req.volume}%"
    
    comm = edge_tts.Communicate(text_filtered, voice, rate=rate_str, pitch=pitch_str, volume=vol_str)
    audio_chunks = []
    async for chunk in comm.stream():
        if chunk["type"] == "audio":
            audio_chunks.append(chunk["data"])
            
    with open(raw_path, "wb") as f:
        f.write(b"".join(audio_chunks))

    # 2. Xử lý Vang, Vọng, Bass qua FFmpeg nếu có
    if req.reverb > 0 or req.echo > 0 or req.bass > 0:
        filters = []
        if req.bass > 0:
            # Bass boost cực mạnh: tăng tới 25dB ở dải 100Hz
            gain = (req.bass / 100) * 25 
            filters.append(f"bass=g={gain}:f=100:w=0.5")
            
        if req.reverb > 0 or req.echo > 0:
            # Echo delay từ 50ms đến 600ms (rõ mồn một)
            # Decay (độ vang dài) lên tới 0.9 (kéo dài 2-3 giây)
            delay = int(50 + (req.echo / 100) * 550) 
            decay = 0.2 + (max(req.reverb, req.echo) / 100) * 0.7
            filters.append(f"aecho=0.8:0.9:{delay}:{decay}")
            
        try:
            ffmpeg_exe = os.path.join(os.path.dirname(__file__), "ffmpeg.exe")
            cmd = [ffmpeg_exe if os.path.exists(ffmpeg_exe) else 'ffmpeg', '-y', '-i', raw_path, '-af', ','.join(filters), final_path]
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            if os.path.exists(raw_path): os.remove(raw_path)
        except (FileNotFoundError, subprocess.CalledProcessError):
            # Fallback nếu máy người dùng chưa cài FFmpeg
            final_path = raw_path
    else:
        final_path = raw_path
    
    return FileResponse(final_path, media_type="audio/mpeg")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
