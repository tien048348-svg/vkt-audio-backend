import asyncio
import os
import uuid
import json
import traceback
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from engine_v2 import process_full_job

# ========== CẤU HÌNH HỆ THỐNG ==========
VERSION = "1.8.0"
MAX_CHAR_LIMIT = 60000       # ~60 phút audio
MAX_QUEUE_SIZE = 3           # Tối đa 3 job đang chờ/chạy
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "jobs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ========== TRẠNG THÁI TOÀN CỤC (In-Memory) ==========
JOB_STATUS: dict = {}   # job_id -> dict thông tin job
JOB_QUEUE: list = []    # Hàng đợi theo thứ tự
IS_PROCESSING = False   # Đang có job chạy không
VOICE_CACHE = None

app = FastAPI(title="NarraVoice Studio API", version=VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========== HEALTH ENDPOINT (Wake-up ping + Auto-detect) ==========
@app.get("/api/health")
async def health():
    active = sum(1 for j in JOB_STATUS.values() if j["status"] in ["QUEUED", "RUNNING", "MERGING"])
    return {
        "status": "ok",
        "version": VERSION,
        "queue_count": active,
        "queue_capacity": MAX_QUEUE_SIZE,
    }

# ========== VOICE LIST ==========
def get_friendly_locale(locale: str) -> str:
    lang_map = {
        "vi": "🇻🇳 Tiếng Việt", "en": "🇺🇸 Tiếng Anh", "zh": "🇨🇳 Tiếng Trung",
        "ja": "🇯🇵 Tiếng Nhật", "ko": "🇰🇷 Tiếng Hàn", "fr": "🇫🇷 Tiếng Pháp",
        "de": "🇩🇪 Tiếng Đức", "es": "🇪🇸 Tiếng Tây Ban Nha", "it": "🇮🇹 Tiếng Ý",
        "pt": "🇵🇹 Tiếng Bồ Đào Nha", "ru": "🇷🇺 Tiếng Nga", "ar": "🇸🇦 Tiếng Ả Rập",
        "hi": "🇮🇳 Tiếng Hindi", "th": "🇹🇭 Tiếng Thái", "id": "🇮🇩 Tiếng Indonesia",
        "ms": "🇲🇾 Tiếng Mã Lai", "tr": "🇹🇷 Tiếng Thổ Nhĩ Kỳ", "pl": "🇵🇱 Tiếng Ba Lan",
        "nl": "🇳🇱 Tiếng Hà Lan", "sv": "🇸🇪 Tiếng Thụy Điển", "da": "🇩🇰 Tiếng Đan Mạch",
        "fi": "🇫🇮 Tiếng Phần Lan", "nb": "🇳🇴 Tiếng Na Uy", "cs": "🇨🇿 Tiếng Séc",
        "el": "🇬🇷 Tiếng Hy Lạp", "he": "🇮🇱 Tiếng Do Thái", "ro": "🇷🇴 Tiếng Romania",
        "hu": "🇭🇺 Tiếng Hungary", "uk": "🇺🇦 Tiếng Ukraina", "ta": "🇮🇳 Tiếng Tamil",
        "te": "🇮🇳 Tiếng Telugu", "bn": "🇧🇩 Tiếng Bengal", "ur": "🇵🇰 Tiếng Urdu",
        "fa": "🇮🇷 Tiếng Ba Tư", "sk": "🇸🇰 Tiếng Slovak", "bg": "🇧🇬 Tiếng Bulgaria",
        "hr": "🇭🇷 Tiếng Croatia", "sr": "🇷🇸 Tiếng Serbia", "ca": "🇪🇸 Tiếng Catalan",
        "af": "🇿🇦 Tiếng Afrikaans", "sw": "🇰🇪 Tiếng Swahili", "tl": "🇵🇭 Tiếng Tagalog",
        "km": "🇰🇭 Tiếng Khmer", "lo": "🇱🇦 Tiếng Lào", "my": "🇲🇲 Tiếng Myanmar",
        "jv": "🇮🇩 Tiếng Java", "zu": "🇿🇦 Tiếng Zulu", "cy": "🏴󠁧󠁢󠁷󠁬󠁳󠁿 Tiếng Wales",
        "ga": "🇮🇪 Tiếng Ireland", "mt": "🇲🇹 Tiếng Malta", "is": "🇮🇸 Tiếng Iceland",
        "am": "🇪🇹 Tiếng Amharic", "az": "🇦🇿 Tiếng Azerbaijan", "bs": "🇧🇦 Tiếng Bosnia",
        "et": "🇪🇪 Tiếng Estonia", "fil": "🇵🇭 Tiếng Philipin", "gl": "🇪🇸 Tiếng Galicia",
        "gu": "🇮🇳 Tiếng Gujarat", "iu": "🇨🇦 Tiếng Inuktitut", "ka": "🇬🇪 Tiếng Gruzia",
        "kk": "🇰🇿 Tiếng Kazakh", "kn": "🇮🇳 Tiếng Kannada", "lt": "🇱🇹 Tiếng Litva",
        "lv": "🇱🇻 Tiếng Latvia", "mk": "🇲🇰 Tiếng Macedonia", "ml": "🇮🇳 Tiếng Malayalam",
        "mn": "🇲🇳 Tiếng Mông Cổ", "mr": "🇮🇳 Tiếng Marathi", "ne": "🇳🇵 Tiếng Nepal",
        "ps": "🇦🇫 Tiếng Pashto", "si": "🇱🇰 Tiếng Sinhala", "sl": "🇸🇮 Tiếng Slovenia",
        "so": "🇸🇴 Tiếng Somali", "sq": "🇦🇱 Tiếng Albania", "su": "🇮🇩 Tiếng Sunda",
        "uz": "🇺🇿 Tiếng Uzbek"
    }
    REGION_MAP = {
        "VN": "Việt Nam", "US": "Mỹ", "GB": "Anh", "AU": "Úc", "CA": "Canada",
        "CN": "Trung Quốc", "TW": "Đài Loan", "HK": "Hồng Kông", "JP": "Nhật",
        "KR": "Hàn Quốc", "IN": "Ấn Độ", "SG": "Singapore", "PH": "Philippines",
        "MY": "Malaysia", "ID": "Indonesia", "TH": "Thái Lan", "FR": "Pháp",
        "DE": "Đức", "ES": "Tây Ban Nha", "IT": "Ý", "RU": "Nga", "BR": "Brazil",
        "MX": "Mexico", "PT": "Bồ Đào Nha", "SA": "Ả Rập Xê Út", "AE": "UAE",
        "NL": "Hà Lan", "PL": "Ba Lan", "SE": "Thụy Điển", "DK": "Đan Mạch",
        "FI": "Phần Lan", "NO": "Na Uy", "TR": "Thổ Nhĩ Kỳ", "GR": "Hy Lạp",
        "ZA": "Nam Phi", "KE": "Kenya", "EG": "Ai Cập", "NG": "Nigeria",
        "NZ": "New Zealand", "IE": "Ireland", "AT": "Áo", "CH": "Thụy Sĩ",
        "IL": "Israel", "IR": "Iran", "PK": "Pakistan", "BD": "Bangladesh",
        "UA": "Ukraina", "CZ": "Séc", "HU": "Hungary", "RO": "Romania",
        "BG": "Bulgaria", "HR": "Croatia", "RS": "Serbia", "SK": "Slovakia",
        "LT": "Litva", "LV": "Latvia", "EE": "Estonia",
    }
    parts = locale.split("-")
    lang_code = parts[0]
    region_code = parts[1] if len(parts) > 1 else ""
    base_name = lang_map.get(lang_code, f"({lang_code})")
    if region_code:
        region_name = REGION_MAP.get(region_code, region_code)
        return f"{base_name} ({region_name})"
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
        market = get_friendly_locale(v.get("Locale", "Unknown"))
        if market not in grouped:
            grouped[market] = []
        name_part = v['ShortName'].split('-')[-1].replace('Neural', '')
        gender = 'Nữ' if v.get('Gender') == 'Female' else 'Nam'
        grouped[market].append({"id": v["ShortName"], "name": f"{name_part} ({gender})"})
    priority = ["🇻🇳 Tiếng Việt (Việt Nam)", "🌐 Tiếng Anh (Mỹ)", "🌐 Tiếng Anh (Anh)"]
    sorted_grouped = {}
    for p in priority:
        if p in grouped:
            sorted_grouped[p] = grouped.pop(p)
    for k in sorted(grouped.keys()):
        sorted_grouped[k] = grouped[k]
    VOICE_CACHE = sorted_grouped
    return sorted_grouped

# ========== PREVIEW ==========
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
    import edge_tts
    import subprocess
    from engine_v2 import apply_pronunciation_filter
    uid = uuid.uuid4().hex[:6]
    raw_path = os.path.join(OUTPUT_DIR, f"raw_{uid}.mp3")
    final_path = os.path.join(OUTPUT_DIR, f"preview_{uid}.mp3")
    text = req.text or "VKT xin chào bạn. Hôm nay, chúng ta cùng lắng nghe một giọng nói rõ ràng, tự nhiên và giàu cảm xúc."
    text_filtered = apply_pronunciation_filter(text, req.voice, req.custom_dict)
    rate_str = f"+{req.rate}%" if req.rate >= 0 else f"{req.rate}%"
    pitch_str = f"+{req.pitch}Hz" if req.pitch >= 0 else f"{req.pitch}Hz"
    vol_str = f"+{req.volume}%" if req.volume >= 0 else f"{req.volume}%"
    comm = edge_tts.Communicate(text_filtered, req.voice, rate=rate_str, pitch=pitch_str, volume=vol_str)
    audio_chunks = []
    async for chunk in comm.stream():
        if chunk["type"] == "audio":
            audio_chunks.append(chunk["data"])
    with open(raw_path, "wb") as f:
        f.write(b"".join(audio_chunks))
    if req.reverb > 0 or req.echo > 0 or req.bass > 0:
        filters = []
        if req.bass > 0:
            filters.append(f"bass=g={(req.bass/100)*25}:f=100:w=0.5")
        if req.reverb > 0 or req.echo > 0:
            delay = int(50 + (req.echo / 100) * 550)
            decay = 0.2 + (max(req.reverb, req.echo) / 100) * 0.7
            filters.append(f"aecho=0.8:0.9:{delay}:{decay}")
        try:
            ffmpeg_exe = os.path.join(os.path.dirname(__file__), "ffmpeg.exe")
            cmd = [ffmpeg_exe if os.path.exists(ffmpeg_exe) else "ffmpeg", "-y", "-i", raw_path, "-af", ",".join(filters), final_path]
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            if os.path.exists(raw_path): os.remove(raw_path)
        except:
            final_path = raw_path
    else:
        final_path = raw_path
    return FileResponse(final_path, media_type="audio/mpeg")

# ========== BGM SERVE (for preview on frontend) ==========
@app.get("/api/bgm/{style}")
async def serve_bgm(style: str):
    bgm_path = os.path.join(os.path.dirname(__file__), "bgm", f"{style}.mp3")
    if not os.path.exists(bgm_path):
        # Fallback to podcast BGM if style not found
        bgm_path = os.path.join(os.path.dirname(__file__), "bgm", "podcast.mp3")
    if not os.path.exists(bgm_path):
        raise HTTPException(status_code=404, detail="BGM not found")
    return FileResponse(bgm_path, media_type="audio/mpeg")

# ========== RENDER ENGINE WITH QUEUE ==========
class RenderRequest(BaseModel):
    script: str
    voice: str
    rate: int = 0
    pitch: int = 0
    volume: int = 0
    reverb: int = 0
    echo: int = 0
    bass: int = 0
    env: str = "podcast"
    custom_dict: dict = {}
    use_bgm: bool = False
    priority_token: str = ""

async def process_job_queue():
    global IS_PROCESSING
    if IS_PROCESSING:
        return
    IS_PROCESSING = True
    try:
        while JOB_QUEUE:
            job_id = JOB_QUEUE[0]
            job = JOB_STATUS.get(job_id)
            if not job:
                JOB_QUEUE.pop(0)
                continue

            job["status"] = "RUNNING"
            job["queue_pos"] = 0

            async def progress_cb(pct: int, done: int, total: int):
                if job_id in JOB_STATUS:
                    JOB_STATUS[job_id]["progress"] = pct
                    JOB_STATUS[job_id]["chunks_done"] = done
                    JOB_STATUS[job_id]["total_chunks"] = total

            try:
                zip_path = await process_full_job(
                    script=job["script"],
                    voice=job["voice"],
                    rate=job["rate"],
                    pitch=job["pitch"],
                    volume=job["volume"],
                    reverb=job["reverb"],
                    echo=job["echo"],
                    bass=job["bass"],
                    env=job["env"],
                    use_bgm=job["use_bgm"],
                    output_dir=OUTPUT_DIR,
                    job_id=job_id,
                    custom_dict=job["custom_dict"],
                    progress_callback=progress_cb
                )
                JOB_STATUS[job_id]["status"] = "DONE"
                JOB_STATUS[job_id]["zip_url"] = f"/api/download/{job_id}"
                JOB_STATUS[job_id]["progress"] = 100
                JOB_STATUS[job_id]["zip_path"] = zip_path
                print(f"[JOB DONE] {job_id}")
            except Exception as e:
                import traceback
                traceback.print_exc()
                JOB_STATUS[job_id]["status"] = "FAILED"
                JOB_STATUS[job_id]["error"] = str(e)

            # Save diagnostic
            try:
                diag = {
                    "job_id": job_id,
                    "voice": job["voice"],
                    "char_count": len(job["script"]),
                    "chunks": job.get("total_chunks", 0),
                    "status": JOB_STATUS[job_id]["status"],
                    "error": JOB_STATUS[job_id].get("error")
                }
                with open(os.path.join(OUTPUT_DIR, f"{job_id}.diag.json"), "w", encoding="utf-8") as f:
                    json.dump(diag, f, ensure_ascii=False, indent=2)
            except:
                pass

            JOB_QUEUE.pop(0)

            # Cập nhật lại vị trí hàng đợi cho các job còn lại
            for i, jid in enumerate(JOB_QUEUE):
                if jid in JOB_STATUS:
                    JOB_STATUS[jid]["queue_pos"] = i + 1 if IS_PROCESSING else i
    finally:
        IS_PROCESSING = False

@app.post("/api/render")
async def start_render(req: RenderRequest, background_tasks: BackgroundTasks):
    if len(req.script) > MAX_CHAR_LIMIT:
        raise HTTPException(
            status_code=400,
            detail=f"Kịch bản quá dài ({len(req.script):,} ký tự). Tối đa {MAX_CHAR_LIMIT:,} ký tự (~60 phút audio)."
        )
    if not req.script.strip():
        raise HTTPException(status_code=400, detail="Kịch bản trống.")

    priority_level = 3
    if req.priority_token == "VKT_S1":
        priority_level = 1
    elif req.priority_token == "VKT_S2":
        priority_level = 2

    if priority_level == 3:
        guest_count = sum(1 for jid in JOB_QUEUE if JOB_STATUS.get(jid, {}).get("priority_level", 3) == 3)
        if guest_count >= 5:
            raise HTTPException(status_code=429, detail="Hệ thống đang quá tải, vui lòng xếp hàng thử lại sau vài phút!")

    job_id = f"nvj_{uuid.uuid4().hex[:8]}"
    
    JOB_STATUS[job_id] = {
        "status": "QUEUED",
        "progress": 0,
        "chunks_done": 0,
        "total_chunks": 0,
        "error": None,
        "zip_path": None,
        "script": req.script,
        "voice": req.voice,
        "rate": req.rate,
        "pitch": req.pitch,
        "volume": req.volume,
        "reverb": req.reverb,
        "echo": req.echo,
        "bass": req.bass,
        "env": req.env,
        "use_bgm": req.use_bgm,
        "custom_dict": req.custom_dict,
        "priority_level": priority_level
    }
    
    if not JOB_QUEUE:
        JOB_QUEUE.append(job_id)
    else:
        insert_idx = 1 if IS_PROCESSING else 0
        if priority_level == 1:
            while insert_idx < len(JOB_QUEUE) and JOB_STATUS.get(JOB_QUEUE[insert_idx], {}).get("priority_level", 3) <= 1:
                insert_idx += 1
            JOB_QUEUE.insert(insert_idx, job_id)
        elif priority_level == 2:
            while insert_idx < len(JOB_QUEUE) and JOB_STATUS.get(JOB_QUEUE[insert_idx], {}).get("priority_level", 3) <= 2:
                insert_idx += 1
            JOB_QUEUE.insert(insert_idx, job_id)
        else:
            JOB_QUEUE.append(job_id)

    queue_pos = JOB_QUEUE.index(job_id)
    if not IS_PROCESSING:
        queue_pos = 1
    JOB_STATUS[job_id]["queue_pos"] = queue_pos

    background_tasks.add_task(process_job_queue)

    return JSONResponse({
        "job_id": job_id,
        "queue_pos": queue_pos,
        "message": "Job đã được xếp hàng" if queue_pos > 1 else "Job đang bắt đầu xử lý"
    })

@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    job = JOB_STATUS.get(job_id)
    if not job:
        return JSONResponse({"status": "not_found"}, status_code=404)
    return JSONResponse({
        "job_id": job_id,
        "status": job["status"],
        "progress": job["progress"],
        "chunks_done": job["chunks_done"],
        "total_chunks": job["total_chunks"],
        "queue_pos": job.get("queue_pos", 0),
        "error": job.get("error"),
    })

@app.get("/api/audio/{job_id}")
async def get_audio(job_id: str):
    job = JOB_STATUS.get(job_id)
    if not job or job.get("status") != "DONE":
        return JSONResponse({"error": "Audio chưa sẵn sàng"}, status_code=404)
    zip_path = job.get("zip_path")
    if not zip_path:
        return JSONResponse({"error": "Không tìm thấy file zip"}, status_code=404)
    
    mp3_path = zip_path.replace(".zip", ".mp3")
    if not os.path.exists(mp3_path):
        return JSONResponse({"error": "Không tìm thấy file MP3"}, status_code=404)
        
    return FileResponse(mp3_path, media_type="audio/mpeg", headers={"Accept-Ranges": "bytes"})

@app.get("/api/download/{job_id}")
async def download_zip(job_id: str):
    job = JOB_STATUS.get(job_id)
    if not job or job["status"] != "DONE":
        return JSONResponse({"error": "Job chưa hoàn thành hoặc không tồn tại"}, status_code=404)
    zip_path = job.get("zip_path")
    if not zip_path or not os.path.exists(zip_path):
        return JSONResponse({"error": "File không tìm thấy trên server"}, status_code=404)
    return FileResponse(zip_path, filename=f"NarraVoice_{job_id}.zip", media_type="application/zip")

@app.get("/api/diagnostic/{job_id}")
async def download_diagnostic(job_id: str):
    diag_path = os.path.join(OUTPUT_DIR, f"{job_id}.diag.json")
    if os.path.exists(diag_path):
        return FileResponse(diag_path, filename=f"Error_{job_id}.json", media_type="application/json")
    return JSONResponse({"error": "Không có file chẩn đoán lỗi"}, status_code=404)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

# Trigger deploy

# v1.9.0-force
