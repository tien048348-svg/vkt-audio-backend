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
# ========== C岷 H脤NH H峄?TH峄怤G ==========
VERSION = "1.8.0"
MAX_CHAR_LIMIT = 60000       # ~60 ph煤t audio
MAX_QUEUE_SIZE = 3           # T峄慽 膽a 3 job 膽ang ch峄?ch岷
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "jobs")
os.makedirs(OUTPUT_DIR, exist_ok=True)
# ========== TR岷燦G TH脕I TO脌N C峄 (In-Memory) ==========
JOB_STATUS: dict = {}   # job_id -> dict th么ng tin job
JOB_QUEUE: list = []    # H脿ng 膽峄 theo th峄?t峄?
IS_PROCESSING = False   # 膼ang c贸 job ch岷 kh么ng
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
    lang = locale.split("-")[0].lower()
    mapping = {
        "vi": "🇻🇳 Tiếng Việt", "en": "🇺🇸 Tiếng Anh", "zh": "🇨🇳 Tiếng Trung", 
        "ja": "🇯🇵 Tiếng Nhật", "ko": "🇰🇷 Tiếng Hàn", "ru": "🇷🇺 Tiếng Nga", 
        "fr": "🇫🇷 Tiếng Pháp", "de": "🇩🇪 Tiếng Đức", "es": "🇪🇸 Tiếng Tây Ban Nha", 
        "it": "🇮🇹 Tiếng Ý", "pt": "🇵🇹 Tiếng Bồ Đào Nha", "hi": "🇮🇳 Tiếng Ấn Độ",
        "th": "🇹🇭 Tiếng Thái", "id": "🇮🇩 Tiếng Indo", "ar": "🇸🇦 Tiếng Ả Rập",
        "ms": "🇲🇾 Tiếng Mã Lai", "tr": "🇹🇷 Tiếng Thổ Nhĩ Kỳ", "pl": "🇵🇱 Tiếng Ba Lan",
        "nl": "🇳🇱 Tiếng Hà Lan", "sv": "🇸🇪 Tiếng Thụy Điển", "fi": "🇫🇮 Tiếng Phần Lan",
        "mn": "🇲🇳 Tiếng Mông Cổ", "mr": "🇮🇳 Tiếng Marathi", "ne": "🇳🇵 Tiếng Nepal"
    }
    return mapping.get(lang, locale)
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
        gender = "Nữ" if v.get("Gender") == "Female" else "Nam"
        grouped[market].append({"id": v["ShortName"], "name": f"{name_part} ({gender})"})
    priority = ["馃嚮馃嚦 Ti岷縩g Vi峄噒 (Vi峄噒 Nam)", "馃寪 Ti岷縩g Anh (M峄?", "馃寪 Ti岷縩g Anh (Anh)"]
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
    text = req.text or "VKT xin ch脿o b岷. H么m nay, ch煤ng ta c霉ng l岷痭g nghe m峄檛 gi峄峮g n贸i r玫 r脿ng, t峄?nhi锚n v脿 gi脿u c岷 x煤c."
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
                    if JOB_STATUS[job_id]['status'] == 'CANCELLED':
                        raise Exception('CANCELLED')
                    JOB_STATUS[job_id]['progress'] = pct
                    JOB_STATUS[job_id]['chunks_done'] = done
                    JOB_STATUS[job_id]['total_chunks'] = total
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
            # C岷璸 nh岷璽 l岷 v峄?tr铆 h脿ng 膽峄 cho c谩c job c貌n l岷
            for i, jid in enumerate(JOB_QUEUE):
                if jid in JOB_STATUS:
                    JOB_STATUS[jid]["queue_pos"] = i + 1 if IS_PROCESSING else i
    finally:
        IS_PROCESSING = False
async def cleanup_old_files_task():
    import time
    while True:
        try:
            now = time.time()
            retention_seconds = 24 * 3600  # 24 gio
            if os.path.exists(OUTPUT_DIR):
                for filename in os.listdir(OUTPUT_DIR):
                    filepath = os.path.join(OUTPUT_DIR, filename)
                    if os.path.isfile(filepath):
                        file_mtime = os.path.getmtime(filepath)
                        if now - file_mtime > retention_seconds:
                            try:
                                os.remove(filepath)
                                print(f"[CLEANUP] Deleted old file: {filename}")
                            except:
                                pass
        except Exception as e:
            print(f"[CLEANUP ERROR] {e}")
        await asyncio.sleep(3600)  # Kiem tra moi 1 gio
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(cleanup_old_files_task())
@app.post("/api/render")
async def start_render(req: RenderRequest, background_tasks: BackgroundTasks):
    if len(req.script) > MAX_CHAR_LIMIT:
        raise HTTPException(
            status_code=400,
            detail=f"K峄媍h b岷 qu谩 d脿i ({len(req.script):,} k媒 t峄?. T峄慽 膽a {MAX_CHAR_LIMIT:,} k媒 t峄?(~60 ph煤t audio)."
        )
    if not req.script.strip():
        raise HTTPException(status_code=400, detail="K峄媍h b岷 tr峄憂g.")
    priority_level = 3
    if req.priority_token == "VKT_S1":
        priority_level = 1
    elif req.priority_token == "VKT_S2":
        priority_level = 2
    if priority_level == 3:
        guest_count = sum(1 for jid in JOB_QUEUE if JOB_STATUS.get(jid, {}).get("priority_level", 3) == 3)
        if guest_count >= 5:
            raise HTTPException(status_code=429, detail="H峄?th峄憂g 膽ang qu谩 t岷, vui l貌ng x岷縫 h脿ng th峄?l岷 sau v脿i ph煤t!")
    from datetime import datetime
    now_str = datetime.now().strftime("%d-%m-%Y_%Hh%Mm%Ss")
    job_id = f"VKT_{now_str}"
    
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
        "message": "Job 膽茫 膽瓢峄 x岷縫 h脿ng" if queue_pos > 1 else "Job 膽ang b岷痶 膽岷 x峄?l媒"
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
@app.post("/api/cancel/{job_id}")
async def cancel_job(job_id: str):
    job = JOB_STATUS.get(job_id)
    if not job:
        return JSONResponse({"status": "not_found"}, status_code=404)
    if job["status"] in ["QUEUED", "RUNNING"]:
        job["status"] = "CANCELLED"
        if job_id in JOB_QUEUE:
            JOB_QUEUE.remove(job_id)
    return JSONResponse({"status": "cancelled"})
@app.get("/api/audio/{job_id}")
async def get_audio(job_id: str):
    job = JOB_STATUS.get(job_id)
    if not job or job.get("status") != "DONE":
        return JSONResponse({"error": "Audio ch瓢a s岷祅 s脿ng"}, status_code=404)
    zip_path = job.get("zip_path")
    if not zip_path:
        return JSONResponse({"error": "Kh么ng t矛m th岷 file zip"}, status_code=404)
    
    mp3_path = zip_path.replace(".zip", ".mp3")
    if not os.path.exists(mp3_path):
        return JSONResponse({"error": "Kh么ng t矛m th岷 file MP3"}, status_code=404)
        
    return FileResponse(mp3_path, media_type="audio/mpeg", headers={"Accept-Ranges": "bytes"})
@app.get("/api/download/{job_id}")
async def download_zip(job_id: str):
    job = JOB_STATUS.get(job_id)
    if not job or job["status"] != "DONE":
        return JSONResponse({"error": "Job ch瓢a ho脿n th脿nh ho岷穋 kh么ng t峄搉 t岷"}, status_code=404)
    zip_path = job.get("zip_path")
    if not zip_path or not os.path.exists(zip_path):
        return JSONResponse({"error": "File kh么ng t矛m th岷 tr锚n server"}, status_code=404)
    return FileResponse(zip_path, filename=f"NarraVoice_{job_id}.zip", media_type="application/zip")
@app.get("/api/diagnostic/{job_id}")
async def download_diagnostic(job_id: str):
    diag_path = os.path.join(OUTPUT_DIR, f"{job_id}.diag.json")
    if os.path.exists(diag_path):
        return FileResponse(diag_path, filename=f"Error_{job_id}.json", media_type="application/json")
    return JSONResponse({"error": "Kh么ng c贸 file ch岷﹏ 膽o谩n l峄梚"}, status_code=404)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
# Trigger deploy
# v1.9.0-force


import json

@app.get("/api/preview_stream")
async def preview_audio_stream(
    text: str = "Xin chào",
    voice: str = "vi-VN-HoaiMyNeural",
    rate: int = 100,
    pitch: int = 0,
    volume: int = 100,
    reverb: int = 0,
    echo: int = 0,
    bass: int = 0,
    custom_dict: str = "{}"
):
    from fastapi.responses import StreamingResponse
    import asyncio
    from engine_v2 import apply_pronunciation_filter
    
    try:
        custom_dict_obj = json.loads(custom_dict)
    except:
        custom_dict_obj = {}
        
    filtered_text = apply_pronunciation_filter(text, voice, custom_dict_obj)
    
    async def generate():
        filters = []
        if rate != 100: filters.append(f"atempo={rate/100}")
        if pitch != 0: filters.append(f"asetrate=44100*{2**(pitch/12)},aresample=44100")
        if volume != 100: filters.append(f"volume={volume/100}")
        if echo > 0: filters.append(f"aecho=0.8:0.9:1000:{echo/100}")
        if reverb > 0: filters.append(f"reverb={reverb}")
        if bass > 0: filters.append(f"bass=g={bass}")
        
        filter_complex = ",".join(filters) if filters else "anull"
        
        edge_cmd = ["edge-tts", "--voice", voice, "--text", filtered_text]
        ffmpeg_cmd = ["ffmpeg", "-i", "pipe:0", "-af", filter_complex, "-ac", "2", "-ar", "44100", "-f", "mp3", "pipe:1"]
        
        p_edge = await asyncio.create_subprocess_exec(
            *edge_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )
        
        p_ffmpeg = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdin=p_edge.stdout,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )
        
        while True:
            chunk = await p_ffmpeg.stdout.read(4096)
            if not chunk:
                break
            yield chunk
            
        await p_edge.wait()
        await p_ffmpeg.wait()

    return StreamingResponse(generate(), media_type="audio/mpeg")
