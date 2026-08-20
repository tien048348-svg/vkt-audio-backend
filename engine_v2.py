"""
NARRAVOICE ENGINE v2 — BẢN HOÀN THIỆN TÍCH HỢP TƯ DUY NGƯỢC
Đã thêm: Bộ lọc phát âm (Dictionary), Chuẩn Font (BOM), và ZIP Packager.
"""
import asyncio
import json
import os
import re
import zipfile
from pathlib import Path
from dataclasses import dataclass
import edge_tts

# --- DICTIONARY: SMART CROSS-LINGUAL TRANSLITERATION ---
DICT_VI_VOICE = {
    # Brand & Tech
    r'\bCEO\b': 'xi i âu', r'\bAI\b': 'ây ai', r'\bIT\b': 'ai ti', r'\bVKT\b': 'vê ka tê',
    r'\bFacebook\b': 'phây búc', r'\bGoogle\b': 'gu gồ', r'\bYouTube\b': 'giu túp',
    r'\bTikTok\b': 'tích tóc', r'\bLivestream\b': 'lai sờ chim', r'\bFanpage\b': 'fan pết',
    r'\bApp\b': 'áp', r'\bWeb\b': 'oép', r'\bVideo\b': 'vi đê ô', r'\bWebsite\b': 'goép xai',
    r'\bInternet\b': 'in tơ nét', r'\bSmartphone\b': 'xờ mát phôn', r'\blaptop\b': 'láp tốp',
    # Marketing & Business
    r'\bMarketing\b': 'ma kít tinh', r'\bContent\b': 'còn ten', r'\bUpdate\b': 'ấp đết',
    r'\bReview\b': 'ri viu', r'\bFeedback\b': 'fít bách', r'\bSale\b': 'xêu',
    r'\bDeal\b': 'điu', r'\bHot\b': 'hót', r'\bTrend\b': 'tren', r'\bViral\b': 'vai rồ',
    r'\bView\b': 'viu', r'\bLike\b': 'lai', r'\bShare\b': 'se', r'\bComment\b': 'com men',
    r'\bInbox\b': 'in bóc', r'\bFollow\b': 'fo lô', r'\bHotgirl\b': 'hót gơn',
    # Geography & Famous Names (Tây Hoá)
    r'\bNew York\b': 'Niu Oóc', r'\bWashington\b': 'Oa-sinh-tơn', r'\bLondon\b': 'Luân Đôn',
    r'\bParis\b': 'Pa-ri', r'\bTokyo\b': 'Tô-ki-ô', r'\bSeoul\b': 'Xê-un', 
    r'\bLos Angeles\b': 'Lốt Ăng-giơ-lét', r'\bSydney\b': 'Xít-ni',
}

DICT_FOREIGN_VOICE = {
    # Food
    r'\bPhở\b': 'Fuh', r'\bBún bò\b': 'Boon baw', r'\bBánh mì\b': 'Bahn mee',
    r'\bBún chả\b': 'Boon cha', r'\bNem\b': 'Nehm', r'\bCà phê\b': 'Ca fay',
    # Common words
    r'\bXin chào\b': 'Sin chow', r'\bCảm ơn\b': 'Kahm uhn', r'\bTạm biệt\b': 'Tahm bee-et',
    r'\bViệt Nam\b': 'Viet Nahm', r'\bHà Nội\b': 'Hah Noy', r'\bSài Gòn\b': 'Sigh Gawn',
    r'\bÁo dài\b': 'Ow zai', r'\bNón lá\b': 'Nawn la',
    # Names
    r'\bNguyễn\b': 'Nwin', r'\bTrần\b': 'Chun', r'\bLê\b': 'Lay', r'\bPhạm\b': 'Fahm',
    r'\bVKT\b': 'Vee Kay Tee',
}

def apply_pronunciation_filter(text: str, voice: str, custom_dict: dict = None) -> str:
    if custom_dict is None: custom_dict = {}
    
    # 1. Trí tuệ cốt lõi: Chọn đúng từ điển theo quốc gia của Giọng đọc
    default_dict = DICT_VI_VOICE if "vi-" in voice.lower() else DICT_FOREIGN_VOICE
    
    for pattern, replacement in default_dict.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
        
    # 2. Xử lý Dictionary của riêng User (Ghi đè, ưu tiên cao nhất)
    for word, replacement in custom_dict.items():
        if not word.strip(): continue
        pattern = re.compile(re.escape(word), re.IGNORECASE)
        text = pattern.sub(replacement, text)
        
    return text

# --- CORE ENGINE ---
@dataclass
class WordTiming:
    word: str
    start_ms: int
    end_ms: int

def edge_val(v: int, suffix: str) -> str:
    return f"{int(v):+d}{suffix}"

def ms_to_srt_time(ms: int) -> str:
    h, r = divmod(ms, 3600000)
    m, r = divmod(r, 60000)
    s, ms2 = divmod(r, 1000)
    return f"{h:02}:{m:02}:{s:02},{ms2:03}"

async def synthesize_turn(text: str, voice: str, rate: int = 0, pitch: int = 0, volume: int = 0, custom_dict: dict = None) -> tuple[bytes, list[WordTiming]]:
    text_filtered = apply_pronunciation_filter(text, voice, custom_dict)
    
    for attempt in range(3):
        try:
            communicate = edge_tts.Communicate(
                text_filtered, voice,
                rate=edge_val(rate, "%"),
                pitch=edge_val(pitch, "Hz"),
                volume=edge_val(volume, "%")
            )
            
            audio_chunks = []
            timings = []
            
            async for chunk in communicate.stream():
                if chunk["type"] == "audio":
                    audio_chunks.append(chunk["data"])
                elif chunk["type"] == "SentenceBoundary":
                    start_ms = chunk["offset"] // 10000
                    dur_ms = chunk["duration"] // 10000
                    timings.append(WordTiming(word=chunk["text"], start_ms=start_ms, end_ms=start_ms + dur_ms))
            
            if audio_chunks:
                return b"".join(audio_chunks), timings
        except Exception as e:
            if attempt == 2: raise e
            await asyncio.sleep(1)

def chunk_text_for_tts(text: str, max_chars: int = 3000) -> list[str]:
    # Chia kịch bản dài thành các đoạn nhỏ dưới max_chars để tránh sập API Microsoft (Chịu tải 1 tiếng+)
    chunks = []
    paragraphs = text.split('\n')
    current_chunk = ""
    for p in paragraphs:
        p = p.strip()
        if not p: continue
        if len(current_chunk) + len(p) < max_chars:
            current_chunk += p + "\n"
        else:
            if current_chunk: chunks.append(current_chunk.strip())
            current_chunk = p + "\n"
    if current_chunk:
        chunks.append(current_chunk.strip())
    return chunks

def generate_srt(words: list[WordTiming]) -> str:
    lines = []
    for i, w in enumerate(words, 1):
        lines += [str(i), f"{ms_to_srt_time(w.start_ms)} --> {ms_to_srt_time(w.end_ms)}", w.word, ""]
    return "\n".join(lines)

def generate_ass(words: list[WordTiming]) -> str:
    header = "[Script Info]\nScriptType: v4.00+\nPlayResX: 1920\nPlayResY: 1080\n\n[V4+ Styles]\nFormat: Name,Fontname,Fontsize,PrimaryColour,Bold,Italic,BorderStyle,Shadow,Alignment,MarginL,MarginR,MarginV\nStyle: Default,Arial,72,&H00FFFFFF,1,0,1,1,2,100,100,60\n\n[Events]\nFormat: Layer,Start,End,Style,Name,MarginL,MarginR,MarginV,Effect,Text\n"
    events = []
    def ms2ass(ms):
        h, r = divmod(ms, 3600000); m, r = divmod(r, 60000); s, ms2 = divmod(r, 1000)
        return f"{h}:{m:02}:{s:02}.{ms2//10:02}"

    for i in range(0, len(words), 8):
        group = words[i:i+8]
        karaoke_text = "".join([f"{{\\k{(w.end_ms - w.start_ms)//10}}}{w.word} " for w in group])
        events.append(f"Dialogue: 0,{ms2ass(group[0].start_ms)},{ms2ass(group[-1].end_ms)},Default,,0,0,0,,{karaoke_text.strip()}")
    return header + "\n".join(events)

def generate_vtt(words: list[WordTiming]) -> str:
    lines = ["WEBVTT\n"]
    for i, w in enumerate(words, 1):
        start = ms_to_srt_time(w.start_ms).replace(',', '.')
        end = ms_to_srt_time(w.end_ms).replace(',', '.')
        lines += [f"{i}", f"{start} --> {end}", w.word, ""]
    return "\n".join(lines)

def generate_lrc(words: list[WordTiming]) -> str:
    lines = []
    for w in words:
        m, r = divmod(w.start_ms, 60000)
        s, ms2 = divmod(r, 1000)
        lines.append(f"[{m:02}:{s:02}.{ms2//10:02}]{w.word}")
    return "\n".join(lines)

def generate_json(words: list[WordTiming]) -> str:
    data = [{"text": w.word, "start_ms": w.start_ms, "end_ms": w.end_ms} for w in words]
    return json.dumps(data, ensure_ascii=False, indent=2)

async def process_full_job(script: str, voice: str, rate: int, pitch: int, volume: int, reverb: int, echo: int, bass: int, output_dir: str, job_id: str, custom_dict: dict = None):
    import subprocess
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Cơ chế Chunking
    text_chunks = chunk_text_for_tts(script, 2000)
    
    # --- TĂNG TỐC ĐỘ XỬ LÝ: CHẠY ĐA LUỒNG SONG SONG (CONCURRENCY) ---
    # Sử dụng Semaphore để giới hạn luồng (max 6 luồng cùng lúc), tránh bị Microsoft Block IP
    sem = asyncio.Semaphore(6)
    
    async def fetch_chunk(idx: int, chunk_text: str):
        async with sem:
            audio_bytes, timings = await synthesize_turn(chunk_text, voice, rate, pitch, volume, custom_dict)
            return idx, audio_bytes, timings

    # Kích hoạt toàn bộ các đoạn cắt chạy TẤN CÔNG ĐỒNG LOẠT lên server Edge
    tasks = [fetch_chunk(i, c) for i, c in enumerate(text_chunks)]
    results = await asyncio.gather(*tasks)
    
    # Sắp xếp lại cho đúng thứ tự phòng hờ
    results.sort(key=lambda x: x[0])
    
    all_audio_bytes = []
    all_timings = []
    current_offset_ms = 0
    
    # 2. Xử lý đồng bộ Subtitles (Toán học nối thời gian)
    for idx, audio_bytes, timings in results:
        all_audio_bytes.append(audio_bytes)
        
        for t in timings:
            all_timings.append(WordTiming(
                word=t.word, 
                start_ms=t.start_ms + current_offset_ms, 
                end_ms=t.end_ms + current_offset_ms
            ))
        
        if timings:
            current_offset_ms += (timings[-1].end_ms + 150) # Padding nối âm
    
    final_raw_bytes = b"".join(all_audio_bytes)
    
    # 3. Xử lý đường dẫn
    raw_mp3_path = os.path.join(output_dir, f"{job_id}_raw.mp3")
    mp3_path = os.path.join(output_dir, f"{job_id}.mp3")
    srt_path = os.path.join(output_dir, f"{job_id}.srt")
    ass_path = os.path.join(output_dir, f"{job_id}.ass")
    vtt_path = os.path.join(output_dir, f"{job_id}.vtt")
    lrc_path = os.path.join(output_dir, f"{job_id}.lrc")
    json_path = os.path.join(output_dir, f"{job_id}.json")
    zip_path = os.path.join(output_dir, f"{job_id}.zip")
    
    # Write RAW MP3
    with open(raw_mp3_path, "wb") as f: f.write(final_raw_bytes)
    
    # 4. Áp dụng hiệu ứng Vang, Vọng, Bass (Studio Mix)
    if reverb > 0 or echo > 0 or bass > 0:
        filters = []
        if bass > 0:
            gain = (bass / 100) * 25 
            filters.append(f"bass=g={gain}:f=100:w=0.5")
        if reverb > 0 or echo > 0:
            delay = int(50 + (echo / 100) * 550) 
            decay = 0.2 + (max(reverb, echo) / 100) * 0.7
            filters.append(f"aecho=0.8:0.9:{delay}:{decay}")
            
        try:
            ffmpeg_exe = os.path.join(os.path.dirname(__file__), "ffmpeg.exe")
            cmd = [ffmpeg_exe if os.path.exists(ffmpeg_exe) else 'ffmpeg', '-y', '-i', raw_mp3_path, '-af', ','.join(filters), mp3_path]
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        except (FileNotFoundError, subprocess.CalledProcessError):
            import shutil
            shutil.copy(raw_mp3_path, mp3_path)
    else:
        import shutil
        shutil.copy(raw_mp3_path, mp3_path)
        
    if os.path.exists(raw_mp3_path): os.remove(raw_mp3_path)
    
    # 5. Write All Subtitles với utf-8-sig (BOM)
    with open(srt_path, "w", encoding="utf-8-sig") as f: f.write(generate_srt(all_timings))
    with open(ass_path, "w", encoding="utf-8-sig") as f: f.write(generate_ass(all_timings))
    with open(vtt_path, "w", encoding="utf-8-sig") as f: f.write(generate_vtt(all_timings))
    with open(lrc_path, "w", encoding="utf-8-sig") as f: f.write(generate_lrc(all_timings))
    with open(json_path, "w", encoding="utf-8-sig") as f: f.write(generate_json(all_timings))
    
    # 6. ZIP đóng gói
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(mp3_path, arcname=f"{job_id}.mp3")
        zipf.write(srt_path, arcname=f"{job_id}.srt")
        zipf.write(ass_path, arcname=f"{job_id}.ass")
        zipf.write(vtt_path, arcname=f"{job_id}.vtt")
        zipf.write(lrc_path, arcname=f"{job_id}.lrc")
        zipf.write(json_path, arcname=f"{job_id}.json")
        
    return zip_path
