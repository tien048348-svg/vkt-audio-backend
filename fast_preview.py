import os
import asyncio
import uuid
import edge_tts
import subprocess
from engine_v2 import chunk_text_for_tts

async def fast_preview(
    script: str, voice: str, rate: int, pitch: int, volume: int,
    reverb: int, echo: int, bass: int, output_dir: str, custom_dict: dict = None
) -> str:
    """
    Tạo audio nghe thử tốc độ cao.
    Chia văn bản thành các chunk nhỏ, chạy đa luồng, và nối file.
    Không tạo subtitle, không nén zip.
    """
    job_id = "preview_" + uuid.uuid4().hex[:8]
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        
    chunk_dir = os.path.join(output_dir, f"chunks_{job_id}")
    os.makedirs(chunk_dir, exist_ok=True)

    chunks = chunk_text_for_tts(script, 2000)
    if not chunks:
        chunks = ["Xin chào"]

    rate_str = f"+{rate}%" if rate >= 0 else f"{rate}%"
    pitch_str = f"+{pitch}Hz" if pitch >= 0 else f"{pitch}Hz"
    vol_str = f"+{volume}%" if volume >= 0 else f"{volume}%"

    sem = asyncio.Semaphore(5)
    chunk_files = []

    async def generate_chunk(idx, text):
        async with sem:
            cf = os.path.join(chunk_dir, f"{idx:04d}.mp3")
            communicate = edge_tts.Communicate(text, voice, rate=rate_str, pitch=pitch_str, volume=vol_str)
            await communicate.save(cf)
            return cf

    tasks = [generate_chunk(i, txt) for i, txt in enumerate(chunks)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, str):
            chunk_files.append(r)
            
    chunk_files.sort()

    raw_mp3_path = os.path.join(output_dir, f"{job_id}_raw.mp3")
    mp3_path = os.path.join(output_dir, f"{job_id}.mp3")

    if len(chunk_files) == 1:
        import shutil
        shutil.copy(chunk_files[0], raw_mp3_path)
    else:
        concat_list_path = os.path.join(chunk_dir, "concat_list.txt")
        with open(concat_list_path, "w", encoding="utf-8") as f:
            for cf in chunk_files:
                f.write(f"file '{cf.replace(chr(92), '/')}'\n")

        ffmpeg_exe = os.path.join(os.path.dirname(__file__), "ffmpeg.exe")
        ffmpeg_cmd = ffmpeg_exe if os.path.exists(ffmpeg_exe) else "ffmpeg"

        subprocess.run(
            [ffmpeg_cmd, "-y", "-f", "concat", "-safe", "0",
             "-i", concat_list_path, "-c", "copy", raw_mp3_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True
        )

    import shutil
    shutil.rmtree(chunk_dir, ignore_errors=True)

    # Audio filters
    filters = []
    if bass > 0:
        gain = (bass / 100) * 25
        filters.append(f"bass=g={gain}:f=100:w=0.5")
    if reverb > 0 or echo > 0:
        delay = int(50 + (echo / 100) * 550)
        decay = 0.2 + (max(reverb, echo) / 100) * 0.7
        filters.append(f"aecho=0.8:0.9:{delay}:{decay}")

    if len(filters) > 0:
        try:
            ffmpeg_exe = os.path.join(os.path.dirname(__file__), "ffmpeg.exe")
            ffmpeg_cmd = ffmpeg_exe if os.path.exists(ffmpeg_exe) else "ffmpeg"
            cmd = [ffmpeg_cmd, "-y", "-i", raw_mp3_path, "-af", ",".join(filters), mp3_path]
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        except Exception:
            shutil.copy(raw_mp3_path, mp3_path)
    else:
        shutil.copy(raw_mp3_path, mp3_path)

    if os.path.exists(raw_mp3_path):
        os.remove(raw_mp3_path)

    return mp3_path
