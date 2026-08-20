import asyncio
import edge_tts
import os
import json
from db import update_job_progress, complete_job, fail_job
import shutil

OUTPUT_DIR = "outputs"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

async def process_turn(turn_id: int, text: str, voice: str, job_dir: str):
    output_file = os.path.join(job_dir, f"chunk_{turn_id}.mp3")
    communicate = edge_tts.Communicate(text, voice)
    await communicate.save(output_file)
    return output_file

async def run_tts_job(job_id: str, turns_total: int, script_text: str):
    try:
        await update_job_progress(job_id, "RUNNING", 5, 0)
        
        job_dir = os.path.join(OUTPUT_DIR, job_id)
        os.makedirs(job_dir, exist_ok=True)

        lines = [line.strip() for line in script_text.split("\n\n") if line.strip()]
        
        concurrency_limit = 12
        semaphore = asyncio.Semaphore(concurrency_limit)
        
        turns_done = 0
        
        async def process_with_semaphore(turn_id: int, text: str):
            nonlocal turns_done
            async with semaphore:
                # Phân tích text để tìm giọng (giả lập đơn giản)
                voice = "vi-VN-HoaiMyNeural" if "Nữ" in text else "vi-VN-NamMinhNeural"
                clean_text = text.split(":", 1)[-1].strip() if ":" in text else text
                
                result = await process_turn(turn_id, clean_text, voice, job_dir)
                turns_done += 1
                progress = int(5 + (turns_done / turns_total) * 80)
                await update_job_progress(job_id, "RUNNING", progress, turns_done)
                return result

        tasks = [process_with_semaphore(i, lines[i]) for i in range(len(lines))]
        results = await asyncio.gather(*tasks)
        
        await update_job_progress(job_id, "MERGING", 90, turns_total)
        
        # Ghép audio (Giả lập đơn giản)
        final_mp3 = os.path.join(job_dir, f"final.mp3")
        if results:
            shutil.copy(results[0], final_mp3) 
            
        zip_path = f"{job_dir}.zip"
        shutil.make_archive(job_dir, 'zip', job_dir)
        
        download_url = f"/outputs/{job_id}.zip"
        await complete_job(job_id, download_url)
        
    except Exception as e:
        await fail_job(job_id, str(e))
