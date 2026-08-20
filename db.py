import aiosqlite
import json
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "jobs.db")

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                status TEXT,
                progress INTEGER,
                turns_total INTEGER,
                turns_done INTEGER,
                download_url TEXT,
                error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

async def create_job(job_id: str, turns_total: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO jobs (id, status, progress, turns_total, turns_done) VALUES (?, ?, ?, ?, ?)",
            (job_id, "QUEUED", 0, turns_total, 0)
        )
        await db.commit()

async def update_job_progress(job_id: str, status: str, progress: int, turns_done: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE jobs SET status = ?, progress = ?, turns_done = ? WHERE id = ?",
            (status, progress, turns_done, job_id)
        )
        await db.commit()

async def complete_job(job_id: str, download_url: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE jobs SET status = ?, progress = ?, download_url = ? WHERE id = ?",
            ("DONE", 100, download_url, job_id)
        )
        await db.commit()

async def fail_job(job_id: str, error: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE jobs SET status = ?, error = ? WHERE id = ?",
            ("FAILED", error, job_id)
        )
        await db.commit()

async def get_job(job_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(row)
            return None
