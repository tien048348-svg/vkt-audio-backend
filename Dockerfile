FROM python:3.10-slim

# Cài đặt FFmpeg lõi (Cần thiết cho hiệu ứng Vang, Bass)
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy thư viện và cài đặt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ mã nguồn Backend
COPY . .

# Tạo thư mục jobs
RUN mkdir -p /app/jobs

# Mở cổng 7860 (Chuẩn mặc định của Hugging Face Spaces)
EXPOSE 7860

# Khởi động Server Lõi
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7860"]
