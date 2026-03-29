# Sử dụng image Python 3.11 bản mỏng nhẹ
FROM python:3.11-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Cập nhật pip để có phiên bản mới nhất, tránh các lỗi liên quan đến build dependency
RUN pip install --upgrade pip

# Copy file requirements và cài đặt dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ mã nguồn vào container
COPY . .

# Mở port 8000 cho FastAPI
EXPOSE 8000

# Lệnh khởi chạy ứng dụng
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
