# Base image Python 3.12 chính thức từ Docker Hub
FROM python:3.12-slim

# Thiết lập biến môi trường
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

# Cài đặt các gói hệ thống cần thiết (R-base cho rpy2, gfortran/blas/lapack cho numpy/scipy/semopy)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    r-base \
    r-base-dev \
    gfortran \
    libblas-dev \
    liblapack-dev \
    pkg-config \
    libgomp1 \
    curl \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy và cài đặt requirements trước để tối ưu layer cache
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir tavily-python && \
    playwright install chromium && \
    playwright install-deps chromium

# Copy toàn bộ mã nguồn
COPY . .

# Lệnh mặc định (sẽ được override bởi docker-compose)
CMD ["python", "main.py"]