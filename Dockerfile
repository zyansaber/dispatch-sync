FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    unixodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# 这里安装 SAP HANA client
# 方式取决于你手上的安装包格式
# 安装完成后，HDBODBC 库需要能被 unixODBC 找到

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 app:app
