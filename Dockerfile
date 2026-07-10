FROM python:3.12-slim

WORKDIR /app

# Install system library required by LightGBM (OpenMP runtime)
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code, models, and data snapshot
COPY src/ ./src/
COPY models/ ./models/
COPY data/collected/ ./data/collected/

WORKDIR /app/src

CMD ["python", "-m", "bot.telegram_bot_handler"]
