FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 OMP_THREAD_LIMIT=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium xvfb pulseaudio pulseaudio-utils ffmpeg tesseract-ocr \
    tesseract-ocr-eng fonts-dejavu-core fonts-noto-color-emoji \
    ca-certificates libgl1-mesa-dri dbus-x11 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --create-home --uid 10001 recorder

COPY --chmod=644 bot_replay.py .

RUN mkdir -p /tmp/.X11-unix && chmod 1777 /tmp/.X11-unix

USER recorder

CMD ["python", "bot_replay.py"]
