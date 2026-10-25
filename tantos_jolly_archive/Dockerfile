FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir fastapi uvicorn jinja2 tzdata

COPY rootfs /
RUN chmod a+x /run.sh

WORKDIR /app
CMD [ "/run.sh" ]
