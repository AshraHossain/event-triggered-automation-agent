FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY etaa ./etaa
COPY examples ./examples
RUN pip install --no-cache-dir .

# Run as an unprivileged user; /app/data holds run history and dead letters.
RUN useradd --create-home --uid 10001 etaa \
    && mkdir -p /app/data \
    && chown -R etaa:etaa /app
USER etaa

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s CMD \
    python -c "import urllib.request as u; u.urlopen('http://127.0.0.1:8000/api/v1/health')"

CMD ["python", "-m", "etaa.main", "server", "--host", "0.0.0.0", "--port", "8000", "--workflows", "examples/"]
