# Sandbox template: Python submission.
# The orchestrator drops the contestant's source in as engine.py before building.
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir "websockets>=12,<13"
COPY engine.py /app/engine.py
ENV ENGINE_PORT=9000
EXPOSE 9000
# Unbuffered so logs stream; runs as non-root (added by orchestrator via --user is
# not portable, so we drop privileges here).
RUN useradd -m runner
USER runner
CMD ["python", "-u", "engine.py"]
