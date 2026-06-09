# Build context is the repo root (see docker-compose.yml) so the orchestrator
# image can bake in the shared submission_templates/.
FROM python:3.12-slim
WORKDIR /app
COPY orchestrator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY orchestrator/app.py orchestrator/sandbox.py orchestrator/sandbox_k8s.py ./
COPY orchestrator/static ./static
# Per-language sandbox templates: the build context is uploaded to the host
# daemon via the mounted docker socket when a submission is containerized.
COPY submission_templates ./submission_templates
EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
