FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingester.py metrics.py ./
CMD ["python", "-u", "ingester.py"]
