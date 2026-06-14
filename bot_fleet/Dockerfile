FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY fleet.py scenarios.py wire.py arena_pb2.py arena_pb2_grpc.py ./
CMD ["python", "-u", "fleet.py"]
