FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY consul_aggregator/ ./consul_aggregator/
CMD ["python", "-m", "consul_aggregator"]