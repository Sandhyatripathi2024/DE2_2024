FROM python:3.9
WORKDIR /app
COPY ./consumer.py consumer.py
RUN pip install --no-cache-dir redis psycopg2-binary
CMD ["python", "consumer.py"]
