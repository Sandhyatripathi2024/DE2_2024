FROM python:3.9
WORKDIR /app
COPY ./producer.py producer.py
COPY ./TLC_data_pipeline.csv TLC_data_pipeline.csv
RUN pip install --no-cache-dir redis pandas
CMD ["python", "producer.py"]

