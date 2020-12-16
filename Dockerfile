FROM bitnami/python:3.8

COPY ./*.txt /app/

RUN apt-get update && apt-get install -yqq cron vim \
    && pip install -r requirements.txt

COPY . /app/
RUN chmod +x /app/entrypoint.sh && mkdir -p /app/data

ENV PYTHONPATH=$PYTHONPATH:/app
ENV DAGSTER_HOME=/app

EXPOSE 3000

ENTRYPOINT ["/app/entrypoint.sh"]
