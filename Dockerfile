FROM bitnami/python:3.8

ARG no_dev=--no-dev

COPY ./p* /app

RUN apt-get update && apt-get install -y cron \
    && curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python - \
    && export PATH=$PATH:$HOME/.poetry/bin \
    && poetry install ${no_dev}

COPY . /app
ENV PATH=$PATH:/root/.poetry/bin
ENV DAGSTER_HOME=/app

ENTRYPOINT poetry shell && dagit