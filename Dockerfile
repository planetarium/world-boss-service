FROM python:3.10.8-bullseye

ARG POETRY_VERSION=1.2.2

RUN apt-get update
RUN apt-get install -y postgresql-client vim jq

# Set up poetry
RUN pip install -U pip "poetry==${POETRY_VERSION}"
RUN poetry config virtualenvs.create false

COPY ./world_boss /app/world_boss
COPY pyproject.toml /app
COPY poetry.lock /app

WORKDIR /app
RUN poetry install --no-root --no-dev
