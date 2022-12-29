# world boss service
[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3100/)
[![Poetry](https://img.shields.io/badge/poetry-1.2.2-blue.svg)](https://python-poetry.org/docs/#installation)
[![Postgres](https://img.shields.io/badge/Postgres-13.7-blue.svg)](https://www.postgresql.org/ftp/source/v13.7/)
[![Redis](https://img.shields.io/badge/redis-7.0-blue.svg)](https://redis.io/download/)
[![codecov](https://codecov.io/gh/planetarium/world-boss-service/branch/main/graph/badge.svg?token=exiAaXgY2Z)](https://codecov.io/gh/planetarium/world-boss-service)

## Introduction
This repository provide world boss ranking service for Nine Chronicles

## Installation
- [awscli](https://aws.amazon.com/ko/cli/)
- [kms](https://aws.amazon.com/ko/kms/) key

## How to run
```commandline
$ git clone git@github.com:planetarium/world-boss-service.git
$ poetry install
$ createdb $dbname
$ poetry shell
$ flask --app world_boss/wsgi.py db upgrade --directory world_boss/migrations
$ flask --app world_boss/wsgi.py --debug run
```

### with worker
```commandline
$ celery -A world_boss.wsgi:cel worker -l debug
```

### testing
```commandline
$ pytest
```
