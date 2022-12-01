import os

from flask import Flask

from world_boss.app.api import api
from world_boss.app.orm import db, migrate


def create_app(db_uri: str = 'postgresql://postgres@localhost:5432/world-boss') -> Flask:
    flask_app = Flask(__name__)
    flask_app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get(
        'DATABASE_URL', db_uri)
    flask_app.register_blueprint(api)
    db.init_app(flask_app)
    migrate.init_app(flask_app, db)
    return flask_app


def index():
    return 'hello world'


app = create_app()
