from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from world_boss.app.config import config

SQLALCHEMY_DATABASE_URL = str(config.database_url)

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
