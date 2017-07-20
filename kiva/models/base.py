import os

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base


session = scoped_session(sessionmaker())
engine = create_engine(os.environ['DATABASE_URI'])
metadata = MetaData(bind=engine)
Base = declarative_base(metadata=metadata)
Base.query = session.query_property()
