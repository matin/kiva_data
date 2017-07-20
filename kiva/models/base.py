from sqlalchemy.ext.declarative import declarative_base

from kiva.db import metadata, Session


Base = declarative_base(metadata=metadata)
Base.query = Session.query_property()
