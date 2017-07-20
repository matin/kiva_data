from sqlalchemy.ext.declarative import declarative_base

from kiva.db import metadata, session


Base = declarative_base(metadata=metadata)
Base.query = session.query_property()
