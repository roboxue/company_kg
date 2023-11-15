from sqlalchemy import create_engine
from .store import Base, Company, Acquisition, Employment
engine = create_engine("sqlite://", echo=True)
Base.metadata.create_all(engine)