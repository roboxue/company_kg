from sqlalchemy import create_engine, Engine
from .store import Base, Company, Acquisition, Employment, DataStore
engine: Engine = create_engine("sqlite://", echo=True)
Base.metadata.create_all(engine)