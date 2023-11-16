from sqlalchemy import create_engine, Engine
from .model import Base, Company, Acquisition, Employment, EntityLink, EntityType, EntityRelationship
from .loader import DataLoader
engine: Engine = create_engine("sqlite://", echo=True)
Base.metadata.create_all(engine)