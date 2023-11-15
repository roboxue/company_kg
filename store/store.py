import enum
from typing import List, Optional
from sqlalchemy import Engine, Enum, Index, String, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session


class EntityType(enum.Enum):
    COMPANY = 1
    PERSON = 2


class EntityRelationship(enum.Enum):
    ACQUIRED = 1
    MERGED = 2
    CURRENTLY_EMPLOYED_AT = 3
    PREVIOUSLY_EMPLOYED_AT = 4


class Base(DeclarativeBase):
    pass


class Company(Base):
    __tablename__ = "Company"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    headcount: Mapped[int] = mapped_column()

    def __repr__(self) -> str:
        return f"{self.__tablename__}(id={self.id!r}, name={self.name!r}, headcount={self.headcount!r})"


class Employment(Base):
    __tablename__ = "Employment"

    id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[int] = mapped_column()
    person_id: Mapped[int] = mapped_column()
    employment_title: Mapped[str] = mapped_column()
    start_date: Mapped[Optional[str]] = mapped_column(String(19))
    end_date: Mapped[Optional[str]] = mapped_column(String(19))

    def __repr__(self) -> str:
        return f"{self.__tablename__}(company_id={self.company_id!r}, employment_title={self.employment_title}, " + \
            f"person_id={self.person_id!r}, start_date={self.start_date!r}, end_date={self.end_date!r})"

Index("unique_idx_employment_company_person_title_startdate", Employment.company_id, Employment.person_id, Employment.employment_title, Employment.start_date, unique=True)

class Acquisition(Base):
    __tablename__ = "Acquisition"

    id: Mapped[int] = mapped_column(primary_key=True)
    parent_company_id: Mapped[int] = mapped_column()
    acquired_company_id: Mapped[int] = mapped_column()
    merged_into_parent_company: Mapped[bool] = mapped_column()

    def __repr__(self) -> str:
        return f"{self.__tablename__}(parent_company_id={self.parent_company_id!r}, " + \
            f"acquired_company_id={self.acquired_company_id!r}, merged_into_parent_company={self.merged_into_parent_company!r})"

Index("unique_idx_parent_child", Acquisition.parent_company_id, Acquisition.acquired_company_id, unique=True)


class EntityLink(Base):
    __tablename__ = "EntityLink"
    id: Mapped[int] = mapped_column(primary_key=True)
    left_id: Mapped[int] = mapped_column()
    left_type: Mapped[EntityType] = mapped_column(Enum(EntityType))
    right_id: Mapped[int] = mapped_column()
    right_type: Mapped[EntityType] = mapped_column(Enum(EntityType))
    relationship_id: Mapped[int] = mapped_column()
    relationship_type: Mapped[EntityRelationship] = mapped_column(
        Enum(EntityRelationship))

    def __repr__(self) -> str:
        return f"{self.__tablename__}(left_id={self.left_id!r}, left_type={self.left_type!r}, " + \
            f"right_id={self.right_id!r}, right_type={self.right_type!r}, " +\
            f"relationship_id={self.relationship_id!r}, relationship_type={self.relationship_type!r})"


class DataStore():
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def get_company(self, ids: List[int]) -> List[dict]:
        with Session(self.engine) as session:
            stmt = select(Company).where(Company.id.in_(ids))
            result_map = {c.id: {
                "company_id": c.id,
                "company_name": c.name,
                "headcount": c.headcount
            } for c in session.scalars(stmt)}
            return [result_map.get(id) for id in ids]
        
    def get_employment(self, ids: List[int]) -> List[dict]:
        with Session(self.engine) as session:
            stmt = select(Employment).where(Employment.id.in_(ids))
            result_map = {e.id: {
                "person_id": e.person_id,
                "company_id": e.company_id,
                "employment_title": e.employment_title,
                "start_date": e.start_date,
                "end_date": e.end_date,
                } for e in session.scalars(stmt)}
            return [result_map.get(id) for id in ids]
        
    def get_person(self, ids: List[int]) -> List[dict]:
        return [{
            "person_id": id
        } for id in ids]
