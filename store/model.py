import enum
from typing import Optional
from sqlalchemy import Enum, Index, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


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

