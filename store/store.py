from typing import List, Optional
from sqlalchemy import Engine, String, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session


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


class Acquisition(Base):
    __tablename__ = "Acquisition"

    id: Mapped[int] = mapped_column(primary_key=True)
    parent_company_id: Mapped[int] = mapped_column()
    acquired_company_id: Mapped[int] = mapped_column()
    merged_into_parent_company: Mapped[bool] = mapped_column()

    def __repr__(self) -> str:
        return f"{self.__tablename__}(parent_company_id={self.parent_company_id!r}, " + \
            f"acquired_company_id={self.acquired_company_id!r}, merged_into_parent_company={self.merged_into_parent_company!r})"


class DataStore():
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def get_company(self, company_ids: List[int]) -> List[Company]:
        with Session(self.engine) as session:
            stmt = select(Company).where(Company.id.in_(company_ids))
            company_map = {c.id: {
                "company_id": c.id,
                "company_name": c.name,
                "headcount": c.headcount
            } for c in session.scalars(stmt)}
            return [company_map.get(id) for id in company_ids]
