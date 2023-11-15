from typing import Optional
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

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
