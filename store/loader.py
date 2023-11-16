
from typing import List
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from store.model import Company, Employment


class DataLoader():
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
