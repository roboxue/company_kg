from ariadne import ObjectType, QueryType, gql, load_schema_from_path, make_executable_schema
from ariadne.asgi import GraphQL
import uvicorn
import json
from store import engine, Company, Acquisition, Employment
from sqlalchemy.orm import Session
from sqlalchemy import select

type_defs = load_schema_from_path("schema.graphql")

query = QueryType()
company = ObjectType("Company")
person_employment = ObjectType("PersonEmployment")

with open("company.json") as j, Session(engine) as session:
    for c in json.load(j):
        session.add(Company(
            id=c["company_id"], name=c["company_name"], headcount=c["headcount"] or 0))
    session.commit()

with open("acqusition.json") as j, Session(engine) as session:
    for a in json.load(j):
        session.add(Acquisition(parent_company_id=a["parent_company_id"],
                                acquired_company_id=a["acquired_company_id"],
                                merged_into_parent_company=a["merged_into_parent_company"]))
    session.commit()

with open("person_employment.json") as j, Session(engine) as session:
    for e in json.load(j):
        session.add(Employment(company_id=e["company_id"],
                               person_id=e["person_id"],
                               employment_title=e["employment_title"],
                               start_date=e["start_date"] if "start_date" in e else None,
                               end_date=e["end_date"] if "end_date" in e else None))
    session.commit()


@query.field("company")
def resolve_query_company(*_, company_id):
    with Session(engine) as session:
        stmt = select(Company).filter_by(id=company_id)
        c = session.scalar(stmt)
        if c is None:
            return None
        return {
            "company_id": c.id,
            "company_name": c.name,
            "headcount": c.headcount
        }


@company.field("acquiredBy")
def resolve_company_acquired_by(obj, *_):
    with Session(engine) as session:
        stmt = select(Acquisition).filter_by(
            acquired_company_id=obj["company_id"])
        c = session.scalar(stmt)
        if c is None:
            return None
        return resolve_query_company(company_id=c.parent_company_id)


@company.field("acquired")
def resolve_company_acquired(obj, *_):
    with Session(engine) as session:
        stmt = select(Acquisition).filter_by(
            parent_company_id=obj["company_id"])
        for c in session.scalars(stmt):
            yield resolve_query_company(company_id=c.acquired_company_id)


@company.field("employees")
def resolve_company_employees(obj, *_, ex_company_ids):
    with Session(engine) as session:
        stmt = select(Employment).filter_by(
            company_id=obj["company_id"], end_date=None)
        for e in session.scalars(stmt):
            yield {
                "person_id": e.person_id,
                "company_id": e.company_id,
                "employment_title": e.employment_title,
                "start_date": e.start_date,
                "end_date": e.end_date,
            }


@person_employment.field("isCurrentlyEmployed")
def resolve_person_employment_is_currently_employed(obj, *_):
    return obj["end_date"] is None


@person_employment.field("company")
def resolve_person_employment_company(obj, *_):
    return resolve_query_company(company_id=obj["company_id"])


schema = make_executable_schema(
    type_defs, query, company, person_employment,
    convert_names_case=True,
)
app = GraphQL(schema, debug=True)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
