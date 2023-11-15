from ariadne import ObjectType, QueryType, gql, graphql_sync, load_schema_from_path, make_executable_schema
from ariadne.asgi import GraphQL
from graphql_sync_dataloaders import DeferredExecutionContext, SyncDataLoader
import uvicorn
import json
from store import engine, Company, Acquisition, Employment
from sqlalchemy.orm import Session
from sqlalchemy import select

from store import DataStore

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
def resolve_query_company(obj, info, company_id):
    return info.context["company_data_loader"].load(company_id)

@company.field("acquiredBy")
def resolve_company_acquired_by(obj, info):
    with Session(engine) as session:
        stmt = select(Acquisition).filter_by(
            acquired_company_id=obj["company_id"])
        c = session.scalar(stmt)
        if c is None:
            return None
        return info.context["company_data_loader"].load(c.parent_company_id)


@company.field("acquired")
def resolve_company_acquired(obj, info):
    with Session(engine) as session:
        stmt = select(Acquisition).filter_by(
            parent_company_id=obj["company_id"])
        return [info.context["company_data_loader"].load(c.acquired_company_id) for c in session.scalars(stmt)]


@company.field("employees")
def resolve_company_employees(obj, *_, ex_company_ids):
    with Session(engine) as session:
        stmt = select(Employment).filter_by(
            company_id=obj["company_id"], end_date=None)
        return [{
                "person_id": e.person_id,
                "company_id": e.company_id,
                "employment_title": e.employment_title,
                "start_date": e.start_date,
                "end_date": e.end_date,
            } for e in session.scalars(stmt)]


@person_employment.field("isCurrentlyEmployed")
def resolve_person_employment_is_currently_employed(obj, *_):
    return obj["end_date"] is None


@person_employment.field("company")
def resolve_person_employment_company(obj, info):
    return info.context["company_data_loader"].load(obj["company_id"])


schema = make_executable_schema(
    type_defs, query, company, person_employment,
    convert_names_case=True,
)
store = DataStore(engine)
app = GraphQL(schema, debug=True, context_value={
    "company_data_loader": SyncDataLoader(store.get_company)
}, execution_context_class=DeferredExecutionContext)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
