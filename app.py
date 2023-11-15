from ariadne import ObjectType, QueryType, gql, graphql_sync, load_schema_from_path, make_executable_schema
from ariadne.asgi import GraphQL
from graphql_sync_dataloaders import DeferredExecutionContext, SyncDataLoader
import uvicorn
import json
from store import engine, Company, Acquisition, Employment, EntityLink, EntityType, EntityRelationship
from sqlalchemy.orm import Session
from sqlalchemy import select, insert

from store import DataStore

type_defs = load_schema_from_path("schema.graphql")

query = QueryType()
company = ObjectType("Company")
person_employment = ObjectType("PersonEmployment")
person = ObjectType("Person")

with open("company.json") as j, Session(engine) as session:
    for c in json.load(j):
        session.add(Company(
            id=c["company_id"], name=c["company_name"], headcount=c["headcount"] or 0))
    session.commit()

with open("acqusition.json") as j, Session(engine) as session:
    for a in json.load(j):
        acquisition = session.execute(insert(Acquisition).values(
            parent_company_id=a["parent_company_id"],
            acquired_company_id=a["acquired_company_id"],
            merged_into_parent_company=a["merged_into_parent_company"],
        ))
        session.add(EntityLink(
            left_id=a["parent_company_id"], left_type=EntityType.COMPANY,
            right_id=a["acquired_company_id"], right_type=EntityType.COMPANY,
            relationship_id=acquisition.inserted_primary_key.id,
            relationship_type=EntityRelationship.MERGED if a["merged_into_parent_company"]
            else EntityRelationship.ACQUIRED
        ))
    session.commit()

with open("person_employment.json") as j, Session(engine) as session:
    for e in json.load(j):
        previously_employed = "end_date" in e and e["end_date"] is not None
        employment = session.execute(insert(Employment).values(
            company_id=e["company_id"],
            person_id=e["person_id"],
            employment_title=e["employment_title"],
            start_date=e["start_date"] if "start_date" in e else None,
            end_date=e["end_date"] if previously_employed else None,
        ))
        session.add(EntityLink(
            left_id=e["person_id"], left_type=EntityType.PERSON,
            right_id=e["company_id"], right_type=EntityType.COMPANY,
            relationship_id=employment.inserted_primary_key.id,
            relationship_type=EntityRelationship.PREVIOUSLY_EMPLOYED_AT if previously_employed
            else EntityRelationship.CURRENTLY_EMPLOYED_AT
        ))
    session.commit()


@query.field("company")
def resolve_query_company(obj, info, company_id):
    return info.context["company_data_loader"].load(company_id)


@query.field("person")
def resolve_query_person(obj, info, person_id):
    return info.context["person_data_loader"].load(person_id)


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
def resolve_company_employees(obj, info, ex_company_ids):
    with Session(engine) as session:
        company_ids = [obj["company_id"]]
        if "acquired" in obj:
            company_ids.extend([sub["company_id"] for sub in obj["acquired"]])
        stmt = (
            select(EntityLink)
            .where(EntityLink.right_type.is_(EntityType.COMPANY))
            .where(EntityLink.right_id.in_(company_ids))
            .where(EntityLink.relationship_type.is_(EntityRelationship.CURRENTLY_EMPLOYED_AT))
        )
        if len(ex_company_ids) > 0:
            person_worked_in_ex_companies = (
                select(EntityLink.left_id)
                .where(EntityLink.right_type.is_(EntityType.COMPANY))
                .where(EntityLink.right_id.in_(ex_company_ids))
                .where(EntityLink.relationship_type.is_(EntityRelationship.PREVIOUSLY_EMPLOYED_AT))
                .distinct()
            )
            stmt = stmt.where(EntityLink.left_id.in_(
                person_worked_in_ex_companies))
        return [info.context["employment_data_loader"].load(e.relationship_id)
                for e in session.scalars(stmt)]


@person_employment.field("isCurrentlyEmployed")
def resolve_person_employment_is_currently_employed(obj, *_):
    return obj["end_date"] is None


@person_employment.field("company")
def resolve_person_employment_company(obj, info):
    return info.context["company_data_loader"].load(obj["company_id"])


@person_employment.field("person")
def resolve_person_employment_person(obj, info):
    return info.context["person_data_loader"].load(obj["person_id"])


@person.field("employment_history")
def resolve_person_employment_history(obj, info):
    with Session(engine) as session:
        stmt = (
            select(EntityLink)
            .where(EntityLink.left_type.is_(EntityType.PERSON))
            .where(EntityLink.left_id.is_(obj["person_id"]))
            .where(EntityLink.relationship_type.in_([
                EntityRelationship.CURRENTLY_EMPLOYED_AT, EntityRelationship.PREVIOUSLY_EMPLOYED_AT
            ]))
        )
        return [info.context["employment_data_loader"].load(e.relationship_id)
                for e in session.scalars(stmt)]


schema = make_executable_schema(
    type_defs, query, company, person_employment, person,
    convert_names_case=True,
)
store = DataStore(engine)
app = GraphQL(schema, debug=True, context_value={
    "company_data_loader": SyncDataLoader(store.get_company),
    "employment_data_loader": SyncDataLoader(store.get_employment),
    "person_data_loader": SyncDataLoader(store.get_person),
}, execution_context_class=DeferredExecutionContext)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
