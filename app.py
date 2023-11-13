from ariadne import ObjectType, QueryType, gql, load_schema_from_path, make_executable_schema
from ariadne.asgi import GraphQL
import uvicorn
import json

type_defs = load_schema_from_path("schema.graphql")

query = QueryType()
company = ObjectType("Company")
person_employment = ObjectType("PersonEmployment")

data_company = {}
with open("company.json") as j:
    data_company_tmp = json.load(j)
    for c in data_company_tmp:
        data_company[c["company_id"]] = c

data_acquisition = []
with open("acqusition.json") as j:
    data_acquisition = json.load(j)

data_person_employment = []
with open("person_employment.json") as j:
    data_person_employment = json.load(j)


@query.field("company")
def resolve_query_company(*_, company_id):
    return data_company[company_id]


@company.field("acquiredBy")
def resolve_company_acquired_by(obj, *_):
    acquired_by = list(
        [a for a in data_acquisition if a["acquired_company_id"] == obj["company_id"]])
    if len(acquired_by) > 0:
        return resolve_query_company(company_id=acquired_by[0]["parent_company_id"])
    else:
        return None


@company.field("acquired")
def resolve_company_acquired(obj, *_):
    acquired = list(
        [a for a in data_acquisition if a["parent_company_id"] == obj["company_id"]])
    return [
        resolve_query_company(company_id=a["acquired_company_id"]) for a in acquired
    ]


@company.field("employees")
def resolve_company_employees(obj, *_, ex_company_ids):
    employees = [e for e in data_person_employment
                          if e["company_id"] == obj["company_id"] and
                          "end_date" in e and e["end_date"] is not None]
    return list([
        {
            "person": {
                "person_id": e["person_id"]
            },
            "company": {
                "company_id": e["company_id"]
            },
            "employment_title": e["employment_title"],
            "start_date": e["start_date"] if "start_date" in e else None,
            "end_date": e["end_date"] if "end_date" in e else None,
        } for e in employees
    ])


@person_employment.field("isCurrentlyEmployed")
def resolve_person_employment_is_currently_employed(obj, *_):
    return obj["end_date"] is None


@person_employment.field("company")
def resolve_person_employment_company(obj, *_):
    return resolve_query_company(company_id=obj["company"]["company_id"])


schema = make_executable_schema(
    type_defs, query, company, person_employment,
    convert_names_case=True,
)
app = GraphQL(schema, debug=True)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
