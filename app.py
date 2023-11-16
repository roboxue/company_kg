from ariadne import MutationType, ObjectType, QueryType, load_schema_from_path, make_executable_schema
from ariadne.asgi import GraphQL
from graphql_sync_dataloaders import DeferredExecutionContext, SyncDataLoader
from sqlalchemy import Engine, create_engine
import uvicorn
from resolvers import Resolver

from store import DataLoader
from store.model import Base


engine: Engine = create_engine("sqlite://", echo=True)
Base.metadata.create_all(engine)
loader = DataLoader(engine)

def generate_schema(resolver: Resolver):

    query = QueryType()
    resolver.resolve_query(query)

    mutation = MutationType()
    resolver.resovle_mutation(mutation)

    company = ObjectType("Company")
    resolver.resolve_company(company)

    person_employment = ObjectType("PersonEmployment")
    resolver.resolve_person_employment(person_employment)

    person = ObjectType("Person")
    resolver.resolve_person(person)

    type_defs = load_schema_from_path("schema.graphql")

    return make_executable_schema(
        type_defs, query, mutation, company, person_employment, person,
        convert_names_case=True,
    )

schema = generate_schema(Resolver(engine))

app = GraphQL(schema, debug=True, context_value={
    "company_data_loader": SyncDataLoader(loader.get_company),
    "employment_data_loader": SyncDataLoader(loader.get_employment),
    "person_data_loader": SyncDataLoader(loader.get_person),
}, execution_context_class=DeferredExecutionContext)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
