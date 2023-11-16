from ariadne import MutationType, ObjectType, QueryType
from store import Company, Acquisition, Employment, EntityLink, EntityType, EntityRelationship
from sqlalchemy.orm import Session
from sqlalchemy import Engine, insert, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert


class Resolver:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def resolve_company(self, company: ObjectType):
        company.set_field("acquiredBy", self.resolve_company_acquired_by)
        company.set_field("acquired", self.resolve_company_acquired)
        company.set_field("employees", self.resolve_company_employees)

    def resolve_person(self, person: ObjectType):
        person.set_field("employment_history",
                         self.resolve_person_employment_history)

    def resolve_person_employment(self, person_employment: ObjectType):
        person_employment.set_field(
            "isCurrentlyEmployed", self.resolve_person_employment_is_currently_employed)
        person_employment.set_field(
            "company", self.resolve_person_employment_company)
        person_employment.set_field(
            "person", self.resolve_person_employment_person)

    def resolve_query(self, query: QueryType):
        query.set_field("debugCompany", self.resolve_debug_company)
        query.set_field("debugAquisition", self.resolve_debug_aquisition)
        query.set_field("debugEntityLink", self.resolve_debug_entity_link)
        query.set_field("company", self.resolve_query_company)
        query.set_field("person", self.resolve_query_person)

    def resovle_mutation(self, mutation: MutationType):
        mutation.set_field(
            "addEmployment", self.resolve_mutation_add_employment)
        mutation.set_field(
            "addAquisition", self.resolve_mutation_add_aquisition)
        mutation.set_field("addCompany", self.resolve_mutation_add_company)

    def resolve_company_acquired_by(self, obj, info):
        with Session(self.engine) as session:
            stmt = (
                select(EntityLink)
                .where(EntityLink.right_id.is_(obj["company_id"]))
                .where(EntityLink.right_type.is_(EntityType.COMPANY))
                .where(EntityLink.relationship_type.in_([
                    EntityRelationship.ACQUIRED, EntityRelationship.MERGED
                ]))
            )
            c = session.scalar(stmt)
            if c is None:
                return None
            return info.context["company_data_loader"].load(c.left_id)

    def resolve_company_acquired(self, obj, info):
        with Session(self.engine) as session:
            stmt = (
                select(EntityLink)
                .where(EntityLink.left_id.is_(obj["company_id"]))
                .where(EntityLink.left_type.is_(EntityType.COMPANY))
                .where(EntityLink.relationship_type.in_([
                    EntityRelationship.ACQUIRED, EntityRelationship.MERGED, EntityRelationship.INDIRECTLY_ACQUIRED
                ]))
            )
            return [info.context["company_data_loader"].load(c.right_id)
                    for c in session.scalars(stmt)]

    def resolve_company_employees(self, obj, info, ex_company_ids):
        with Session(self.engine) as session:
            company_ids = [obj["company_id"]]
            # Look for subsidaries
            stmt = (
                select(EntityLink.right_id)
                .where(EntityLink.left_id.is_(obj["company_id"]))
                .where(EntityLink.left_type.is_(EntityType.COMPANY))
                .where(EntityLink.relationship_type.in_([
                    EntityRelationship.ACQUIRED, EntityRelationship.MERGED, EntityRelationship.INDIRECTLY_ACQUIRED
                ]))
            )
            company_ids.extend(session.scalars(stmt).all())
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

    def resolve_person_employment_is_currently_employed(self, obj, *_):
        return obj["end_date"] is None

    def resolve_person_employment_company(self, obj, info):
        return info.context["company_data_loader"].load(obj["company_id"])

    def resolve_person_employment_person(self, obj, info):
        return info.context["person_data_loader"].load(obj["person_id"])

    def resolve_person_employment_history(self, obj, info):
        with Session(self.engine) as session:
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

    def resolve_query_company(self, obj, info, company_id):
        # For a new query, always clear cache
        info.context["company_data_loader"].clear(company_id)
        return info.context["company_data_loader"].load(company_id)

    def resolve_query_person(self, obj, info, person_id):
        # For a new query, always clear cache
        info.context["person_data_loader"].clear(person_id)
        return info.context["person_data_loader"].load(person_id)

    def resolve_debug_company(self, obj, info):
        with Session(self.engine) as session:
            return [{
                "company_id": r.id,
                "company_name": r.name,
                "headoucnt": r.headcount,
            } for r in session.scalars(select(Company)).all()]

    def resolve_debug_aquisition(self, obj, info):
        with Session(self.engine) as session:
            return [{
                "id": r.id,
                "parent_company_id": r.parent_company_id,
                "acquired_company_id": r.acquired_company_id,
                "merged_into_parent_company": r.merged_into_parent_company
            } for r in session.scalars(select(Acquisition)).all()]

    def resolve_debug_entity_link(self, obj, info):
        with Session(self.engine) as session:
            return [{
                "id": r.id,
                "left_id": r.left_id,
                "left_type": str(r.left_type),
                "right_id": r.right_id,
                "right_type": str(r.right_type),
                "relationship_id": r.relationship_id,
                "relationship_type": str(r.relationship_type),
            } for r in session.scalars(select(EntityLink)).all()]

    def resolve_mutation_add_company(self, obj, info, companies):
        try:
            with Session(self.engine) as session:
                stmt = sqlite_upsert(Company).values([{
                    "id": c["company_id"],
                    "name": c["company_name"],
                    "headcount": c["headcount"] or 0
                } for c in companies])
                stmt = stmt.on_conflict_do_update(
                    index_elements=[Company.id],
                    set_=dict(name=stmt.excluded.name, headcount=stmt.excluded.headcount))
                session.execute(stmt)
                session.commit()
            return "Done"
        except BaseException as e:
            return str(e)

    def resolve_mutation_add_aquisition(self, obj, info, acquisitions):
        try:
            with Session(self.engine) as session:
                for a in acquisitions:
                    stmt = insert(Acquisition).values(
                        parent_company_id=a["parent_company_id"],
                        acquired_company_id=a["acquired_company_id"],
                        merged_into_parent_company=a["merged_into_parent_company"])
                    acquisition = session.execute(stmt)
                    # Insert the acquisition record
                    session.add(EntityLink(
                        left_id=a["parent_company_id"], left_type=EntityType.COMPANY,
                        right_id=a["acquired_company_id"], right_type=EntityType.COMPANY,
                        relationship_id=acquisition.inserted_primary_key.id,
                        relationship_type=EntityRelationship.MERGED if a["merged_into_parent_company"]
                        else EntityRelationship.ACQUIRED
                    ))
                    # Check if the parent company has already been acquired
                    grandfather_id = a["parent_company_id"]
                    grandfather = session.scalar(select(EntityLink)
                                                 .where(EntityLink.right_id.is_(a["parent_company_id"]))
                                                 .where(EntityLink.right_type.is_(EntityType.COMPANY))
                                                 .where(EntityLink.relationship_type.in_([EntityRelationship.MERGED, EntityRelationship.ACQUIRED]))
                                                 )
                    if grandfather is not None:
                    # If the parent company has already been acquired, the current acquisitoin is indirectly acquired by grandfather
                        grandfather_id = grandfather.left_id
                        session.add(EntityLink(
                            left_id=grandfather_id, left_type=EntityType.COMPANY,
                            right_id=a["acquired_company_id"], right_type=EntityType.COMPANY,
                            relationship_id=acquisition.inserted_primary_key.id,
                            relationship_type=EntityRelationship.INDIRECTLY_ACQUIRED
                        ))
                    # The acquired companies's previous indirect acquisition are now indirectly linked to the parent company or grandfather
                    stmt = (
                        update(EntityLink)
                        .where(EntityLink.left_id.is_(a["acquired_company_id"]))
                        .where(EntityLink.left_type.is_(EntityType.COMPANY))
                        .where(EntityLink.relationship_type.in_([EntityRelationship.INDIRECTLY_ACQUIRED]))
                        .values(left_id=grandfather_id)
                    )
                    session.execute(stmt)
                    # The acquired companies's previous direct acquisition are now indirectly linked to the parent company or grandfather
                    for direct_acquisition in session.scalars(
                        select(EntityLink.right_id)
                        .where(EntityLink.left_id.is_(a["acquired_company_id"]))
                        .where(EntityLink.left_type.is_(EntityType.COMPANY))
                        .where(EntityLink.relationship_type.in_([EntityRelationship.MERGED, EntityRelationship.ACQUIRED]))
                    ):
                        session.add(EntityLink(
                            left_id=grandfather_id, left_type=EntityType.COMPANY,
                            right_id=direct_acquisition, right_type=EntityType.COMPANY,
                            relationship_id=acquisition.inserted_primary_key.id,
                            relationship_type=EntityRelationship.INDIRECTLY_ACQUIRED
                        ))
                session.commit()
            return "Done"
        except BaseException as e:
            return str(e)

    def resolve_mutation_add_employment(self, obj, info, employments):
        try:
            with Session(self.engine) as session:
                for e in employments:
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
            return "Done"
        except BaseException as e:
            return str(e)
