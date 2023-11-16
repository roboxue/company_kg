import unittest
from graphql import graphql_sync
from graphql_sync_dataloaders import DeferredExecutionContext, SyncDataLoader

from sqlalchemy import Engine, create_engine
from app import generate_schema
from resolvers import Resolver
from store.loader import DataLoader

from store.model import Base


def empty_db():
    engine: Engine = create_engine("sqlite://", echo=True)
    Base.metadata.create_all(engine)
    return engine


def graphql_context(engine):
    loader = DataLoader(engine)
    return {
        "company_data_loader": SyncDataLoader(loader.get_company),
        "employment_data_loader": SyncDataLoader(loader.get_employment),
        "person_data_loader": SyncDataLoader(loader.get_person),
    }


LIST_COMPANY_TABLE_QUERY = """
query DebugCompanyTable {
    debugCompany {
        company_id
        company_name
    }
}"""

LIST_LINK_TABLE_QUERY = """
query DebugLinkTable {
    debugEntityLink {
    id
    left_id
    left_type
    right_id
    right_type
    relationship_id
    relationship_type
  }
}"""

INSERT_COMPANY_QUERY = """
mutation InitData($companies: [CompanyInput!]!) {
    addCompany(companies: $companies)
}"""

INSERT_ACQUISITION_QUERY = """
mutation InitData($acquisitions: [AcquisitionInput!]!) {
    addAquisition(acquisitions: $acquisitions)
}"""

INSERT_EMPLOYMENT_QUERY = """
mutation InitData($employments: [EmploymentInput!]!) {
    addEmployment(employments: $employments)
}"""

COMPANY_INFO_LOOKUP_QUERY = """
query CompanyLookup($companyId: Int!) {
    company(companyId: $companyId) {
        companyId
        companyName
        headcount
        acquiredBy {
            companyId
            companyName
        }
        acquired {
            companyId
            companyName
        }
    }
}"""

EMPLOYEE_LOOKUP_QUERY = """
query EmployeeLookup($companyId: Int!, $exCompanyIds: [Int!]!) {
  company(companyId: $companyId) {
    employees(exCompanyIds: $exCompanyIds) {
      person {
        person_id
        employment_history {
          company {
            companyName
          }
          isCurrentlyEmployed
        }
      }
      employmentTitle
      isCurrentlyEmployed
      startDate
      endDate
    }
	}
}"""


class TestResovler(unittest.TestCase):
    def test_acquisition_feature(self):
        engine = empty_db()
        resolver = Resolver(engine)
        schema = generate_schema(resolver)

        def _graphql(query_string, variable_values):
            return graphql_sync(schema, query_string,
                                variable_values=variable_values,
                                context_value=graphql_context(engine),
                                execution_context_class=DeferredExecutionContext)

        # Step 1: Add 5 companies
        r1 = _graphql(INSERT_COMPANY_QUERY, {
            "companies": [
                {"company_id": 1, "company_name": "Big Corp 1", "headcount": 10000},
                {"company_id": 2, "company_name": "Small Corp 2", "headcount": 2000},
                {"company_id": 3, "company_name": "Small Corp 3", "headcount": 300},
                {"company_id": 4, "company_name": "Startup 4", "headcount": 40},
                {"company_id": 5, "company_name": "Startup 5", "headcount": 5},
            ]
        })
        self.assertIsNone(r1.errors)

        # Step 2: Verify there are 5 companies in the DB
        r2 = _graphql(LIST_COMPANY_TABLE_QUERY, {})
        self.assertIsNone(r2.errors)
        self.assertEqual(len(r2.data["debugCompany"]), 5)

        # Step 3: Now "Small Corp 2" acquired "Startup 4"
        r3 = _graphql(INSERT_ACQUISITION_QUERY, {"acquisitions": [
            {"parent_company_id": 2, "acquired_company_id": 4,
             "merged_into_parent_company": True}
        ]})
        self.assertIsNone(r3.errors)

        # Step 4: Verify the KG acknowledged "Small Corp 2" acquired "Startup 4"
        small_corp_2 = _graphql(COMPANY_INFO_LOOKUP_QUERY, {"companyId": 2})
        self.assertIsNone(small_corp_2.errors)
        acquired = small_corp_2.data["company"]["acquired"]
        self.assertEqual(len(acquired), 1)
        self.assertListEqual([c["companyId"] for c in acquired], [4])

        # Step 5: Verify the KG acknowledged "Startup 4" acquired by "Small Corp 2"
        start_up_4 = _graphql(COMPANY_INFO_LOOKUP_QUERY, {"companyId": 4})
        self.assertIsNone(start_up_4.errors)
        acquired_by = start_up_4.data["company"]["acquiredBy"]
        self.assertEqual(acquired_by["companyId"], 2)

        # Step 6: Now "Small Corp 2" acquired "Startup 5"
        r5 = _graphql(INSERT_ACQUISITION_QUERY, {"acquisitions": [
            {"parent_company_id": 2, "acquired_company_id": 5,
             "merged_into_parent_company": False}
        ]})
        self.assertIsNone(r5.errors)

        # Step 7: Verify the KG acknowledged "Small Corp 2" acquired "Startup 5"
        small_corp_2 = _graphql(COMPANY_INFO_LOOKUP_QUERY, {"companyId": 2})
        self.assertIsNone(small_corp_2.errors)
        acquired = small_corp_2.data["company"]["acquired"]
        self.assertEqual(len(acquired), 2)
        self.assertListEqual([c["companyId"] for c in acquired], [4, 5])

        # Step 8: Verify the KG acknowledged "Startup 5" acquired by "Small Corp 2"
        start_up_5 = _graphql(COMPANY_INFO_LOOKUP_QUERY, {"companyId": 5})
        self.assertIsNone(start_up_5.errors)
        acquired_by = start_up_5.data["company"]["acquiredBy"]
        self.assertEqual(acquired_by["companyId"], 2)

        # Step 9: "Big Corp 1" acquired "Small Corp 2"
        r9 = _graphql(INSERT_ACQUISITION_QUERY, {"acquisitions": [
            {"parent_company_id": 1, "acquired_company_id": 2,
             "merged_into_parent_company": False}
        ]})
        self.assertIsNone(r9.errors)
        r9 = _graphql(LIST_LINK_TABLE_QUERY, {})
        self.assertIsNone(r9.errors)
        print(r9.data)
        big_corp_1 = _graphql(COMPANY_INFO_LOOKUP_QUERY, {"companyId": 1})
        self.assertIsNone(big_corp_1.errors)
        acquired = big_corp_1.data["company"]["acquired"]
        self.assertEqual(len(acquired), 3)
        self.assertListEqual([c["companyId"] for c in acquired], [2, 4, 5])

    def test_employee_search_feature(self):
        engine = empty_db()
        resolver = Resolver(engine)
        schema = generate_schema(resolver)

        def _graphql(query_string, variable_values):
            return graphql_sync(schema, query_string,
                                variable_values=variable_values,
                                context_value=graphql_context(engine),
                                execution_context_class=DeferredExecutionContext)

        # Step 1: Add 4 companies
        r1 = _graphql(INSERT_COMPANY_QUERY, {
            "companies": [
                {"company_id": 1, "company_name": "Big Corp 1", "headcount": 10000},
                {"company_id": 2, "company_name": "Small Corp 2", "headcount": 2000},
                {"company_id": 3, "company_name": "Small Corp 3", "headcount": 300},
                {"company_id": 4, "company_name": "Startup 4", "headcount": 40},
            ]
        })
        self.assertIsNone(r1.errors)

        # Step 2: 5 Person in total. (in chronological order)
        # Person1 worked for [1, 3, 1] (job fliping)
        # Person2 worked for [1]       (no job change)
        # Person3 worked for [3]       (will be included when acquisition (3->1) happened)
        # Person4 worked for [2, 1]    (will be included in "also worked at" query)
        # Person5 worked for [4]       (should not be included in searches)
        r2 = _graphql(INSERT_EMPLOYMENT_QUERY, {"employments": [
            {"person_id": 1, "company_id": 1, "employment_title": "SDE I",
                "start_date": "2020-01-01 00:00:00", "end_date": "2020-12-31 00:00:00"},
            {"person_id": 1, "company_id": 3, "employment_title": "SDE II",
                "start_date": "2021-01-01 00:00:00", "end_date": "2021-12-31 00:00:00"},
            {"person_id": 1, "company_id": 1, "employment_title": "SDE III",
                "start_date": "2022-01-01 00:00:00", "end_date": None},
            {"person_id": 2, "company_id": 1, "employment_title": "Manager",
                "start_date": "2022-01-01 00:00:00", "end_date": None},
            {"person_id": 3, "company_id": 3, "employment_title": "Director",
                "start_date": "2022-01-01 00:00:00", "end_date": None},
            {"person_id": 4, "company_id": 2, "employment_title": "Intern",
                "start_date": "2020-01-01 00:00:00", "end_date": "2020-12-31 00:00:00"},
            {"person_id": 4, "company_id": 1, "employment_title": "AE",
                "start_date": "2021-01-01 00:00:00", "end_date": None},
            {"person_id": 5, "company_id": 4, "employment_title": "CEO",
                "start_date": "2021-01-01 00:00:00", "end_date": None},
        ]})
        self.assertIsNone(r2.errors)

        # Step 3: Search Big Corp 1's current employee base
        employee_list_1 = _graphql(EMPLOYEE_LOOKUP_QUERY, {
                                   "companyId": 1, "exCompanyIds": []})
        self.assertIsNone(employee_list_1.errors)
        self.assertEquals(len(employee_list_1.data["company"]["employees"]), 3)
        # Verify person 1, 2, 4 are included
        self.assertListEqual([e["person"]["person_id"] for e in employee_list_1.data["company"]["employees"]],
                             [1, 2, 4])
        # Verify only person 4 are included when looking for employee who also worked at Small Corp 2
        employee_list_2 = _graphql(EMPLOYEE_LOOKUP_QUERY, {
                                   "companyId": 1, "exCompanyIds": [2]})
        self.assertIsNone(employee_list_2.errors)
        self.assertEquals(len(employee_list_2.data["company"]["employees"]), 1)
        self.assertListEqual([e["person"]["person_id"] for e in employee_list_2.data["company"]["employees"]],
                             [4])

        # Step 4: Small Corp 3 has acquired Startup 4
        r4 = _graphql(INSERT_ACQUISITION_QUERY, {"acquisitions": [
            {"parent_company_id": 3, "acquired_company_id": 4,
             "merged_into_parent_company": True}
        ]})
        self.assertIsNone(r4.errors)

        # Step 5: Big Corp 1 acquired Small Corp 3
        r5 = _graphql(INSERT_ACQUISITION_QUERY, {"acquisitions": [
            {"parent_company_id": 1, "acquired_company_id": 3,
             "merged_into_parent_company": True}
        ]})
        self.assertIsNone(r5.errors)

        # Step 6: Search Big Corp 1's current employee base again, now person 3, 5 should be included as well
        employee_list_3 = _graphql(EMPLOYEE_LOOKUP_QUERY, {
                                   "companyId": 1, "exCompanyIds": []})
        self.assertIsNone(employee_list_3.errors)
        self.assertEquals(len(employee_list_3.data["company"]["employees"]), 5)
        # Verify person 1, 2, 3, 4 are included
        self.assertListEqual([e["person"]["person_id"] for e in employee_list_3.data["company"]["employees"]],
                             [1, 2, 3, 4, 5])


if __name__ == '__main__':
    unittest.main()
