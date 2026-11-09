#!/usr/bin/python
"""
Unit tests for fts.

"""

import logging
from os.path import join

import pytest
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir

from hdx.scraper.fts.dataset_generator import DatasetGenerator
from hdx.scraper.fts.download import FTSDownload
from hdx.scraper.fts.hapi_output import HAPIOutput
from hdx.scraper.fts.locations import Locations
from hdx.scraper.fts.noncountry_funding import NonCountryFunding
from hdx.scraper.fts.pipeline import Pipeline
from hdx.scraper.fts.requirements_funding import RequirementsFunding

logger = logging.getLogger(__name__)


class TestFTS:
    @pytest.fixture(scope="function")
    def read_dataset(self, monkeypatch):
        def read_from_hdx(dataset_name):
            return Dataset.load_from_json(
                join(
                    "tests",
                    "fixtures",
                    "input",
                    f"dataset-{dataset_name}.json",
                )
            )

        monkeypatch.setattr(Dataset, "read_from_hdx", staticmethod(read_from_hdx))

    def test_generate_dataset_and_showcase(self, configuration, read_dataset):
        def check_resources(dsresources):
            for resource in dsresources:
                resource_name = resource["name"]
                expected_file = join("tests", "fixtures", resource_name)
                actual_file = join(folder, resource_name)
                assert_files_same(expected_file, actual_file)

        with (
            temp_dir("FTS-TEST", delete_on_failure=False) as folder,
            Download(user_agent="test") as downloader,
        ):
            ftsdownloader = FTSDownload(configuration, downloader, testpath=True)
            notes = configuration["notes"]
            today = parse_date("2020-12-31")

            locations = Locations(ftsdownloader)
            logger.info(
                f"Number of country datasets to upload: {len(locations.countries)}"
            )

            pipeline = Pipeline(
                configuration,
                ftsdownloader,
                folder,
                locations,
                today,
                start_year=2020,
            )
            dataset_generator = DatasetGenerator(today, notes, ("covid-19",))

            country = locations.countries[0]
            dataset, showcase = dataset_generator.get_country_dataset_and_showcase(
                country,
            )
            pipeline.generate_country_dataset_and_showcase(country, dataset)
            assert dataset == {
                "groups": [{"name": "afg"}],
                "name": "afg-requirements-and-funding-data",
                "title": "Afghanistan - Requirements and Funding Data",
                "tags": [
                    {
                        "name": "funding",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "humanitarian financial tracking service-fts",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "covid-19",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                ],
                "dataset_date": "[2016-03-31T00:00:00 TO 2020-12-31T23:59:59]",
                "data_update_frequency": "1",
                "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                "owner_org": "fb7c2910-6080-4b66-8b4f-0be9b6dc4d8e",
                "subnational": "0",
                "notes": notes,
            }
            resources = dataset.get_resources()
            assert resources == [
                {
                    "name": "fts_requirements_funding_afg.csv",
                    "description": "FTS Annual Requirements and Funding Data for Afghanistan",
                    "format": "csv",
                },
                {
                    "name": "fts_requirements_funding_covid_afg.csv",
                    "description": "FTS Annual Requirements, Funding and Covid Funding Data for Afghanistan",
                    "format": "csv",
                },
                {
                    "name": "fts_requirements_funding_cluster_afg.csv",
                    "description": "FTS Annual Requirements and Funding Data by Cluster for Afghanistan",
                    "format": "csv",
                },
                {
                    "name": "fts_requirements_funding_globalcluster_afg.csv",
                    "description": "FTS Annual Requirements and Funding Data by Global Cluster for Afghanistan",
                    "format": "csv",
                },
                {
                    "name": "fts_incoming_funding_afg.csv",
                    "description": "FTS Incoming Funding Data for Afghanistan for 2020",
                    "format": "csv",
                },
                {
                    "name": "fts_internal_funding_afg.csv",
                    "description": "FTS Internal Funding Data for Afghanistan for 2020",
                    "format": "csv",
                },
            ]
            check_resources(resources)
            assert showcase == {
                "image_url": "https://fts.unocha.org/themes/custom/fts_public/img/logos/fts-logo.svg",
                "name": "afg-requirements-and-funding-data-showcase",
                "notes": "Click the image to go to the FTS funding summary page for Afghanistan",
                "url": "https://fts.unocha.org/countries/1/flows/2020",
                "title": "FTS Afghanistan Summary Page",
                "tags": [
                    {
                        "name": "funding",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "humanitarian financial tracking service-fts",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "covid-19",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                ],
            }
            country = locations.countries[1]
            dataset, showcase = dataset_generator.get_country_dataset_and_showcase(
                country,
            )
            pipeline.generate_country_dataset_and_showcase(country, dataset)
            assert dataset == {
                "groups": [{"name": "jor"}],
                "name": "jor-requirements-and-funding-data",
                "title": "Jordan - Requirements and Funding Data",
                "tags": [
                    {
                        "name": "funding",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "humanitarian financial tracking service-fts",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "covid-19",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                ],
                "dataset_date": "[2017-04-05T00:00:00 TO 2020-12-31T23:59:59]",
                "data_update_frequency": "1",
                "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                "owner_org": "fb7c2910-6080-4b66-8b4f-0be9b6dc4d8e",
                "subnational": "0",
                "notes": notes,
            }

            resources = dataset.get_resources()
            assert resources == [
                {
                    "name": "fts_requirements_funding_jor.csv",
                    "description": "FTS Annual Requirements and Funding Data for Jordan",
                    "format": "csv",
                },
                {
                    "name": "fts_requirements_funding_covid_jor.csv",
                    "description": "FTS Annual Requirements, Funding and Covid Funding Data for Jordan",
                    "format": "csv",
                },
                {
                    "name": "fts_requirements_funding_cluster_jor.csv",
                    "description": "FTS Annual Requirements and Funding Data by Cluster for Jordan",
                    "format": "csv",
                },
                {
                    "name": "fts_requirements_funding_globalcluster_jor.csv",
                    "description": "FTS Annual Requirements and Funding Data by Global Cluster for Jordan",
                    "format": "csv",
                },
                {
                    "name": "fts_incoming_funding_jor.csv",
                    "description": "FTS Incoming Funding Data for Jordan for 2020",
                    "format": "csv",
                },
                {
                    "name": "fts_internal_funding_jor.csv",
                    "description": "FTS Internal Funding Data for Jordan for 2020",
                    "format": "csv",
                },
            ]
            check_resources(resources)
            assert showcase == {
                "image_url": "https://fts.unocha.org/themes/custom/fts_public/img/logos/fts-logo.svg",
                "name": "jor-requirements-and-funding-data-showcase",
                "notes": "Click the image to go to the FTS funding summary page for Jordan",
                "url": "https://fts.unocha.org/countries/114/flows/2020",
                "title": "FTS Jordan Summary Page",
                "tags": [
                    {
                        "name": "funding",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "humanitarian financial tracking service-fts",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "covid-19",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                ],
            }

            country = locations.countries[2]
            dataset, showcase = dataset_generator.get_country_dataset_and_showcase(
                country,
            )
            pipeline.generate_country_dataset_and_showcase(country, dataset)
            assert dataset == {
                "groups": [{"name": "pse"}],
                "name": "pse-requirements-and-funding-data",
                "title": "occupied Palestinian territory - Requirements and Funding Data",
                "tags": [
                    {
                        "name": "funding",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "humanitarian financial tracking service-fts",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "covid-19",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                ],
                "dataset_date": "[2016-02-26T00:00:00 TO 2020-12-31T23:59:59]",
                "data_update_frequency": "1",
                "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                "owner_org": "fb7c2910-6080-4b66-8b4f-0be9b6dc4d8e",
                "subnational": "0",
                "notes": notes,
            }

            resources = dataset.get_resources()
            assert resources == [
                {
                    "name": "fts_requirements_funding_pse.csv",
                    "description": "FTS Annual Requirements and Funding Data for occupied Palestinian territory",
                    "format": "csv",
                },
                {
                    "name": "fts_requirements_funding_covid_pse.csv",
                    "description": "FTS Annual Requirements, Funding and Covid Funding Data for occupied Palestinian territory",
                    "format": "csv",
                },
                {
                    "name": "fts_requirements_funding_cluster_pse.csv",
                    "description": "FTS Annual Requirements and Funding Data by Cluster for occupied Palestinian territory",
                    "format": "csv",
                },
                {
                    "name": "fts_requirements_funding_globalcluster_pse.csv",
                    "description": "FTS Annual Requirements and Funding Data by Global Cluster for occupied Palestinian territory",
                    "format": "csv",
                },
                {
                    "name": "fts_incoming_funding_pse.csv",
                    "description": "FTS Incoming Funding Data for occupied Palestinian territory for 2020",
                    "format": "csv",
                },
                {
                    "name": "fts_internal_funding_pse.csv",
                    "description": "FTS Internal Funding Data for occupied Palestinian territory for 2020",
                    "format": "csv",
                },
                {
                    "name": "fts_outgoing_funding_pse.csv",
                    "description": "FTS Outgoing Funding Data for occupied Palestinian territory for 2020",
                    "format": "csv",
                },
            ]
            check_resources(resources)
            assert showcase == {
                "image_url": "https://fts.unocha.org/themes/custom/fts_public/img/logos/fts-logo.svg",
                "name": "pse-requirements-and-funding-data-showcase",
                "notes": "Click the image to go to the FTS funding summary page for occupied Palestinian territory",
                "url": "https://fts.unocha.org/countries/171/flows/2020",
                "title": "FTS occupied Palestinian territory Summary Page",
                "tags": [
                    {
                        "name": "funding",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "humanitarian financial tracking service-fts",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "covid-19",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                ],
            }

            global_dataset = dataset_generator.get_global_dataset()
            global_results = pipeline.generate_global_dataset(global_dataset)
            assert global_dataset == {
                "data_update_frequency": "1",
                "dataset_date": "[2016-02-26T00:00:00 TO 2020-12-31T23:59:59]",
                "groups": [{"name": "world"}],
                "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                "name": "global-requirements-and-funding-data",
                "notes": "FTS publishes data on humanitarian funding flows as reported "
                "by donors and recipient organizations. It presents all humanitarian "
                "funding to a country and funding that is specifically reported or "
                "that can be specifically mapped against funding requirements stated "
                "in humanitarian response plans. The data comes from OCHA's "
                "[Financial Tracking Service](https://fts.unocha.org/) and is "
                "encoded as utf-8.",
                "owner_org": "fb7c2910-6080-4b66-8b4f-0be9b6dc4d8e",
                "subnational": "0",
                "tags": [
                    {
                        "name": "funding",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "humanitarian financial tracking service-fts",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "covid-19",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                ],
                "title": "Global - Requirements and Funding Data",
            }
            resources = global_dataset.get_resources()
            assert resources == [
                {
                    "description": "FTS Annual Requirements and Funding Data globally",
                    "format": "csv",
                    "name": "fts_requirements_funding_global.csv",
                },
                {
                    "description": "FTS Annual Requirements, Funding and Covid Funding Data "
                    "globally",
                    "format": "csv",
                    "name": "fts_requirements_funding_covid_global.csv",
                },
                {
                    "description": "FTS Annual Requirements and Funding Data by Cluster globally",
                    "format": "csv",
                    "name": "fts_requirements_funding_cluster_global.csv",
                },
                {
                    "description": "FTS Annual Requirements and Funding Data by Global Cluster "
                    "globally",
                    "format": "csv",
                    "name": "fts_requirements_funding_globalcluster_global.csv",
                },
                {
                    "description": "FTS Incoming Funding Data globally for 2020",
                    "format": "csv",
                    "name": "fts_incoming_funding_global.csv",
                },
                {
                    "description": "FTS Internal Funding Data globally for 2020",
                    "format": "csv",
                    "name": "fts_internal_funding_global.csv",
                },
                {
                    "description": "FTS Outgoing Funding Data globally for 2020",
                    "format": "csv",
                    "name": "fts_outgoing_funding_global.csv",
                },
            ]
            # Fixtures cover only AFG, JOR and PSE, so funding of multi-country plans
            # (e.g. 3RP) to other countries lands on "Multiple locations" plan rows and
            # the shared "Multiple locations" row goes negative
            assert len(global_results["rows"]) == 19
            for filename in (
                "fts_requirements_funding_global.csv",
                "fts_requirements_funding_covid_global.csv",
                "fts_requirements_funding_cluster_global.csv",
                "fts_requirements_funding_globalcluster_global.csv",
                "fts_incoming_funding_global.csv",
                "fts_internal_funding_global.csv",
                "fts_outgoing_funding_global.csv",
            ):
                assert_files_same(
                    join("tests", "fixtures", filename),
                    join(folder, filename),
                )

            global_results["dataset"]["id"] = "1234"
            global_results["resource"]["id"] = "5678"
            with HDXErrorHandler() as error_handler:
                hapi_output = HAPIOutput(
                    configuration,
                    error_handler,
                    global_results,
                    today,
                    folder,
                )
                dataset = hapi_output.generate_dataset()
                assert dataset == {
                    "name": "hdx-hapi-funding",
                    "title": "HDX HAPI - Coordination & Context: Funding",
                    "dataset_date": "[2020-01-01T00:00:00 TO 2020-12-31T23:59:59]",
                    "tags": [
                        {
                            "name": "funding",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "humanitarian financial tracking service-fts",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                    "groups": [{"name": "world"}],
                }

                resources = dataset.get_resources()
                assert resources[0] == {
                    "name": "Global Coordination & Context: Funding",
                    "description": "Funding data from HDX HAPI, please see [the documentation]"
                    "(https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/coordination_and_context/#funding) "
                    "for more information",
                    "format": "csv",
                }

                assert_files_same(
                    join("tests", "fixtures", "hdx_hapi_funding_global.csv"),
                    join(folder, "hdx_hapi_funding_global.csv"),
                )

    def test_country_without_plans(self):
        class StubDownloader:
            @staticmethod
            def download(partial_url):
                if partial_url.endswith("/2025"):
                    return [{"year": 2020, "totalFunding": 65000000}]
                return []

        reqfund = RequirementsFunding(
            StubDownloader(), "", None, set(), parse_date("2020-12-31"), 1999
        )
        country = {"id": 60, "iso3": "DNK", "name": "Denmark"}
        reqfund.add_country_global_rows({}, country)
        assert reqfund.get_global_rows() == [
            {
                "location": "Denmark",
                "locationType": "country",
                "countryCode": "DNK",
                "id": "",
                "name": "Not specified",
                "code": "",
                "typeId": "",
                "typeName": "",
                "isPartOfGHO": "N",
                "startDate": "",
                "endDate": "",
                "year": 2020,
                "requirements": "",
                "funding": 65000000,
                "percentFunded": "",
            }
        ]
        reqfund = RequirementsFunding(
            StubDownloader(), "", None, set(), parse_date("2010-12-31"), 1999
        )
        reqfund.add_country_global_rows({}, country)
        assert reqfund.get_global_rows() == []

    def test_plan_countries_by_location(self):
        # Plan with null countries: ZWE has funding, MOZ only requirements and
        # 30 goes to no location
        plan = {
            "id": 1,
            "name": "Plan",
            "code": "P1",
            "planType": {"id": 1, "name": "Other"},
            "startDate": "2025-01-01",
            "endDate": "2025-12-31",
            "countries": [],
            "countriesByLocation": True,
            "requirements": {"revisedRequirements": 100},
            "funding": {"totalFunding": 50},
        }

        class StubDownloader:
            @staticmethod
            def download(partial_url):
                return {
                    "requirements": {
                        "totalRevisedReqs": 100,
                        "objects": [
                            {
                                "id": "152",
                                "name": "Mozambique",
                                "revisedRequirements": 40,
                            },
                            {"id": 243, "name": "Zimbabwe", "revisedRequirements": 20},
                        ],
                    },
                    "report3": {
                        "fundingTotals": {
                            "objects": [
                                {
                                    "objectsBreakdown": [
                                        {
                                            "id": "243",
                                            "name": "Zimbabwe",
                                            "totalFunding": 20,
                                        },
                                        {"name": "Not specified", "totalFunding": 30},
                                    ]
                                }
                            ]
                        }
                    },
                }

        class StubLocations:
            countries = ()

            @staticmethod
            def get_countryid_from_object(object):
                countryid = object.get("id")
                return None if countryid is None else int(countryid)

            @staticmethod
            def get_id_to_iso3():
                return {152: "MOZ", 243: "ZWE"}

        reqfund = RequirementsFunding(
            StubDownloader(),
            "",
            StubLocations(),
            set(),
            parse_date("2025-12-31"),
            1999,
        )
        countries = plan["countries"]
        reqfund.add_country_requirements_funding(1, plan, countries)
        assert [(c["iso3"], c["requirements"], c["funding"]) for c in countries] == [
            ("MOZ", 40, None),
            ("ZWE", 20, 20),
        ]
        assert plan["noLocationFunding"] == 30

        noncountry = NonCountryFunding(None, StubLocations(), {2025: {1: plan}}, {})
        rows = noncountry.generate_location_rows(
            None, "Not specified", "unknown", 2025, 100
        )
        assert [(row["id"], row["funding"]) for row in rows] == [(1, 30), ("", 70)]
        country_rows = [
            {"id": 1, "requirements": c["requirements"], "funding": c["funding"]}
            for c in countries
        ]
        rows, funding = noncountry.generate_unattributed_rows(2025, country_rows + rows)
        assert funding == 0
        assert [(row["id"], row["requirements"], row["funding"]) for row in rows] == [
            (1, 40, "")
        ]

    def test_pipeline_plan_countries_by_location(self, configuration):
        # Plan 131 (2003) has null countries: Zimbabwe 1,243,781 funding, no
        # location 309,344,656 funding and all 464,103,581 requirements
        no_cluster = {
            "requirements": {"objects": []},
            "report3": {
                "fundingTotals": {
                    "objects": [
                        {"objectsBreakdown": [], "totalBreakdown": {"sharedFunding": 0}}
                    ]
                }
            },
        }
        responses = {
            "1/public/location": [{"id": 250, "iso3": "ZWE", "name": "Zimbabwe"}],
            "2/fts/flow/plan/overview/progress/2003": {
                "plans": [
                    {
                        "id": 131,
                        "name": "Humanitarian Crisis in Southern Africa - REGION (July 2003 - June 2004)",
                        "code": "R0304",
                        "customLocationCode": None,
                        "planType": {"id": 111, "name": "Regional response plan"},
                        "startDate": "2003-07-01",
                        "endDate": "2004-06-30",
                        "countries": None,
                        "requirements": {"revisedRequirements": 464103581},
                        "funding": {"totalFunding": 310588437, "progress": 66.92},
                    }
                ]
            },
            "1/fts/flow/custom-search?planid=131&groupby=location": {
                "requirements": {
                    "totalRevisedReqs": 464103581,
                    "objects": [
                        {"name": "Not specified", "revisedRequirements": 464103581}
                    ],
                },
                "report3": {
                    "fundingTotals": {
                        "objects": [
                            {
                                "objectsBreakdown": [
                                    {
                                        "name": "Not specified",
                                        "totalFunding": 309344656,
                                    },
                                    {
                                        "id": "250",
                                        "name": "Zimbabwe",
                                        "totalFunding": 1243781,
                                    },
                                ]
                            }
                        ]
                    }
                },
            },
            "1/fts/flow/custom-search?emergencyid=911&planid=&groupby=plan": {
                "report3": {"fundingTotals": {"objects": [{"objectsBreakdown": []}]}}
            },
            "1/fts/flow/custom-search?planid=131&groupby=cluster": no_cluster,
            "1/fts/flow/custom-search?planid=131&groupby=globalcluster": no_cluster,
            # 2002 is before the start year so is dropped
            "2/country/250/summary/trends/2008": [
                {"year": 2002, "totalFunding": 235047986},
                {"year": 2003, "totalFunding": 51546079},
            ],
            "1/fts/flow/custom-search?year=2003&groupby=location": {
                "report3": {
                    "fundingTotals": {
                        "total": -521065356,
                        "objects": [
                            {
                                "objectsBreakdown": [
                                    {
                                        "name": "Not specified",
                                        "totalFunding": -572611435,
                                    },
                                    {
                                        "id": "250",
                                        "name": "Zimbabwe",
                                        "totalFunding": 51546079,
                                    },
                                ]
                            }
                        ],
                    }
                }
            },
        }

        class StubDownloader:
            @staticmethod
            def download(partial_url):
                return responses[partial_url]

        locations = Locations(StubDownloader())
        today = parse_date("2003-12-31")
        pipeline = Pipeline(
            configuration, StubDownloader(), "", locations, today, start_year=2003
        )
        plan = pipeline._plans_by_id[131]
        assert plan["countries"] == [
            {
                "id": 250,
                "iso3": "ZWE",
                "name": "Zimbabwe",
                "adminlevel": 0,
                "requirements": None,
                "funding": 1243781,
            }
        ]
        assert plan["noLocationFunding"] == 309344656
        assert 131 not in pipeline._planidswithonelocation
        assert pipeline._plans_by_year_by_country == {"ZWE": {2003: [plan]}}

        country = locations.countries[0]
        plans_by_year = pipeline._plans_by_year_by_country["ZWE"]
        rows, years = pipeline._reqfund.generate_country_rows(
            plans_by_year, country, pipeline.call_others
        )
        assert years == [2003]
        assert [(row["id"], row["requirements"], row["funding"]) for row in rows] == [
            (131, "", 1243781),
            ("", "", 50302298),
        ]
        # Plan-wide cluster breakdown is not country-specific and there is no
        # COVID component
        for other in pipeline._others.values():
            assert other._rows == []

        pipeline.add_country_to_global(country)
        noncountry_funding = NonCountryFunding(
            pipeline._downloader,
            locations,
            pipeline._plans_by_year,
            pipeline._plans_by_id,
        )
        country_rows = pipeline._reqfund.get_global_rows()
        rows = noncountry_funding.generate_rows(country_rows)
        assert [
            (row["location"], row["id"], row["requirements"], row["funding"])
            for row in rows
        ] == [
            ("Not specified", 131, "", 309344656),
            ("Not specified", "", "", -881956091),
            ("Multiple locations", 131, 464103581, ""),
        ]
        total = sum(row["funding"] for row in country_rows + rows if row["funding"])
        assert total == -521065356

    def test_check_total(self, caplog):
        rows = [{"funding": 100}, {"funding": ""}, {"funding": -1}]
        with caplog.at_level(logging.WARNING):
            NonCountryFunding.check_total(2020, rows, 100)
            assert "differs" not in caplog.text
            NonCountryFunding.check_total(2020, rows, 200)
            assert "2020: total funding in global file 99 differs" in caplog.text
