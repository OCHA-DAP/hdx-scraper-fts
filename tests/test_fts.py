#!/usr/bin/python
"""
Unit tests for fts.

"""

import logging
from os.path import join

import pytest
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.scraper.fts.dataset_generator import DatasetGenerator
from hdx.scraper.fts.download import FTSDownload
from hdx.scraper.fts.hapi_output import HAPIOutput
from hdx.scraper.fts.locations import Locations
from hdx.scraper.fts.pipeline import Pipeline
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir

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

        with temp_dir("FTS-TEST", delete_on_failure=False) as folder:
            with Download(user_agent="test") as downloader:
                ftsdownloader = FTSDownload(configuration, downloader, testpath=True)
                notes = configuration["notes"]
                today = parse_date("2020-12-31")

                locations = Locations(ftsdownloader)
                logger.info(
                    f"Number of country datasets to upload: {len(locations.countries)}"
                )

                pipeline = Pipeline(
                    ftsdownloader, folder, locations, today, start_year=2019
                )
                dataset_generator = DatasetGenerator(today, notes, ("covid-19",))

                country = locations.countries[0]
                dataset, showcase = dataset_generator.get_country_dataset_and_showcase(
                    country,
                )
                hxl_resource = pipeline.generate_country_dataset_and_showcase(
                    country, dataset
                )
                assert dataset == {
                    "groups": [{"name": "afg"}],
                    "name": "afg-requirements-and-funding-data",
                    "title": "Afghanistan - Requirements and Funding Data",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "funding",
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
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_requirements_funding_covid_afg.csv",
                        "description": "FTS Annual Requirements, Funding and Covid Funding Data for Afghanistan",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_requirements_funding_cluster_afg.csv",
                        "description": "FTS Annual Requirements and Funding Data by Cluster for Afghanistan",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_requirements_funding_globalcluster_afg.csv",
                        "description": "FTS Annual Requirements and Funding Data by Global Cluster for Afghanistan",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_incoming_funding_afg.csv",
                        "description": "FTS Incoming Funding Data for Afghanistan for 2020",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_internal_funding_afg.csv",
                        "description": "FTS Internal Funding Data for Afghanistan for 2020",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
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
                            "name": "hxl",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "funding",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "covid-19",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                }
                assert hxl_resource == resources[2]

                country = locations.countries[1]
                dataset, showcase = dataset_generator.get_country_dataset_and_showcase(
                    country,
                )
                hxl_resource = pipeline.generate_country_dataset_and_showcase(
                    country, dataset
                )
                assert dataset == {
                    "groups": [{"name": "jor"}],
                    "name": "jor-requirements-and-funding-data",
                    "title": "Jordan - Requirements and Funding Data",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "funding",
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
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_requirements_funding_covid_jor.csv",
                        "description": "FTS Annual Requirements, Funding and Covid Funding Data for Jordan",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_requirements_funding_cluster_jor.csv",
                        "description": "FTS Annual Requirements and Funding Data by Cluster for Jordan",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_requirements_funding_globalcluster_jor.csv",
                        "description": "FTS Annual Requirements and Funding Data by Global Cluster for Jordan",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_incoming_funding_jor.csv",
                        "description": "FTS Incoming Funding Data for Jordan for 2020",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_internal_funding_jor.csv",
                        "description": "FTS Internal Funding Data for Jordan for 2020",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
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
                            "name": "hxl",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "funding",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "covid-19",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                }
                assert hxl_resource == resources[2]

                country = locations.countries[2]
                dataset, showcase = dataset_generator.get_country_dataset_and_showcase(
                    country,
                )
                hxl_resource = pipeline.generate_country_dataset_and_showcase(
                    country, dataset
                )
                assert dataset == {
                    "groups": [{"name": "pse"}],
                    "name": "pse-requirements-and-funding-data",
                    "title": "occupied Palestinian territory - Requirements and Funding Data",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "funding",
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
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_requirements_funding_covid_pse.csv",
                        "description": "FTS Annual Requirements, Funding and Covid Funding Data for occupied Palestinian territory",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_requirements_funding_cluster_pse.csv",
                        "description": "FTS Annual Requirements and Funding Data by Cluster for occupied Palestinian territory",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_requirements_funding_globalcluster_pse.csv",
                        "description": "FTS Annual Requirements and Funding Data by Global Cluster for occupied Palestinian territory",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_incoming_funding_pse.csv",
                        "description": "FTS Incoming Funding Data for occupied Palestinian territory for 2020",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_internal_funding_pse.csv",
                        "description": "FTS Internal Funding Data for occupied Palestinian territory for 2020",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "fts_outgoing_funding_pse.csv",
                        "description": "FTS Outgoing Funding Data for occupied Palestinian territory for 2020",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
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
                            "name": "hxl",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "funding",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "covid-19",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                }
                assert hxl_resource == resources[2]

                global_dataset = dataset_generator.get_global_dataset()
                global_results = pipeline.generate_global_dataset(global_dataset)
                assert global_dataset == {
                    "data_update_frequency": "1",
                    "dataset_date": "[2016-02-26T00:00:00 TO 2020-12-31T23:59:59]",
                    "groups": [{"name": "world"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "global-requirements-and-funding-data",
                    "notes": "FTS publishes data on humanitarian funding flows as reported by "
                    "donors and recipient organizations. It presents all humanitarian "
                    "funding to a country and funding that is specifically reported or "
                    "that can be specifically mapped against funding requirements stated "
                    "in humanitarian response plans. The data comes from OCHA's "
                    "[Financial Tracking Service](https://fts.unocha.org/), is encoded "
                    "as utf-8 and the second row of the CSV contains "
                    "[HXL](http://hxlstandard.org) tags.",
                    "owner_org": "fb7c2910-6080-4b66-8b4f-0be9b6dc4d8e",
                    "subnational": "0",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "funding",
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
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "FTS Annual Requirements, Funding and Covid Funding Data "
                        "globally",
                        "format": "csv",
                        "name": "fts_requirements_funding_covid_global.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "FTS Annual Requirements and Funding Data by Cluster globally",
                        "format": "csv",
                        "name": "fts_requirements_funding_cluster_global.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "FTS Annual Requirements and Funding Data by Global Cluster "
                        "globally",
                        "format": "csv",
                        "name": "fts_requirements_funding_globalcluster_global.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "FTS Incoming Funding Data globally for 2020",
                        "format": "csv",
                        "name": "fts_incoming_funding_global.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "FTS Internal Funding Data globally for 2020",
                        "format": "csv",
                        "name": "fts_internal_funding_global.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "FTS Outgoing Funding Data globally for 2020",
                        "format": "csv",
                        "name": "fts_outgoing_funding_global.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]
                assert len(global_results["rows"]) == 9
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
                                "name": "hxl",
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
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }

                    assert_files_same(
                        join("tests", "fixtures", "hdx_hapi_funding_global.csv"),
                        join(folder, "hdx_hapi_funding_global.csv"),
                    )
