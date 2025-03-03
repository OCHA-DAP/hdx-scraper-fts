#!/usr/bin/python
"""
Unit tests for fts.

"""
import logging
from os.path import join

import pytest
from fts.download import FTSDownload
from fts.hapi_output import HAPIOutput
from fts.locations import Locations
from fts.main import FTS
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations as HDXLocations
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir

logger = logging.getLogger(__name__)


class TestFTS:
    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join("tests", "config",
                                     "project_configuration.yml"),
        )
        HDXLocations.set_validlocations(
            [
                {"name": "afg", "title": "Afghanistan"},
                {"name": "jor", "title": "Jordan"},
                {"name": "pse", "title": "occupied Palestinian territory"},
                {"name": "world", "title": "World"},
            ]
        )
        Country.countriesdata(False)
        Vocabulary._approved_vocabulary = {
            "tags": [
                {"name": "hxl"},
                {"name": "funding"},
                {"name": "covid-19"},
            ],
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }
        return Configuration.read()

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
                ftsdownloader = FTSDownload(configuration, downloader,
                                            testpath=True)
                notes = configuration["notes"]
                today = parse_date("2020-12-31")

                locations = Locations(ftsdownloader)
                logger.info(
                    f"Number of country datasets to upload: {len(locations.countries)}"
                )

                fts = FTS(ftsdownloader, locations, today, notes,
                          start_year=2019)
                (
                    dataset,
                    showcase,
                    hxl_resource,
                ) = fts.generate_dataset_and_showcase(folder,
                                                      locations.countries[0])
                assert dataset == {
                    "groups": [{"name": "afg"}],
                    "name": "fts-requirements-and-funding-data-for-afghanistan",
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
                    "name": "fts-requirements-and-funding-data-for-afghanistan-showcase",
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

                (
                    dataset,
                    showcase,
                    hxl_resource,
                ) = fts.generate_dataset_and_showcase(folder,
                                                      locations.countries[1])
                assert dataset == {
                    "groups": [{"name": "jor"}],
                    "name": "fts-requirements-and-funding-data-for-jordan",
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
                    "name": "fts-requirements-and-funding-data-for-jordan-showcase",
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

                (
                    dataset,
                    showcase,
                    hxl_resource,
                ) = fts.generate_dataset_and_showcase(folder,
                                                      locations.countries[2])
                assert dataset == {
                    "groups": [{"name": "pse"}],
                    "name": "fts-requirements-and-funding-data-for-occupied-palestinian-territory",
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
                    "name": "fts-requirements-and-funding-data-for-occupied-palestinian-territory-showcase",
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

                with HDXErrorHandler() as error_handler:
                    hapi_output = HAPIOutput(configuration, error_handler, fts.reqfund.global_rows, today, folder)
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
                            }],
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
