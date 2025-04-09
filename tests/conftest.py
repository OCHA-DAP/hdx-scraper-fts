from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.useragent import UserAgent


@pytest.fixture(scope="session")
def fixtures_dir():
    return join("tests", "fixtures")


@pytest.fixture(scope="session")
def input_dir(fixtures_dir):
    return join(fixtures_dir, "input")


@pytest.fixture(scope="session")
def config_dir(fixtures_dir):
    return join("tests", "config")


@pytest.fixture(scope="session")
def configuration(config_dir):
    UserAgent.set_global("test")
    Configuration._create(
        hdx_read_only=True,
        hdx_site="prod",
        project_config_yaml=join(config_dir, "project_configuration.yaml"),
    )
    Locations.set_validlocations(
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
            {"name": tag}
            for tag in (
                "hxl",
                "funding",
                "covid-19",
            )
        ],
        "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
        "name": "approved",
    }
    return Configuration.read()
