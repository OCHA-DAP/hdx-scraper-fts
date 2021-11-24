#!/usr/bin/python
"""
REGISTER:
---------

Caller script. Designed to call all other functions
that register datasets in HDX.

"""
import argparse
import logging
from datetime import datetime
from os.path import expanduser, join

from fts.download import FTSDownload
from fts.locations import Locations
from fts.main import FTS
from hdx.api.configuration import Configuration
from hdx.facades.simple import facade
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_tempdir

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-fts"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--today", default=None, help="Date to use for today")
    parser.add_argument("-c", "--countries", default=None, help="Countries to run")
    parser.add_argument("-y", "--years", default=None, help="Years to run")
    parser.add_argument(
        "-t", "--testfolder", default=None, help="Output test data to folder"
    )
    args = parser.parse_args()
    return args


def main():
    """Generate dataset and create it in HDX"""

    with Download(
        fail_on_missing_file=False,
        extra_params_yaml=join(expanduser("~"), ".extraparams.yml"),
        extra_params_lookup=lookup,
        rate_limit={"calls": 1, "period": 1},
    ) as downloader:
        args = parse_args()
        configuration = Configuration.read()
        ftsdownloader = FTSDownload(
            configuration,
            downloader,
            countryisos=args.countries,
            years=args.years,
            testfolder=args.testfolder,
        )
        notes = configuration["notes"]
        if args.today:
            today = parse_date(args.today)
        else:
            today = datetime.now()

        locations = Locations(ftsdownloader)
        logger.info(f"Number of country datasets to upload: {len(locations.countries)}")

        fts = FTS(ftsdownloader, locations, today, notes)
        for info, country in progress_storing_tempdir(
            "FTS", locations.countries, "iso3"
        ):
            folder = info["folder"]
            # for testing specific countries only
            #             if country['iso3'] not in ['AFG', 'JOR', 'TUR', 'PHL', 'SDN', 'PSE']:
            #                 continue
            (
                dataset,
                showcase,
                hxl_resource,
            ) = fts.generate_dataset_and_showcase(folder, country)
            if dataset is not None:
                dataset.update_from_yaml()
                if hxl_resource is None:
                    dataset.preview_off()
                else:
                    dataset.set_quickchart_resource(hxl_resource)
                dataset.create_in_hdx(
                    remove_additional_resources=True,
                    match_resource_order=True,
                    hxl_update=False,
                    updated_by_script="HDX Scraper: FTS",
                    batch=info["batch"],
                )
                if hxl_resource and "cluster" not in hxl_resource["name"]:
                    hxl_update = True
                else:
                    hxl_update = False
                if hxl_update:
                    dataset.hxl_update()
                elif hxl_resource:
                    dataset.generate_resource_view()
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
