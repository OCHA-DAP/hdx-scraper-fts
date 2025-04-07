#!/usr/bin/python
"""
REGISTER:
---------

Caller script. Designed to call all other functions
that register datasets in HDX.

"""

import logging
from datetime import datetime
from os import getenv
from os.path import expanduser, join
from typing import Optional

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.scraper.fts._version import __version__
from hdx.scraper.fts.download import FTSDownload
from hdx.scraper.fts.hapi_output import HAPIOutput
from hdx.scraper.fts.locations import Locations
from hdx.scraper.fts.pipeline import Pipeline
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import progress_storing_tempdir, script_dir_plus_file

setup_logging()
logger = logging.getLogger(__name__)

lookup = "hdx-scraper-fts"


def main(
    today: str = "",
    countries: str = "",
    years: str = "",
    testfolder: str = "",
    err_to_hdx: Optional[bool] = None,
) -> None:
    """Generate dataset and create it in HDX

    Args:
        today (str): Date to use for today. Defaults to "".
        countries (str): Countries to run. Defaults to "".
        years (str): Years to run. Defaults to "".
        testfolder (str): Output test data to folder. Defaults to "".
        err_to_hdx (Optional[bool]): Whether to write any errors to HDX metadata. Defaults to None.
    """

    logger.info(f"##### {lookup} version {__version__} ####")
    if not User.check_current_user_organization_access(
        "fb7c2910-6080-4b66-8b4f-0be9b6dc4d8e", "create_dataset"
    ):
        raise PermissionError(
            "API Token does not give access to OCHA FTS organisation!"
        )
    if err_to_hdx is None:
        err_to_hdx = getenv("ERR_TO_HDX")
    with HDXErrorHandler(write_to_hdx=err_to_hdx) as error_handler:
        with Download(
            fail_on_missing_file=False,
            extra_params_yaml=join(expanduser("~"), ".extraparams.yaml"),
            extra_params_lookup=lookup,
            rate_limit={"calls": 1, "period": 1},
        ) as downloader:
            configuration = Configuration.read()
            ftsdownloader = FTSDownload(
                configuration,
                downloader,
                countryiso3s=countries,
                years=years,
                testfolder=testfolder,
            )
            notes = configuration["notes"]
            if today:
                today = parse_date(today)
            else:
                today = datetime.now()

            locations = Locations(ftsdownloader)
            logger.info(
                f"Number of country datasets to upload: {len(locations.countries)}"
            )

            pipeline = Pipeline(ftsdownloader, locations, today, notes)
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
                ) = pipeline.generate_dataset_and_showcase(folder, country)
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
                    if hxl_resource:
                        if "cluster" in hxl_resource["name"]:
                            dataset.generate_quickcharts(hxl_resource)
                        else:
                            dataset.generate_quickcharts(
                                hxl_resource, bites_disabled=[False, True, True]
                            )

                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)

            hapi_output = HAPIOutput(
                configuration,
                error_handler,
                pipeline.reqfund.global_rows,
                today,
                folder,
            )
            dataset = hapi_output.generate_dataset()
            dataset.update_from_yaml(
                path=join("config", "hdx_hapi_dataset_static.yaml")
            )
            dataset.create_in_hdx(
                remove_additional_resources=True,
                match_resource_order=False,
                hxl_update=False,
                updated_by_script="HDX Scraper: FTS",
            )


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
