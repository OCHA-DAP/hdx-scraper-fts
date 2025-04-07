import logging

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from slugify import slugify

logger = logging.getLogger(__name__)


class DatasetGenerator:
    def __init__(self, today, description):
        self.today = today
        self.description = description

    def get_dataset_and_showcase(
        self,
        country,
        additional_tags=list(),
    ):
        countryname = country["name"]
        if countryname == "World":
            logger.info(f"Ignoring {countryname}")
            return None, None
        title = f"{countryname} - Requirements and Funding Data"
        countryiso3 = country["iso3"]
        if countryiso3 is None:
            logger.error(f"{title} has a problem! Iso3 is None!")
            return None, None
        logger.info(f"Adding FTS data for {countryname}")
        slugified_name = slugify(
            f"FTS Requirements and Funding Data for {countryname}"
        ).lower()
        showcase_url = (
            f"https://fts.unocha.org/countries/{country['id']}/flows/{self.today.year}"
        )

        dataset = Dataset(
            {"name": slugified_name, "title": title, "notes": self.description}
        )
        try:
            dataset.add_country_location(countryiso3)
        except HDXError as e:
            logger.error(f"{title} has a problem! {e}")
            return None, None

        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("fb7c2910-6080-4b66-8b4f-0be9b6dc4d8e")
        dataset.set_expected_update_frequency("Every day")
        dataset.set_subnational(False)
        tags = ["hxl", "funding"]
        tags.extend(additional_tags)
        dataset.add_tags(tags)
        showcase = Showcase(
            {
                "name": f"{slugified_name}-showcase",
                "title": f"FTS {countryname} Summary Page",
                "notes": f"Click the image to go to the FTS funding summary page for {countryname}",
                "url": showcase_url,
                "image_url": "https://fts.unocha.org/themes/custom/fts_public/img/logos/fts-logo.svg",
            }
        )
        showcase.add_tags(tags)
        return dataset, showcase
