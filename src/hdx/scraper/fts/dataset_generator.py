import logging

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from slugify import slugify

logger = logging.getLogger(__name__)


class DatasetGenerator:
    def __init__(self, today, description, additional_tags=()):
        self._today = today
        self._description = description
        self._additional_tags = additional_tags

    def get_dataset(
        self,
        countryiso3="world",
        countryname=None,
    ):
        if countryname:
            slug = countryiso3
        else:
            countryname = slug = "Global"
        logger.info(f"Adding {countryname} FTS data")
        slugified_name = slugify(f"{slug} - Requirements and Funding Data")
        title = f"{countryname} - Requirements and Funding Data"
        dataset = Dataset(
            {"name": slugified_name, "title": title, "notes": self._description}
        )
        if countryiso3 == "world":
            try:
                dataset.add_other_location(countryiso3)
            except HDXError as e:
                logger.error(f"{title} has a problem! {e}")
                return None, None
        else:
            try:
                dataset.add_country_location(countryiso3)
            except HDXError as e:
                logger.error(f"{title} has a problem! {e}")
                return None, None

        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("fb7c2910-6080-4b66-8b4f-0be9b6dc4d8e")
        dataset.set_expected_update_frequency("Every day")
        dataset.set_subnational(False)
        tags = ["funding", "humanitarian financial tracking service-fts"]
        tags.extend(self._additional_tags)
        dataset.add_tags(tags)
        return dataset, slugified_name

    def get_country_dataset_and_showcase(
        self,
        country,
    ):
        countryname = country["name"]
        if countryname == "World":
            logger.info(f"Ignoring {countryname}")
            return None, None
        countryiso3 = country["iso3"]
        if countryiso3 is None:
            logger.error(f"{countryname} has a problem! ISO3 is None!")
            return None, None
        dataset, slugified_name = self.get_dataset(countryiso3, countryname)

        showcase_url = (
            f"https://fts.unocha.org/countries/{country['id']}/flows/{self._today.year}"
        )
        showcase = Showcase(
            {
                "name": f"{slugified_name}-showcase",
                "title": f"FTS {countryname} Summary Page",
                "notes": f"Click the image to go to the FTS funding summary page for {countryname}",
                "url": showcase_url,
                "image_url": "https://fts.unocha.org/themes/custom/fts_public/img/logos/fts-logo.svg",
            }
        )
        showcase.add_tags(dataset.get_tags())
        return dataset, showcase

    def get_global_dataset(self):
        dataset, _ = self.get_dataset()
        return dataset
