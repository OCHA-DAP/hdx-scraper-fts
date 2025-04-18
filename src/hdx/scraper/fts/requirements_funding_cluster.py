import copy
import logging

from hdx.scraper.fts.helpers import hxl_names
from hdx.scraper.fts.resource_generator import ResourceGenerator
from hdx.utilities.downloader import DownloadError

logger = logging.getLogger(__name__)


class RequirementsFundingCluster(ResourceGenerator):
    def __init__(self, downloader, folder, planidswithonelocation, clusterlevel=""):
        super().__init__(downloader, folder, hxl_names)
        self._planidswithonelocation = planidswithonelocation
        self._clusterlevel = clusterlevel
        self._rows = []
        self._iso3_latestdata = {}
        self._iso3_latestpopulated = {}
        self._filename = f"fts_requirements_funding_{clusterlevel}cluster"
        self._description = "FTS Annual Requirements and Funding Data by Cluster"
        if clusterlevel:
            self._description = self._description.replace(
                "Cluster", f"{clusterlevel.capitalize()} Cluster"
            )

    def get_requirements_funding_plan(self, inrow):
        planid = inrow["id"]
        try:
            data = self._downloader.download(
                f"1/fts/flow/custom-search?planid={planid}&groupby={self._clusterlevel}cluster"
            )
        except DownloadError:
            logger.error(f"Problem with downloading cluster data for {planid}!")
            return None, None, None, None
        requirements_clusters = {}
        for reqobject in data["requirements"]["objects"]:
            requirements = reqobject.get("revisedRequirements")
            if requirements is not None:
                clusterid = reqobject.get("id")
                if clusterid is not None:
                    requirements_clusters[clusterid] = (reqobject["name"], requirements)
        funding_clusters = {}
        fund_objects = data["report3"]["fundingTotals"]["objects"]
        notspecified = None
        if len(fund_objects) == 0:
            logger.warning(f"{planid} has no funding objects!")
            shared = None
        else:
            for fundobject in fund_objects[0]["objectsBreakdown"]:
                funding = fundobject.get("totalFunding")
                if funding is not None:
                    clusterid = fundobject.get("id")
                    if clusterid is None or clusterid == "undefined":
                        notspecified = funding
                    else:
                        clusterid = int(clusterid)
                        funding_clusters[clusterid] = (fundobject["name"], funding)
            shared = fund_objects[0]["totalBreakdown"]["sharedFunding"]
        return requirements_clusters, funding_clusters, notspecified, shared

    @staticmethod
    def create_row(
        base_row, clusterid="", name="", requirements="", funding="", percentFunded=""
    ):
        row = copy.deepcopy(base_row)
        row["clusterCode"] = clusterid
        row["cluster"] = name
        row["requirements"] = requirements
        row["funding"] = funding
        row["percentFunded"] = percentFunded
        return row

    def generate_rows_requirements_funding(
        self, inrow, requirements_clusters, funding_clusters, notspecified, shared
    ):
        if requirements_clusters is None and funding_clusters is None:
            return
        planid = inrow["id"]
        if planid not in self._planidswithonelocation:
            return
        base_row = copy.deepcopy(inrow)
        del base_row["typeId"]
        del base_row["typeName"]
        del base_row["requirements"]
        del base_row["funding"]
        del base_row["percentFunded"]
        countryiso3 = base_row["countryCode"]
        year = base_row["year"]
        if year >= self._iso3_latestdata.get(countryiso3, year):
            self._iso3_latestdata[countryiso3] = year
        year = max(year, self._iso3_latestpopulated.get(countryiso3, year))
        subrows = []
        for clusterid, (fundname, funding) in funding_clusters.items():
            requirements_cluster = requirements_clusters.get(clusterid)
            if requirements_cluster is None:
                requirements = ""
            else:
                reqname, requirements = requirements_cluster
                if not fundname:
                    fundname = reqname
            row = self.create_row(base_row, clusterid, fundname, requirements, funding)
            if requirements and funding != "":
                row["percentFunded"] = int(funding / requirements * 100 + 0.5)
                self._iso3_latestpopulated[countryiso3] = year
            else:
                row["percentFunded"] = ""
            subrows.append(row)

        fundclusterids = list(funding_clusters.keys())
        for clusterid, (reqname, requirements) in requirements_clusters.items():
            if clusterid in fundclusterids:
                continue
            row = self.create_row(base_row, clusterid, reqname, requirements)
            subrows.append(row)

        self._rows.extend(sorted(subrows, key=lambda k: k["cluster"]))

        row = self.create_row(base_row, name="Not specified", funding=notspecified)
        self._rows.append(row)
        row = self.create_row(
            base_row, name="Multiple clusters/sectors (shared)", funding=shared
        )
        self._rows.append(row)

    def generate_plan_requirements_funding(self, inrow):
        (
            requirements_clusters,
            funding_clusters,
            notspecified,
            shared,
        ) = self.get_requirements_funding_plan(inrow)
        self.generate_rows_requirements_funding(
            inrow, requirements_clusters, funding_clusters, notspecified, shared
        )

    def generate_country_resource(self, dataset, country):
        if not self._rows:
            return None
        countryiso3 = country["iso3"]
        success, results = self.generate_resource(
            dataset, self._rows, countryiso3, countryname=country["name"]
        )
        self._global_rows[countryiso3] = self._rows
        self._rows = []
        if success:
            return results["resource"]
        else:
            return None

    def can_make_quickchart(self, countryiso3):
        latest_year = self._iso3_latestdata.get(countryiso3)
        if not latest_year:
            return False
        populated_year = self._iso3_latestpopulated.get(countryiso3)
        if not populated_year:
            return False
        if populated_year == latest_year:
            return True
        return False
