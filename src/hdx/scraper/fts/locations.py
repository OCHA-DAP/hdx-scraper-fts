from hdx.location.country import Country


class Locations:
    def __init__(self, downloader):
        self._name_to_iso3 = {}
        self._name_to_id = {}
        self._id_to_iso3 = {}
        self._noncountry_names = set()
        countries = set()
        for country in downloader.download("1/public/location"):
            countryiso3 = country["iso3"]
            if countryiso3 is None:
                # Global and regions
                self._noncountry_names.add(country["name"])
                continue
            countryid = country["id"]
            countryname = country["name"]
            self._name_to_iso3[countryname] = countryiso3
            self._name_to_id[countryname] = countryid
            self._id_to_iso3[countryid] = countryiso3
            hdxcountryname = Country.get_country_name_from_iso3(countryiso3)
            if hdxcountryname is None:
                continue
            countries.add((countryname, countryiso3, countryid))
        self.countries = [
            {"id": country[2], "iso3": country[1], "name": country[0]}
            for country in sorted(countries)
        ]

    def get_countryid_from_object(self, object):
        countryid = object.get("id")
        if countryid is None:
            countryname = object.get("name")
            if countryname is not None:
                countryid = self._name_to_id.get(countryname)
        if countryid is None:
            return None
        return int(countryid)

    def get_iso3_or_noncountry_name(self, name):
        """ISO3 for a country, the name for Global or a region, else None"""
        iso3 = self._name_to_iso3.get(name)
        if iso3:
            return iso3
        if name in self._noncountry_names:
            return name
        return None

    def get_id_to_iso3(self):
        return self._id_to_iso3
