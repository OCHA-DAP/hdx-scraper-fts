from hdx.location.country import Country


class Locations:
    def __init__(self, downloader):
        self._name_to_iso3 = {}
        self._name_to_id = {}
        self._id_to_iso3 = {}
        countries = set()
        for country in downloader.download("1/public/location"):
            countryiso3 = country["iso3"]
            if countryiso3 is None:
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
        return countryid

    def get_countryiso_from_name(self, name):
        return self._name_to_iso3.get(name)

    def get_id_to_iso3(self):
        return self._id_to_iso3
