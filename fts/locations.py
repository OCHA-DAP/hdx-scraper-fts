from hdx.location.country import Country


class Locations:
    def __init__(self, downloader):
        self.name_to_iso3 = dict()
        self.name_to_id = dict()
        self.id_to_iso3 = dict()
        countries = set()
        for country in downloader.download("1/public/location"):
            countryiso = country["iso3"]
            if countryiso is None:
                continue
            countryid = country["id"]
            countryname = country["name"]
            self.name_to_iso3[countryname] = countryiso
            self.name_to_id[countryname] = countryid
            self.id_to_iso3[countryid] = countryiso
            hdxcountryname = Country.get_country_name_from_iso3(countryiso)
            if hdxcountryname is None:
                continue
            countries.add((countryname, countryiso, countryid))
        self.countries = [
            {"id": country[2], "iso3": country[1], "name": country[0]}
            for country in sorted(countries)
        ]

    def get_countryid_from_object(self, object):
        countryid = object.get("id")
        if countryid is None:
            countryname = object.get("name")
            if countryname is not None:
                countryid = self.name_to_id.get(countryname)
        return countryid

    def get_countryiso_from_name(self, name):
        return self.name_to_iso3.get(name)
