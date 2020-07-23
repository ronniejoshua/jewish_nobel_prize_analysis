import requests


# Flattening out the json object for parsing nobel.org api endpoint response
def flatten_json(y):
    out = {}

    def flatten(x, name=""):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + "_")
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + "_")
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


# Json Response Parser
def extract_records(obj):
    obj_kn = flatten_json(obj.get("knownName"))
    obj_bR = flatten_json(obj.get("birth"))
    obj_nP = flatten_json(obj.get("nobelPrizes")[0])
    result = {
        "id": obj.get("id"),
        "knownName": obj_kn.get("en"),
        "gender": obj.get("gender"),
        "date": obj_bR.get("date"),
        "city": obj_bR.get("place_city_en"),
        "cityNow": obj_bR.get("place_cityNow_en"),
        "continent": obj_bR.get("place_continent_en"),
        "country": obj_bR.get("place_country_en"),
        "countryNow": obj_bR.get("place_countryNow_en"),
        "locationString": obj_bR.get("place_locationString_en"),
        "awardYear": obj_nP.get("awardYear"),
        "category": obj_nP.get("category_en"),
        "categoryFullName": obj_nP.get("categoryFullName_en"),
        "dateAwarded": obj_nP.get("dateAwarded"),
        "motivation": obj_nP.get("motivation_en"),
        "portion": obj_nP.get("portion"),
        "prizeAmount": obj_nP.get("prizeAmount"),
        "prizeAmountAdjusted": obj_nP.get("prizeAmountAdjusted"),
        "prizeStatus": obj_nP.get("prizeStatus"),
        "sortOrder": obj_nP.get("sortOrder"),
        "aff_city": obj_nP.get("affiliations_0_city_en"),
        "aff_cityNow": obj_nP.get("affiliations_0_cityNow_en"),
        "aff_country": obj_nP.get("affiliations_0_country_en"),
        "aff_countryNow": obj_nP.get("affiliations_0_countryNow_en"),
        "aff_locationString": obj_nP.get("affiliations_0_locationString_en"),
        "aff_name": obj_nP.get("affiliations_0_name_en"),
        "aff_nameNow": obj_nP.get("affiliations_0_nameNow_en"),
    }
    return result


def nobel_api_laureates():
    URL = "https://api.nobelprize.org/2.0/laureates"
    payload = {"limit": 1000, "offset": 0}
    response = requests.get(URL, params=payload)
    laureates = response.json().get("laureates")
    result_list = list()
    for laureate in laureates:
        result_list.append(extract_records(laureate))
    return result_list
