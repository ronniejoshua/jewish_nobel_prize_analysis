from fuzzywuzzy import process
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd


# Scrapping the jinfo.org website for jewish nobel prize winners
def jewish_nobel_winners():
    sub_domains = [
        "Nobels_Chemistry",
        "Nobels_Economics",
        "Nobels_Literature",
        "Nobels_Medicine",
        "Nobels_Peace",
        "Nobels_Physics",
    ]

    jewish_list = list()
    for domain in sub_domains:
        response = requests.get(f"http://jinfo.org/{domain}.html")
        soup = BeautifulSoup(response.content, "html.parser")
        objs = soup.ul
        for obj in objs:
            try:
                res = [
                    re.findall("[a-zA-Z()]+\s", obj.text),
                    re.findall("\d{4}", obj.text),
                ]
                jewish_list.append(
                    {
                        "jinfo_laureate": "".join(res[0]).strip().replace(" )", ""),
                        "jinfo_award_year": res[1][0],
                        "jinfo_category": domain.replace("Nobels_", ""),
                    }
                )
            except (AttributeError, IndexError):
                continue
    return jewish_list


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


def fuzzy_merge(df_left, df_right, key_left, key_right, threshold=84, limit=1):
    """
    df_1: the left table to join
    df_2: the right table to join
    key_left: the key column of the left table
    key_right: the key column of the right table
    threshold: how close the matches should be to return a match, based on Levenshtein distance
    limit: the amount of matches that will get returned, these are sorted high to low
    """
    s = df_right[key_right].tolist()
    m = df_left[key_left].apply(lambda x: process.extract(x, s, limit=limit))
    df_left["matches"] = m
    m2 = df_left["matches"].apply(
        lambda x: ", ".join([i[0] for i in x if i[1] >= threshold])
    )
    df_left["matches"] = m2
    return df_left


def nobel_laureates_dataframe(df_left, df_right):
    domains = ["Economics", "Physics", "Chemistry", "Peace", "Medicine", "Literature"]

    df_dict = dict()
    for domain in domains:
        df_l_n = df_left[df_left["new_category"] == domain]
        df_r_j = df_right[df_right["jinfo_category"] == domain]
        df_dict[domain] = fuzzy_merge(df_l_n, df_r_j, "key_col", "key_col")

    bigdata = pd.concat(
        [
            df_dict["Economics"],
            df_dict["Physics"],
            df_dict["Chemistry"],
            df_dict["Peace"],
            df_dict["Medicine"],
            df_dict["Literature"],
        ],
        ignore_index=True,
        sort=False,
    )

    return bigdata










# Extract Data & Create Basic Data Frames
df_jew = pd.DataFrame(jewish_nobel_winners())
df_nobel = pd.DataFrame(nobel_api_laureates())


# Mapping Categories between two dataframes
dict_key = {
    "Economic Sciences": "Economics",
    "Physics": "Physics",
    "Chemistry": "Chemistry",
    "Peace": "Peace",
    "Physiology or Medicine": "Medicine",
    "Literature": "Literature",
}

# Create a new col
df_nobel["new_category"] = df_nobel["category"].map(dict_key)


# Creating key_col to join the two dataframes to findout jewish laureates
# we need to use a fuzzy match here
def convert_name(x):
    ans = str(x).lower().replace(" ", "")
    return ans.replace(".", "")


df_nobel["key_col"] = (
    df_nobel["knownName"].apply(lambda x: convert_name(x))
    + df_nobel["awardYear"].map(str)
    + df_nobel["new_category"].map(str).map(str.lower)
)


df_jew["key_col"] = (
    df_jew["jinfo_laureate"].map(str).map(str.lower).apply(lambda x: x.replace(" ", ""))
    + df_jew["jinfo_award_year"].map(str)
    + df_jew["jinfo_category"].map(str).map(str.lower)
)


# Create a df_matched - which combines the two dfs and does a fuzzy join
df_matched = nobel_laureates_dataframe(df_nobel, df_jew)
df_matched["check"] = df_matched["matches"].apply(lambda x: len(x))
df_jew_matched = df_matched[df_matched["check"] > 0]
df_jew_matched.shape
df_matched.to_csv("./data.csv")

