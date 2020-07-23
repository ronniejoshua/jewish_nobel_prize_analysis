from bs4 import BeautifulSoup
import requests
import re


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
                    re.findall(r"[a-zA-Z()]+\s", obj.text),
                    re.findall(r"\d{4}", obj.text),
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
