import pandas as pd
from data_extractor.nobel_api import nobel_api_laureates
from data_extractor.jinfo_data import jewish_nobel_winners
from data_extractor.utils import nobel_laureates_dataframe

if __name__ == "__main__":
    # Extract data
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

    # Create a new col - Mapping the "Nobel Categories"
    df_nobel.loc[:, "new_category"] = df_nobel["category"].map(dict_key)

    # Creating key_col to join the two dataframes to findout jewish laureates
    # fuzzy match is based on the name, hence we normalize the name

    def convert_name(x):
        ans = str(x).lower().replace(" ", "")
        return ans.replace(".", "")


    # Creating the Key Column {Fuzzy Match Based on Name, year & Category}
    # Note the use of () to chain the operations
    df_nobel.loc[:, "key_col"] = (
            df_nobel["knownName"].apply(lambda x: convert_name(x))
            + df_nobel["awardYear"].map(str)
            + df_nobel["new_category"].map(str).map(str.lower)
    )

    df_jew.loc[:, "key_col"] = (
            df_jew["jinfo_laureate"].map(str).map(str.lower).apply(lambda x: x.replace(" ", ""))
            + df_jew["jinfo_award_year"].map(str)
            + df_jew["jinfo_category"].map(str).map(str.lower)
    )

    # Create a df_matched - which combines the two dfs and does a fuzzy join
    df_matched = nobel_laureates_dataframe(df_nobel, df_jew)
    df_matched.loc[:, "check"] = df_matched["matches"].apply(lambda x: len(x))
    df_jew_matched = df_matched[df_matched["check"] > 0]
    print(df_jew_matched.shape)
    df_matched.to_csv("./data.csv")
