from fuzzywuzzy import process
import pandas as pd


def fuzzy_merge(df_left, df_right, key_left, key_right, threshold=84, limit=1):
    """
    df_1: the left table to join
    df_2: the right table to join
    key_left: the key column of the left table
    key_right: the key column of the right table
    threshold: how close the matches should be to return a match, based on Levenshtein distance
    limit: the amount of matches that will get returned, these are sorted high to low
    """
    s = df_right.loc[:, key_right].tolist()
    m = df_left.loc[:, key_left].apply(lambda x: process.extract(x, s, limit=limit))
    new_df_left = df_left.assign(matches=m)
    m2 = new_df_left.loc[:, "matches"].apply(
        lambda x: ", ".join([i[0] for i in x if i[1] >= threshold])
    )
    new_df_left.loc[:, "matches"] = m2
    return new_df_left


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
