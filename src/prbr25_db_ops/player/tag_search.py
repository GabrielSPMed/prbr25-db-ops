from pandas import DataFrame
from thefuzz.fuzz import token_set_ratio


def fuzzy_tag_search(player_df: DataFrame, tag: str) -> DataFrame:
    player_df["tag_match_score"] = player_df["tag"].apply(
        lambda x: token_set_ratio(x, tag)
    )
    return player_df
