import math
from functools import reduce
from typing import List, Dict

import pandas as pd
import toolz as t
from pandas import DataFrame
import math

def check_group(group):
    if group is None or group in ["NaN", "nan", "Unknown", "unknown"] or (isinstance(group, float) and math.isnan(group)):
        return None
    return group


def convert_all_groups(df):

    group1_valid = df['group_name'].apply(check_group)
    group2_valid = df['group_name2'].apply(check_group)
    valid_groups = pd.concat([group1_valid, group2_valid]).dropna().tolist()
    return valid_groups


def convert_to_event_model(row):
    groups = [check_group(row["group_name"]), check_group(row["group_name2"])]


    return {
        "eventid": row["eventid"],
        "year": row["year"],
        "month": row["month"],
        "day": row["day"],
        "city": row["city"],
        "groups": [group for group in groups if group],
        "attack_type": row["attacktype1_txt"],
        "target_type": row["target_type"],
    }


def create_model_location(pda: DataFrame):
    locations = []
    for _, df in pda.iterrows():
        if any(pd.isna(df[col]) or df[col] is None for col in ['country', 'city', 'region']):
            locations.append(None)
        else:
            locations.append({
                "country": df["country"],
                "city": df["city"],
                "region": df["region"]
            })
    return locations


def create_models_to_neo4j(df):
    res = {
        "groups": convert_all_groups(df),
        "attack_type": df["attacktype1_txt"].tolist(),
        "target_type": df["target_type"].tolist(),
    }
    return res

def create_model(df: DataFrame) ->List[Dict[str,str]]:
    result = [
        {
            "eventid": row["eventid"],
            "date": {"year": row["year"], "month": row["month"], "day": row["day"]},
            "location": {"country": row["country"], "city": row["city"], "region": row["region"],
                         "latitude": row["latitude"], "longitude": row["longitude"]},
            "target": {"target_type": row["target_type"], "target1_name": row["target1"],
                       "target_nationality": row["target_nationality"]},
            "groups": [row["group_name"], row["group_name2"]],
            "attack": {"num_terrorists": row["num_terrorists"], "attack_type": row["attacktype1_txt"]},
            "result": {"num_spread": row["num_spread"], "num_killed": row["num_killed"]},
            "summary": row["summary"]
        }
        for _, row in df.iterrows()
    ]
    return result


def process_mongo(pd) -> List[Dict[str, str]]:
    return create_model(pd)


def process_neo4j(message: DataFrame) -> Dict[str, str]:
    models = create_models_to_neo4j(message)
    rows_as_dicts = message.apply(convert_to_event_model, axis=1).tolist()
    return {**models,"events": rows_as_dicts,"locations":create_model_location(message)}




def process_elastic(message: pd.DataFrame) -> List[Dict[str, str]]:
    message_clean = message.dropna(subset=['summary'])
    return [{"eventid": row["eventid"], "summary": row["summary"]} for _, row in message_clean.iterrows()]

