from functools import reduce
from typing import List, Dict

import toolz as t
from sqlalchemy.engine import row
from toolz import pluck, get_in, pipe



def convert_event_to_save(row: List[str]):
    res =  {
        "eventid":row[0],
        "year": row[1],
        "month": row[2],
        "day": row[3],
        "city": row[5],
        "attack_type": row[14],
        "target_type": row[9],
        "groups": [row[12], row[13]]
    }
    return res


def create_model(row: List[str]) -> Dict[str, Dict[str, str]]:
    result = {
        "eventid": row[0],
        "date": {"year": row[1], "month": row[2], "day": row[3]},
        "location": {"country": row[4], "city": row[5], "region": row[8], "latitude": row[6], "longitude": row[7]},
        "target": {"target_type": row[9], "target1_name": row[10], "target_nationality": row[11]},
        "groups": [row[12], row[13]],
        "attack": {"num_terrorists": row[15], "attack_type": row[14]},
        "result": {"num_spread": row[17], "num_killed": row[18]},
        "description": row[16]
    }
    return result


def process_mongo(list_of_rows) -> str:
    return t.pipe(
        list_of_rows,
        lambda x: [create_model(row) for row in x if row],
    )

def create_model_to_neo4j(row):
    res =  {
            "location": {"country": row[4], "city": row[5], "region": row[8], "latitude": row[6], "longitude": row[7]},
            "groups":  [row[12], row[13]],
            "attack_type":  row[14],
            "target_type": row[9],
            "event": [convert_event_to_save(row)]
        }
    return res

def process_neo4j(message: List[List[str]]) -> Dict[str, str]:
    models = [create_model_to_neo4j(row) for row in message]

    def merge_dicts(accum, curr):
        for key, value in curr.items():
            accum[key] = accum.get(key, []) + [value]
        return accum

    return reduce(merge_dicts, models, {})

def process_elastic(message: List[str]) -> List[Dict[str, str]]:
    return [{"eventid": row[0],"description": row[-1] }for row in message]
