from typing import List, Dict

import toolz as t

a = {0: 'eventid', 1: 'year', 2: 'month', 3: 'day', 4: 'country', 5: 'city', 6: 'latitude', 7: 'longitude', 8: 'region',
 9: 'target_type', 10: 'target1', 11: 'target_nationality', 12: 'group_name', 13: 'group_name2', 14: 'attacktype1_txt',
 15: 'num_terrorists', 16: 'num_spread', 17: 'num_killed'}

a = (['eventid', 'year', 'month', 'day', 'country', 'city', 'latitude',
      'longitude', 'region', 'target_type', 'target1',
      'target_nationality', 'group_name', 'group_name2', 'attacktype1_txt',
      'num_terrorists', 'num_spread', 'num_killed'])


def create_model(row: List[str]) -> Dict[str, Dict[str, str]]:
    a = {k: v for k, v in enumerate(row)}
    return {
        "eventid": row[0],
        "date": {"year": row[1], "month": row[2], "day": row[3]},
        "location": {"country": row[4], "city": row[5], "region": row[8], "latitude": row[6], "longitude": row[7]},
        "target": {"target_type": row[9], "target1_name": row[10], "target_nationality": row[11]},
        "groups": [row[12], row[13]],
        "attack": {"num_terrorists": row[15], "attack_type": row[14]},
        "result": {"num_spread": row[16], "num_killed": row[17]},
    }


def process_neo4j(list_of_rows) -> str:
    return t.pipe(
        list_of_rows,

    )


def process_mongo(list_of_rows) -> str:
    return t.pipe(
        list_of_rows,
        lambda x: [create_model(row) for row in x],
    )
