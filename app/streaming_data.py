from typing import List, Dict

import toolz as t
from toolz import pluck, get_in


def convert_event_to_save(event: Dict[str, Dict[str, str]]):
    return {"eventid": event.get("event_id"),
            **event.get("data"),
            "country": get_in(["location", "country"], event),
            "attack_type": get_in(["attack", "attack_type"], event),
            "target_type": get_in([["target", "target_type"]], event),
            "groups": event.get("groups")}


def create_model(row: List[str]) -> Dict[str, Dict[str, str]]:
    return {
        "eventid": row[0],
        "date": {"year": row[1], "month": row[2], "day": row[3]},
        "location": {"country": row[4], "city": row[5], "region": row[8], "latitude": row[6], "longitude": row[7]},
        "target": {"target_type": row[9], "target1_name": row[10], "target_nationality": row[11]},
        "groups": [row[12], row[13]],
        "attack": {"num_terrorists": row[15], "attack_type": row[14]},
        "result": {"num_spread": row[16], "num_killed": row[17]},
    }


def process_mongo(list_of_rows) -> str:
    return t.pipe(
        list_of_rows,
        lambda x: [create_model(row) for row in x],
    )


def process_neo4j(message: List[Dict[str, Dict[str, str]]]):
    return {
        "location": set(pluck(["location"], message)),
        "groups": set(pluck("groups", message)),
        "attack_type": set(pluck(["attack", "attack_type"], message)),
        "target_type": set(pluck(["target", "target_type"], message)),
        "event": [convert_event_to_save(d) for d in message]
    }
