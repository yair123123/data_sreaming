import json
from typing import List, Any

import toolz as t
from pandas import DataFrame
from toolz.curried import partial




def process(list_of_rows) -> str:
    list_of_rows: List[List[Any]] = json.loads(list_of_rows.value().decode('utf-8'))
    df_data = DataFrame(list_of_rows)
    return t.pipe(
        df_data,

    )
