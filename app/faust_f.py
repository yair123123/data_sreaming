import asyncio
import os
from typing import Dict

import faust
import pandas as pd
from dotenv import load_dotenv

from streaming_data import process_neo4j, process_mongo, process_elastic

load_dotenv(verbose=True)

app = faust.App(
    "streaming",
    broker=os.getenv("BOOTSTRAP_SERVER"),
    value_serializer='json'
)

data1 = app.topic(os.getenv("TOPIC_PROCESS1"))
data2 = app.topic(os.getenv("TOPIC_PROCESS2"))

topic_mongo = app.topic(os.getenv("TOPIC_CONSUME_MONGO"))
topic_neo4j_target = app.topic(os.getenv("TOPIC_CONSUME_NEO4J_TARGET"))
topic_neo4j_attack = app.topic(os.getenv("TOPIC_CONSUME_NEO4J_ATTACK"))
topic_neo4j_group = app.topic(os.getenv("TOPIC_CONSUME_NEO4J_GROUP"))
topic_neo4j_region = app.topic(os.getenv("TOPIC_CONSUME_NEO4J_LOCATION"))
topic_neo4j_event = app.topic(os.getenv("TOPIC_CONSUME_NEO4J_EVENT"))
topic_elastic = app.topic(os.getenv("TOPIC_CONSUME_ELASTIC"))




@app.agent(data1)
async def send1_to_dbs(messages):
    await send(messages)

@app.agent(data2)
async def send2_to_dbs(messages):
    await send(messages)

async def send(messages):
    async for message in messages:
        message = pd.DataFrame(message)
        processed:Dict[str,str] = process_neo4j(message)
        print(processed.keys())
        try:
            tasks = [
                topic_mongo.send(value=process_mongo(message)),
                topic_elastic.send(value=process_elastic(message)),
                topic_neo4j_target.send(value=processed.get("target_type")),
                topic_neo4j_attack.send(value=processed.get("attack_type")),
                topic_neo4j_group.send(value=processed.get("groups")),
                topic_neo4j_region.send(value=processed.get("locations")),
                topic_neo4j_event.send(value=processed.get("events"))
            ]
            await asyncio.gather(*tasks)
        except Exception as e:
            print(e)








