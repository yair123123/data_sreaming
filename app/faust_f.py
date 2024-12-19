import asyncio
import os

import faust
from dotenv import load_dotenv

from app.streaming_data import process_neo4j
from streaming_data import  process

load_dotenv(verbose=True)

app = faust.App(
    "streaming",
    broker=os.getenv("BOOTSTRAP_SERVER"),
    value_serializer='json'
)

@app.agent(app.topic(os.getenv("TOPIC_PROCESS")))
async def send_to_dbs(messages):
    async for message in messages:
        print(type(message))
        topic_mongo = app.topic(os.getenv("TOPIC_CONSUME_MONGO"))
        topic_neo4j_target = app.topic(os.getenv("TOPIC_CONSUME_NEO4J_TARGET"))
        topic_neo4j_attack = app.topic(os.getenv("TOPIC_CONSUME_NEO4J_ATTACK"))
        topic_neo4j_group = app.topic(os.getenv("TOPIC_CONSUME_NEO4J_GROUP"))
        topic_neo4j_region = app.topic(os.getenv("TOPIC_CONSUME_NEO4J_LOCATION"))
        topic_neo4j_event = app.topic(os.getenv("TOPIC_CONSUME_NEO4J_EVENT"))
        processed = process_neo4j(message)
        try:
            tasks = [
                topic_mongo.send(value=process(message)),
                topic_neo4j_target.send(value=processed.get("target")),
                topic_neo4j_attack.send(value=processed.get("attack")),
                topic_neo4j_group.send(value=processed.get("group")),
                topic_neo4j_region.send(value=processed.get("location")),
                topic_neo4j_event.send(value=processed.get("event"))
            ]
            await asyncio.gather(*tasks)
        except Exception as e:
            print(e)
