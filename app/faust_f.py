import os

import faust
from dotenv import load_dotenv
from streaming_data import  process_mongo, process_neo4j

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
        # topic_neo4j = app.topic(os.getenv("TOPIC_CONSUME_NEO4J"))
        try:
            await topic_mongo.send(value=process_mongo(message))
            # await topic_neo4j.send(value=process_neo4j((message)))
        except Exception as e:
            print(e)
