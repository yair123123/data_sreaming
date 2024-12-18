import os

import faust
from dotenv import load_dotenv
from streaming_data import process
load_dotenv(verbose=True)

app_faust = faust.App(
    "streaming",
    broker=os.getenv("BOOTSTRAP_SERVER"),
    value_serializer='json'
)

@app_faust.agent(os.getenv("TOPIC_PROCESS"))
async def send_to_dbs(messages):
    async for message in messages:
        topic_psql = app_faust.topic(os.getenv("TOPIC_CONSUME_MONGO"), message)
        topic_mongo = app_faust.topic(os.getenv("TOPIC_CONSUME_NEO4J"), message)
        try:
            await topic_mongo.send(value=message.value)
            await topic_psql.send(value=process((message.value)))
        except Exception as e:
            print(e)