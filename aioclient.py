import asyncio
from json import loads

from aiokafka import AIOKafkaConsumer
from utils.sleeper import sleep

async def app():
    consumer = AIOKafkaConsumer(
        'world',
        bootstrap_servers='localhost:9092',
        group_id="test-group",
        auto_offset_reset='latest', # 오프셋 위치(earliest:가장 처음, latest: 가장 최근)
        enable_auto_commit=True, # 오프셋 자동 커밋 여부
        value_deserializer=lambda x: loads(x.decode('utf-8')), # 메시지의 값 역직렬화
        consumer_timeout_ms=1000 # 데이터를 기다리는 최대 시간
    )
    await consumer.start()

    try:
        while(True):
            print("Listening...")
            await consume(consumer)
            await sleep()
    finally:
        print("end up consumer")
        await consumer.stop()

async def consume(consumer):
    # Consume messages
    async for msg in consumer:
        print("processing message ...")
        await sleep()
        print("consumed: ", msg.topic, msg.partition, msg.offset,
                msg.key, msg.value, msg.timestamp)
        ## error occurs
        # raise Exception("Unexpected error!")

asyncio.run(app())