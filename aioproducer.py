import asyncio
from json import dumps
from datetime import datetime

from aiokafka import AIOKafkaProducer
from utils.sleeper import sleep

async def send_one():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092', # 서버
        value_serializer=lambda x:dumps(x).encode('utf-8'), # 메시지의 값 직렬화
        compression_type='gzip', # 메시지 전달할 때 압축(None, gzip, snappy, lz4 등)
    )
    # Get cluster layout and initial topic/partition leadership information

    print("generate a topic ...")
    await sleep()
    data = {'message': "result " + str(datetime.now())}
    print("complete to generate a topic")

    print("connect to server")
    await producer.start()
    try:
        # Produce message
        print("produce the topic")
        await producer.send_and_wait("world", value=data)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()
        print("complete to produce the topic")

print("startintg app")
asyncio.run(send_one())
print("app started")