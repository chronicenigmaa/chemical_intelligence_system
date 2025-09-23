import asyncio, json, os
from aiokafka import AIOKafkaConsumer

# Quickly view messages. Change topic to requests/results/dlq as needed


BOOT  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "chem.ingest.requests.v1")
N     = int(os.getenv("N", "20"))

async def peek(topic: str, n: int):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=BOOT,
        group_id=None,                 # no commits (tap mode)
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        seen = 0
        async for msg in consumer:
            try:
                print(json.dumps(json.loads(msg.value), indent=2))
            except Exception:
                print(msg.value.decode(errors="replace"))
            seen += 1
            if seen >= n:
                break
    finally:
        await consumer.stop()

if __name__ == "__main__":
    # Usage:
    #   python -m scripts.peek_topic
    #   TOPIC=chem.ingest.results.v1 N=50 python -m scripts.peek_topic
    asyncio.run(peek(TOPIC, N))
