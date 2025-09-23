import asyncio
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
import os

BOOT = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

async def main():
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOT)
    await admin.start()
    try:
        topics = [
            NewTopic("chem.ingest.requests.v1", num_partitions=6, replication_factor=1),
            NewTopic("chem.ingest.results.v1",  num_partitions=3, replication_factor=1),
            NewTopic("chem.ingest.dlq.v1",      num_partitions=3, replication_factor=1),
        ]
        await admin.create_topics(new_topics=topics, validate_only=False)
        print("Topics created")
    except Exception as e:
        print("Create topics error:", e)
    finally:
        await admin.close()

if __name__ == "__main__":
    asyncio.run(main())
