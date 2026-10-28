"""Read the last record of every partition of the raw topic (input to resume.decide_resume)."""

from confluent_kafka import Consumer, TopicPartition

from streampulse_producer.resume import LastRecord


def read_last_records(bootstrap_servers: str, topic: str, timeout_s: float = 10.0) -> list[LastRecord]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "streampulse-producer-resume",  # required by the client; nothing is committed
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=timeout_s)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"topic {topic} is not available: {getattr(topic_metadata, 'error', None)}")
        records = []
        for partition in sorted(topic_metadata.partitions):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=timeout_s)
            if high <= low:
                continue  # empty partition
            consumer.assign([TopicPartition(topic, partition, high - 1)])
            message = consumer.poll(timeout_s)
            if message is None or message.error() is not None:
                raise RuntimeError(f"could not read the last record of {topic}[{partition}]")
            headers = {name: value or b"" for name, value in (message.headers() or [])}
            records.append(LastRecord(partition=partition, headers=headers))
        return records
    finally:
        consumer.close()
