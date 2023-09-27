import asyncio
import os

from aiokafka.structs import RecordMetadata
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import UnknownTopicOrPartitionError, KafkaConnectionError

from styx.common.logging import logging

from worker.egress.base_egress import BaseEgress
from worker.util.kafka_producer_pool import KafkaProducerPool

EGRESS_TOPIC_NAME: str = os.getenv('EGRESS_TOPIC_NAME', 'styx-egress')
KAFKA_URL: str = os.getenv('KAFKA_URL', None)
EPOCH_INTERVAL_MS: int = int(os.getenv('EPOCH_INTERVAL_MS', 1))


class StyxKafkaEgress(BaseEgress):

    worker_id: int

    def __init__(self, output_offset: int = -1):
        self.kafka_egress_producer_pool = KafkaProducerPool(KAFKA_URL)
        self.output_offset: int = output_offset
        self.messages_sent_before_recovery: set = set()

    def clear_messages_sent_before_recovery(self):
        self.messages_sent_before_recovery: set = set()

    async def start(self, worker_id):
        if self.output_offset != -1:
            logging.warning(f'Getting messages sent before recovery: {self.output_offset}')
            await self.get_messages_sent_before_recovery(worker_id, self.output_offset)
            logging.warning('Got messages sent before recovery')
        self.worker_id = worker_id
        await self.kafka_egress_producer_pool.start(worker_id)

    async def stop(self):
        await self.kafka_egress_producer_pool.close()

    async def send(self, key, value):
        if key not in self.messages_sent_before_recovery:
            res: RecordMetadata = await next(self.kafka_egress_producer_pool).send_and_wait(EGRESS_TOPIC_NAME,
                                                                                            key=key,
                                                                                            value=value,
                                                                                            partition=self.worker_id-1)
            self.output_offset = max(self.output_offset, res.offset)
        else:
            self.messages_sent_before_recovery.remove(key)

    async def get_messages_sent_before_recovery(self, worker_id, output_offset_at_snapshot: int):
        kafka_output_consumer = AIOKafkaConsumer(bootstrap_servers=[KAFKA_URL],
                                                 enable_auto_commit=False)
        output_topic_partition = [TopicPartition(EGRESS_TOPIC_NAME, worker_id - 1)]
        kafka_output_consumer.assign(output_topic_partition)
        while True:
            # start the kafka consumer
            try:
                await kafka_output_consumer.start()
                kafka_output_consumer.seek(output_topic_partition[0], output_offset_at_snapshot + 1)
            except (UnknownTopicOrPartitionError, KafkaConnectionError):
                await asyncio.sleep(1)
                logging.warning(f'Kafka at {KAFKA_URL} not ready yet, sleeping for 1 second')
                continue
            break
        try:
            # step 1 get current offset
            current_offset = await kafka_output_consumer.end_offsets(output_topic_partition)
            current_offset = list(current_offset.values())[0]
            continue_reading = True
            while continue_reading:
                result = await kafka_output_consumer.getmany(timeout_ms=EPOCH_INTERVAL_MS)
                for _, messages in result.items():
                    continue_reading = self.process_messages_sent_before_recovery(messages, current_offset)
                    if not continue_reading:
                        break
        finally:
            await kafka_output_consumer.stop()

    def process_messages_sent_before_recovery(self, messages, current_offset):
        for message in messages:
            if message.offset >= current_offset - 1:
                self.messages_sent_before_recovery.add(message.key)
                return False
            else:
                self.messages_sent_before_recovery.add(message.key)
        return True
