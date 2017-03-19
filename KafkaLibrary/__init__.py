# -*- coding: utf-8 -*-
from Consumer import Consumer
from Producer import Producer
import logging
from kafka import TopicPartition


class KafkaLibrary(Consumer, Producer):

    def connect_to_kafka(self, bootstrap_servers='127.0.0.1:9092',
                         auto_offset_reset='earliest',
                         client_id='Robot',
                         group_id=None,
                         enable_auto_commit=True,
                         key_deserializer=None,
                         value_deserializer=None,
                         consumer_timeout_ms=1000):

        self.connect_consumer(
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset=auto_offset_reset,
            client_id=client_id,
            group_id=group_id,
            enable_auto_commit=enable_auto_commit,
            consumer_timeout_ms=consumer_timeout_ms,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer
        )
        self.connect_producer(bootstrap_servers=bootstrap_servers, client_id=client_id)

    def create_topicpartition(self, topic, partition):
        return TopicPartition(topic=topic, partition=partition)

    def close(self):
        self.consumer.close()
