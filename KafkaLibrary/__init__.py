from .Consumer import Consumer
from .Producer import Producer
from kafka import TopicPartition
from .version import VERSION

__version__ = VERSION


class KafkaLibrary(Consumer, Producer):

    ROBOT_LIBRARY_VERSION = VERSION
    ROBOT_LIBRARY_SCOPE = 'GLOBAL'

    def connect_to_kafka(self, bootstrap_servers='127.0.0.1:9092',
                         auto_offset_reset='latest',
                         client_id='Robot',
                         **kwargs
                         ):
        """Connect to kafka
        - ``bootstrap_servers``: default 127.0.0.1:9092
        - ``client_id``: default: Robot
        """

        self.connect_consumer(
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset=auto_offset_reset,
            client_id=client_id,
            **kwargs
        )
        self.connect_producer(bootstrap_servers=bootstrap_servers, client_id=client_id)

    def create_topicpartition(self, topic, partition):
        """Create TopicPartition object

        - ``topic``: kafka topic name
        - ``partition``: topic partition number
        """

        return TopicPartition(topic=topic, partition=partition)

    def close(self):
        self.consumer.close()
