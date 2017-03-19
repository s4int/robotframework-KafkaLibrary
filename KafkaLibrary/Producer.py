from kafka import KafkaProducer


class Producer(object):
    producer = None

    def connect_producer(self, bootstrap_servers='127.0.0.1:9092', client_id='Robot', **kwargs):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id=client_id, **kwargs)

    def send(self, topic, value=None, timeout=60, key=None, partition=None, timestamp_ms=None):
        """
        Publish a message to a topic.

        :param topic(str): topic where the message will be published
        :param value: message value. Must be type bytes, or be serializable to bytes via configured value_serializer.
         If value is None, key is required and message acts as a ‘delete’.
        :param timeout:
        :param key: a key to associate with the message. Can be used to determine which partition
         to send the message to. If partition is None (and producer’s partitioner config is left as default), then messages with the same key will be delivered to the same partition (but if key is None, partition is chosen randomly). Must be type bytes, or be serializable to bytes via configured key_serializer.
        :param partition (int): optionally specify a partition. If not set, the partition will be selected using the configured ‘partitioner’.
        :param timestamp_ms (int): epoch milliseconds (from Jan 1 1970 UTC) to use as the message timestamp.
         Defaults to current time.
        :return:
        """
        future = self.producer.send(topic, value=value, key=key, partition=partition, timestamp_ms=timestamp_ms)
        future.get(timeout=timeout)

    def flush(self, timeout=None):
        """
        Invoking this method makes all buffered records immediately available to send (even if
        linger_ms is greater than 0) and blocks on the completion of the requests associated with these records.

        :param timeout (float): timeout in seconds to wait for completion
        """
        self.producer.flush(timeout=timeout)

    def close(self, timeout=None):
        """
        Close this producer.

        :param timeout (float):  timeout in seconds to wait for completion.
        """

        self.producer.close(timeout=timeout)
