from kafka import KafkaConsumer, TopicPartition


class Consumer(object):
    consumer = None

    def connect_consumer(
            self,
            bootstrap_servers='127.0.0.1:9092',
            auto_offset_reset='earliest',
            client_id='Robot',
            group_id=None,
            enable_auto_commit=True,
            consumer_timeout_ms=1000,
            key_deserializer=None,
            value_deserializer=None,
            **kwargs
    ):

        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset=auto_offset_reset,
            client_id=client_id,
            group_id=group_id,
            enable_auto_commit=enable_auto_commit,
            consumer_timeout_ms=consumer_timeout_ms,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
            **kwargs
        )

    def _is_assigned(self, topic_partition):
        for tp in topic_partition:
            if tp in self.consumer.assignment():
                return True
        return False

    def get_kafka_topics(self):
        return list(self.consumer.topics())

    def get_kafka_partitions_for_topic(self, topic=None):
        return list(self.consumer.partitions_for_topic(topic))

    def assign_to_topic_partition(self, topic_partition=None):
        if isinstance(topic_partition, (unicode, str)):
            topic_partition = eval(topic_partition)
        if isinstance(topic_partition, TopicPartition):
            topic_partition = [topic_partition]
        if not self._is_assigned(topic_partition):
            self.consumer.assign(topic_partition)

    def subscribe_topic(self, topics=[]):
        if not isinstance(topics, list):
            topics = [topics]
        self.consumer.subscribe(topics)

    def get_position(self, topic_partition=None):
        if isinstance(topic_partition, (unicode, str)):
            topic_partition = eval(topic_partition)
        if isinstance(topic_partition, TopicPartition):
            return self.consumer.position(topic_partition)
        else:
            raise TypeError("topic_partition must be of type TopicPartition, create it with Create TopicPartition keyword.")

    def go_to_position(self, offset, topic_partition=None):
        if isinstance(topic_partition, (unicode, str)):
            topic_partition = eval(topic_partition)
        if isinstance(topic_partition, TopicPartition):
            self.consumer.seek(topic_partition, offset=offset)
        else:
            raise TypeError("topic_partition must be of type TopicPartition, create it with Create TopicPartition keyword.")

    def go_to_beginning(self, topic_partition=None):
        if isinstance(topic_partition, (unicode, str)):
            topic_partition = eval(topic_partition)
        if isinstance(topic_partition, TopicPartition):
            self.consumer.seek_to_beginning(topic_partition)
        else:
            raise TypeError("topic_partition must be of type TopicPartition, create it with Create TopicPartition keyword.")

    def go_to_end(self, topic_partition=None):
        if isinstance(topic_partition, (unicode, str)):
            topic_partition = eval(topic_partition)
        if isinstance(topic_partition, TopicPartition):
            self.consumer.seek_to_end(topic_partition)
        else:
            raise TypeError("topic_partition must be of type TopicPartition, create it with Create TopicPartition keyword.")

    def get_assigned_partitions(self):
        """
        Get the TopicPartitions currently assigned to this consumer.
        :return: list TopicPartition
        """
        return list(self.consumer.assignment())

    def get_number_of_messages_in_topics(self, topics):
        if not isinstance(topics, list):
            topics = [topics]

        number_of_messages = 0
        for t in topics:
            part = self.get_kafka_partitions_for_topic(topic=t)
            Partitions = map(lambda p: TopicPartition(topic=t, partition=p), part)
            number_of_messages += self.get_number_of_messages_in_topicpartition(Partitions)

        return number_of_messages

    def get_number_of_messages_in_topicpartition(self, topic_partition=None):
        if isinstance(topic_partition, (unicode, str)):
            topic_partition = eval(topic_partition)
        if isinstance(topic_partition, TopicPartition):
            topic_partition = [topic_partition]

        number_of_messages = 0
        subscription = self.consumer.subscription()

        self.consumer.unsubscribe()
        for Partition in topic_partition:
            if not isinstance(Partition, TopicPartition):
                raise TypeError("topic_partition must be of type TopicPartition, create it with Create TopicPartition keyword.")

            self.assign_to_topic_partition(Partition)

            self.consumer.seek_to_end(Partition)
            end = self.consumer.position(Partition)
            self.consumer.seek_to_beginning(Partition)
            start = self.consumer.position(Partition)
            number_of_messages += end-start

        self.consumer.unsubscribe()
        self.consumer.subscribe(subscription)
        return number_of_messages

    def poll(self, max_records=None, timeout_ms=1500):
        print("poll")
        messages = self.consumer.poll(timeout_ms=timeout_ms, max_records=max_records)

        result = []
        for _, msg in messages.iteritems():
            for item in msg:
                result.append(item)
        return result

    def commit(self, offset=None):
        """
        Commit offsets to kafka, blocking until success or error.

        :param offset (dict, optional): {TopicPartition: OffsetAndMetadata} dict to commit with
         the configured group_id. Defaults to currently consumed offsets for all subscribed partitions.
        """
        pass

    def committed(self, topic_partition):
        """
        Get the last committed offset for the given partition.
        :param topic_partition (TopicPartition): The partition to check.:
        :return: The last committed offset, or None if there was no prior commit.
        """
        pass

    def close(self, autocommit=True):
        """
        Close the consumer, waiting indefinitely for any needed cleanup.

        :param autocommit (bool):
        """
        self.consumer.close(autocommit=autocommit)
