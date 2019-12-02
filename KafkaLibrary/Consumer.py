from kafka import KafkaConsumer, TopicPartition


class Consumer(object):
    consumer = None

    def connect_consumer(
            self,
            bootstrap_servers='127.0.0.1:9092',
            client_id='Robot',
            group_id=None,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            **kwargs
    ):
        """Connect kafka consumer.
    
        Keyword Arguments:
        - ``bootstrap_servers``: 'host[:port]' string (or list of 'host[:port]'
            strings) that the consumer should contact to bootstrap initial
            cluster metadata. This does not have to be the full node list.
            It just needs to have at least one broker that will respond to a
            Metadata API Request. Default: `127.0.0.1:9092`.
        - ``client_id`` (str): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client. Also
            submitted to GroupCoordinator for logging with respect to
            consumer group administration. Default: `Robot`.
        - ``group_id`` (str or None): name of the consumer group to join for dynamic
            partition assignment (if enabled), and to use for fetching and
            committing offsets. If None, auto-partition assignment (via
            group coordinator) and offset commits are disabled.
            Default: `None`.
        - ``auto_offset_reset`` (str): A policy for resetting offsets on
            OffsetOutOfRange errors: `earliest` will move to the oldest
            available message, `latest` will move to the most recent. Any
            other value will raise the exception. Default: `latest`.
        - ``enable_auto_commit`` (bool): If true the consumer's offset will be
            periodically committed in the background. Default: `True`.
            
        Note:
        Configuration parameters are described in more detail at
        http://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
        """

        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset=auto_offset_reset,
            client_id=client_id,
            group_id=group_id,
            enable_auto_commit=enable_auto_commit,
            **kwargs
        )

    def _is_assigned(self, topic_partition):
        for tp in topic_partition:
            if tp in self.consumer.assignment():
                return True
        return False

    def get_kafka_topics(self):
        """Return list of kafka topics.
        """

        return list(self.consumer.topics())

    def get_kafka_partitions_for_topic(self, topic=None):
        """Retrun list of partitions for kafka topic.
        
        - ``topic`` (str): Topic to check.
        """

        return list(self.consumer.partitions_for_topic(topic))

    def assign_to_topic_partition(self, topic_partition=None):
        """Assign a list of TopicPartitions to this consumer.
        
        - ``partitions`` (list of `TopicPartition`): Assignment for this instance.
        """

        if isinstance(topic_partition, TopicPartition):
            topic_partition = [topic_partition]
        if not self._is_assigned(topic_partition):
            self.consumer.assign(topic_partition)

    def subscribe_topic(self, topics=[], pattern=None):
        """Subscribe to a list of topics, or a topic regex pattern.
        
        - ``topics`` (list): List of topics for subscription.
        - ``pattern`` (str): Pattern to match available topics. You must provide either topics or pattern,
          but not both.
        """

        if not isinstance(topics, list):
            topics = [topics]
        self.consumer.subscribe(topics, pattern=pattern)

    def get_position(self, topic_partition=None):
        """Return offset of the next record that will be fetched.
        
        - ``topic_partition`` (TopicPartition): Partition to check
        """

        if isinstance(topic_partition, TopicPartition):
            return self.consumer.position(topic_partition)
        else:
            raise TypeError("topic_partition must be of type TopicPartition, create it with Create TopicPartition keyword.")

    def seek(self, offset, topic_partition=None):
        """Manually specify the fetch offset for a TopicPartition.
        
        - ``offset``: Message offset in partition
        - ``topic_partition`` (`TopicPartition`): Partition for seek operation
        """

        if isinstance(topic_partition, TopicPartition):
            self.consumer.seek(topic_partition, offset=offset)
        else:
            raise TypeError("topic_partition must be of type TopicPartition, create it with Create TopicPartition keyword.")

    def seek_to_beginning(self, topic_partition=None):
        """Seek to the oldest available offset for partitions.
        
        - ``topic_partition``: Optionally provide specific TopicPartitions,
          otherwise default to all assigned partitions.
        """

        if isinstance(topic_partition, TopicPartition):
            self.consumer.seek_to_beginning(topic_partition)
        else:
            raise TypeError("topic_partition must be of type TopicPartition, create it with Create TopicPartition keyword.")

    def seek_to_end(self, topic_partition=None):
        """Seek to the most recent available offset for partitions.
        
        - ``topic_partition``: Optionally provide specific `TopicPartitions`,
          otherwise default to all assigned partitions.
        """

        if isinstance(topic_partition, TopicPartition):
            self.consumer.seek_to_end(topic_partition)
        else:
            raise TypeError("topic_partition must be of type TopicPartition, create it with Create TopicPartition keyword.")

    def get_assigned_partitions(self):
        """Return `TopicPartitions` currently assigned to this consumer.
        """
        return list(self.consumer.assignment())

    def get_number_of_messages_in_topics(self, topics):
        """Retrun number of messages in topics.
        
        - ``topics`` (list): list of topics.
        """

        if not isinstance(topics, list):
            topics = [topics]

        number_of_messages = 0
        for t in topics:
            part = self.get_kafka_partitions_for_topic(topic=t)
            Partitions = map(lambda p: TopicPartition(topic=t, partition=p), part)
            number_of_messages += self.get_number_of_messages_in_topicpartition(Partitions)

        return number_of_messages

    def get_number_of_messages_in_topicpartition(self, topic_partition=None):
        """Return number of messages in TopicPartition.
        
        - ``topic_partition`` (list of TopicPartition)
        """

        if isinstance(topic_partition, TopicPartition):
            topic_partition = [topic_partition]

        number_of_messages = 0
        assignment = self.consumer.assignment()

        for Partition in topic_partition:
            if not isinstance(Partition, TopicPartition):
                raise TypeError("topic_partition must be of type TopicPartition, create it with Create TopicPartition keyword.")

            self.assign_to_topic_partition(Partition)
            current = self.consumer.position(Partition)

            self.consumer.seek_to_end(Partition)
            end = self.consumer.position(Partition)

            self.consumer.seek_to_beginning(Partition)
            start = self.consumer.position(Partition)

            self.consumer.seek(Partition, current)
            number_of_messages += end-start

        self.consumer.assign(assignment)
        return number_of_messages

    def poll(self, timeout_ms=0, max_records=None):
        """Fetch data from assigned topics / partitions.
        
        - ``max_records`` (int): maximum number of records to poll. Default: Inherit value from max_poll_records.
        - ``timeout_ms`` (int): Milliseconds spent waiting in poll if data is not available in the buffer.
          If 0, returns immediately with any records that are available currently in the buffer, else returns empty.
          Must not be negative. Default: `0`
        """

        messages = self.consumer.poll(timeout_ms=timeout_ms, max_records=max_records)

        result = []
        for _, msg in messages.items():
            for item in msg:
                result.append(item)
        return result

    def commit(self, offsets=None):
        """Commit offsets to kafka, blocking until success or error.

        - ``offset`` (dict): `{TopicPartition: OffsetAndMetadata}` dict to commit with
          the configured group_id. Defaults to currently consumed offsets for all subscribed partitions.
        """

        self.consumer.commit(offsets)

    def committed(self, topic_partition):
        """Returns the last committed offset for the given partition, or None if there was no prior commit.

        - ``topic_partition`` (`TopicPartition`): The partition to check.
        """
        return self.consumer.committed(topic_partition)

    def close(self, autocommit=True):
        """Close the consumer, waiting indefinitely for any needed cleanup.

        - ``autocommit`` (bool): Default `True`.
        """
        self.consumer.close(autocommit=autocommit)
