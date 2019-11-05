*** Settings ***
Library           KafkaLibrary

*** Test Cases ***
Connect Consumer
    Connect To Kafka	bootstrap_servers=127.0.0.1:9092  auto_offset_reset=latest  client_id=Robot  group_id=Robot
