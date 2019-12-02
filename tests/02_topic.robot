*** Settings ***
Resource          resources.robot
Suite Setup     Connect to Local Kafka
Suite Teardown   Close

*** Variable ***
${TEST_TOPIC_PARTITIONS}=  ${1}

*** Test Cases ***
Get Kafka Topics
    ${topics}=  Get Kafka Topics
    LOG  ${topics}
    Should Not Be Empty  ${topics}
    Should Contain	${topics}	__confluent.support.metrics

Create Topicpartition object
    ${ret}=  Create Topicpartition  ${topic}  ${TEST_TOPIC_PARTITIONS}
    LOG  ${ret}
    Should Be Equal  ${ret.topic}  ${topic}
    Should Be Equal  ${ret.partition}  ${TEST_TOPIC_PARTITIONS}

Auto Create Topic on kafka with new message
    ${topics}=  Get Kafka Topics
    ${topics_no}=  Get Length  ${topics}
    Should Not Be Empty  ${topics}

    ${test_topic}=  Topic name with random sufix  ${topic}
    Should Not Contain  ${topics}  ${test_topic}
    Send Text Message  ${test_topic}  Create new topic

    Connect To Kafka	bootstrap_servers=${BOOTSTRAP_SERVERS}  client_id=${CLIENT_ID}  group_id=${GROUP_ID}
    ${topics_after_create}=  Get Kafka Topics
    ${topics_after_create_no}=  Get Length  ${topics_after_create}
    Should Be True	${topics_no} < ${topics_after_create_no}
    Should Contain  ${topics_after_create}  ${test_topic}
