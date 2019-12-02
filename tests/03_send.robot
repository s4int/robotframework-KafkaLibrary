*** Settings ***
Resource           resources.robot
Suite Setup        Connect to Local Kafka
Suite Teardown     Run Keyword And Ignore Error    Close

*** Test Cases ***
Send Message Verification
    ${test_topic}=  Topic name with random sufix  ${topic}
    ${TopicPartition}=  Create Partition  ${test_topic}
    ${topics}    Get Kafka Topics
    List Should Contain Value    ${topics}    ${test_topic}
    ${no1}    Get Number Of Messages In Topics    ${test_topic}
    Send Text Message    ${test_topic}    message
    ${no2}    Verify no of message    ${test_topic}    ${no1}

    Send Text Message    ${test_topic}    message2
    ${no3}    Verify no of message    ${test_topic}    ${no2}
    ${no4}    Get Number Of Messages In Topicpartition    ${TopicPartition}
    Should Be Equal As Integers    ${no3}    ${no4}
