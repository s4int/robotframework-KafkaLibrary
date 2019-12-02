*** Settings ***
Resource          resources.robot
Test Setup        Connect to Local Kafka
Test Teardown     Run Keyword And Ignore Error    Close

*** Variables ***
${no_msg_to_send}=  50
*** Test Cases ***
Poll max_records Verification
    ${test_topic}=  Topic name with random sufix  ${topic}
    ${TopicPartition}=    Create Partition    ${test_topic}
    Seek To End  ${TopicPartition}

    ${offset_before_send}=  Get Position  ${TopicPartition}
    ${no_of_msg_before_send}=    Get Number Of Messages In Topics    ${test_topic}

    Send ${no_msg_to_send} Text Messages to topic ${test_topic}

    ${expected_no_of_msg_after_send}=  Evaluate  ${no_of_msg_before_send}+${no_msg_to_send}
    ${no_of_msg_after_send}=    Get Number Of Messages In Topics    ${test_topic}
    Should Be Equal As Integers    ${expected_no_of_msg_after_send}    ${no_of_msg_after_send}

    ${messages}=  Poll  max_records=${10}  timeout_ms=${500}
    ${msg_no}=  Get Length  ${messages}
    Should Be Equal As Integers    ${msg_no}    ${10}

Poll timeout_ms Verification commited offset change
    ${test_topic}=  Topic name with random sufix  ${topic}
    ${TopicPartition}=    Create Partition    ${test_topic}
    Seek To End  ${TopicPartition}

    ${offset_before_send}=  Get Position  ${TopicPartition}
    ${no_of_msg_before_send}=    Get Number Of Messages In Topics    ${test_topic}

    Send ${no_msg_to_send} Text Messages to topic ${test_topic}

    ${messages}=  Poll  ${500}
    ${no}=  Get Length  ${messages}
    ${msg_no}=  Get Length  ${messages}
    Should Be Equal As Integers    ${msg_no}    ${no_msg_to_send}

