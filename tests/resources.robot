*** Settings ***
Library           KafkaLibrary
Library           String
Library           Collections

*** Variable ***
${BOOTSTRAP_SERVERS}=   127.0.0.1:9092
${CLIENT_ID}=           Robot
${GROUP_ID}=            Robot
${topic}=               test-topic

*** Keyword ***
Connect Consumer Extended
    Connect Consumer    bootstrap_servers=${BOOTSTRAP_SERVERS}    client_id=Consumer    group_id=${GROUP_ID}  enable_auto_commit=${TRUE}

Connect Producer Extended
    Connect Producer    bootstrap_servers=${BOOTSTRAP_SERVERS}    client_id=Producer

Connect to Local Kafka
    Connect To Kafka    bootstrap_servers=${BOOTSTRAP_SERVERS}    auto_offset_reset=latest    client_id=Robot    group_id=${GROUP_ID}

Send Text Message
    [Arguments]    ${topic}    ${message_text}
    ${bytes}    Encode String To Bytes    ${message_text}    UTF-8
    Send    ${topic}    value=${bytes}

Create new topic
    [Arguments]  ${topic}
    Send Text Message  ${topic}  New Topic

    Connect to Local Kafka
    ${topicPartition}=  Create Topicpartition    ${topic}    ${0}
    Assign To Topic Partition    ${topicPartition}
    [Return]  ${topicPartition}

Create Partition
    [Arguments]    ${topic}
    ${topics}=  Get Kafka Topics
    ${part}=  Run Keyword If	$topic in $topics  get kafka partitions for topic    ${topic}
    ...  ELSE  Set variable  ${None}
    ${topicPartition}=  Run Keyword If	${part} is ${None}	 Create new topic  ${topic}
    ...  ELSE	Create Topicpartition    ${topic}    ${part[0]}
    Assign To Topic Partition    ${topicPartition}
    [Return]    ${topicPartition}

Send ${n} Text Messages to topic ${topic}
    [Documentation]    Loops over values from 0 to n
    FOR    ${index}    IN RANGE    0    ${n}
        Send Text Message  ${topic}  messahe no. ${index}
    END

Verify no of message
    [Arguments]    ${topic}    ${before_sending}
    ${after}    Get Number Of Messages In Topics    ${topic}
    ${expected after}    Evaluate    ${before_sending}+1
    Should Be Equal As Integers    ${after}    ${expected after}
    [Return]    ${after}

Topic name with random sufix
    [Arguments]     ${topic_name}
    ${suffix}=	Generate Random String	4	[LOWER]
    ${topic_name_with_random}=	Format String	{}-{}	${topic_name}	${suffix}
    [Return]  ${topic_name_with_random}