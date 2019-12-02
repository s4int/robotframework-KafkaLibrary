*** Settings ***
Resource          resources.robot
Test Teardown   Close

*** Test Cases ***
Connect To Kafka without defined group
    Connect To Kafka	bootstrap_servers=${BOOTSTRAP_SERVERS}  client_id=${CLIENT_ID}

Connect To Kafka with defined group
    Connect To Kafka	bootstrap_servers=${BOOTSTRAP_SERVERS}  client_id=${CLIENT_ID}  group_id=${GROUP_ID}

Connect Consumer without defined group
    Connect Consumer	bootstrap_servers=${BOOTSTRAP_SERVERS}  client_id=Robot

Connect Consumer with defined group
    Connect Consumer	bootstrap_servers=${BOOTSTRAP_SERVERS}  client_id=Robot  group_id=${GROUP_ID}

Connect Producer
    Connect Producer	bootstrap_servers=${BOOTSTRAP_SERVERS}  client_id=Robot
