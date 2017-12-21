## Build a Blockchain with Apache Kafka

Code for a [tutorial](link) about building a simple blockchain on Kafka.


## Prerequisites
- Python 3.6+
- [Docker](https://www.docker.com/)
- [Kafkacat](https://github.com/edenhill/kafkacat)

## Environment Setup
There are several ways to set up a Python virtual environment, one is with [pyenv](https://github.com/pyenv/pyenv) and pip:

    pyenv install 3.6.3
    pyenv shell 3.6.3
    mkvirtualenv --python=`pyenv which python` mynewenv
    pip install -r requirements.txt

Set up local networking to Kafka/Zookeeper with the following lines in /etc/hosts:

    127.0.0.1 kafka
    127.0.0.1 zookeeper

## Usage

Start the broker:

    docker-compose up -d

Run a consumer on partition 0:

    python kafka_blockchain.py 0

Publish 3 transactions:

    for i in `seq 1 3`;
    do
            echo "{\"from\": \"alice\", \"to\":\"bob\", \"amount\": 3}"  | kafkacat -b kafka:9092 -t transactions -p 0
    done

    
Check the transactions were added to a block on the `blockchain` topic:

    kafkacat -C -b kafka:9092  -t blockchain

You should see some output like this:

    $ kafkacat -C -b kafka:9092  -t blockchain
    {"index": 1, "timestamp": 1513818733.589511, "transactions": [], "proof": 100, "previous_hash": 1}
    % Reached end of topic blockchain [0] at offset 1
    {"index": 2, "timestamp": 1513819012.545352, "transactions": [{"sender": "alice", "recipient": "bob", "amount": 3}, {"sender": "alice", "recipient": "bob", "amount": 3}, {"sender": "alice", "recipient": "bob", "amount": 3}, {"sender": "0", "recipient": "a75abb3f132d440da6b88e87d0d5db62", "amount": 1}], "proof": 35293, "previous_hash": "09b672b4fc2f068f020df9f121db9fdbc73579b386612f2f9d421ade6ad6dfa8"}
    % Reached end of topic blockchain [0] at offset 2

To balance transactions across two consumers, start a second consumer on partition 1, and remove `-p 0` from the publication script above.