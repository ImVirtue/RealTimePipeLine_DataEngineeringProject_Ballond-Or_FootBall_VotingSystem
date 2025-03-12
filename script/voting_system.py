from confluent_kafka import Producer, Consumer, KafkaError
from interact_with_db import insert_into_vote
import random
import json
from datetime import datetime
from script.interact_with_db import create_connection, insert_into_vote

candidates_id = ['CR7', 'M10', 'N9', 'AI8']

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_group',
    'auto.offset.reset': 'earliest'
}

p = Producer({
    'bootstrap.servers' : 'localhost:9092'
})

consumer = Consumer(conf)

def acked(err, msg):
    if err:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


if __name__ == '__main__':
    conn = create_connection()
    consumer.subscribe(['voters_topic'])

    try:
        while True:
            msg = consumer.poll(1.0)
            # print(type(msg))
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = candidates_id[random.randint(0, 3)]
                voting_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                data = (
                    voter['voter_id'],
                    chosen_candidate,
                    voting_time,
                    1
                )

                msg = json.dumps({
                        'voter_id' : voter['voter_id'],
                        'candidate_id': chosen_candidate,
                        'voting_time' : voting_time,
                        'vote' : 1
                    })

                print(f"msg after processing: {msg}")

                insert_into_vote(conn, data)

                p.produce(
                    'votes_topic',
                    value = msg,
                    callback=acked
                )
                p.poll(0)
                p.flush()


    except Exception as e:
        print(e)

