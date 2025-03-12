import requests
from confluent_kafka import Consumer, Producer
import json
import uuid
from interact_with_db import create_connection, create_table, insert_into_voter, insert_into_candidate, check_candidates_data

p = Producer({
    'bootstrap.servers' : 'localhost:9092'
})

def generate_voters_data():
    import requests
    api_url = 'https://api.api-ninjas.com/v1/randomuser'
    response = requests.get(api_url, headers={'X-Api-Key': 'vejkonSCzoNumMUM9LJDbQ==JlpZHGxINWvYoSlc'})
    if response.status_code == requests.codes.ok:
        data = json.loads(response.text)
        return {
            'voter_id' : str(uuid.uuid4()),
            'voter_user' : data['username'],
            'gender' : data['sex'],
            'address' : data['address'],
            'voter_name' : data['name'],
            'voter_email' : data['email'],
            'voter_dob' : data['birthday']
         }

    else:
        print("Error:", response.status_code, response.text)
        return None

def acked(err, msg):
    if err:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


if __name__ == '__main__':
    # Initialization
    conn = create_connection()
    create_table(conn)

    if not check_candidates_data(conn):
        insert_into_candidate(conn)

    for i in range(10):
        voter_data = generate_voters_data()

        data = (
            str(voter_data['voter_id']),
            str(voter_data['voter_user']),
            str(voter_data['gender']),
            str(voter_data['address']),
            str(voter_data['voter_name']),
            str(voter_data['voter_email']),
            str(voter_data['voter_dob'])
        )

        msg = json.dumps(voter_data)

        insert_into_voter(conn, data)

        p.produce(
            'voters_topic',
            value=msg,
            callback=acked
        )

    p.flush()
