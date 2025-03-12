from script.interact_with_db import create_connection, delete_tables
from confluent_kafka.admin import AdminClient
import shutil

checkpoint_path = "/home/davidntd/PycharmProjects/RealTime_DataEngineeringProject_Ballond'Or_FootBall_VotingSystem/checkpoints/checkpoint1"

if __name__ == '__main__':
    conn = create_connection()
    delete_tables(conn)

    admin_client = AdminClient({
        "bootstrap.servers": "localhost:9092"
    })

    shutil.rmtree(checkpoint_path)

    topics_to_delete = ["votes_topic", "voters_topic", 'aggregated_votes_per_candidate']

    fs = admin_client.delete_topics(topics_to_delete)

    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' đã được xóa thành công.")
        except Exception as e:
            print(f"Không thể xóa topic '{topic}': {e}")
