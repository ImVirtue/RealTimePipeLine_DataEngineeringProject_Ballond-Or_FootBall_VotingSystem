import streamlit as st
import pandas as pd
from confluent_kafka import Consumer, KafkaException
import json
from PIL import Image
import time

# Thiết lập giao diện chính
st.sidebar.title("Sidebar Menu")
st.title("Ballond'Or Voting")

# Đường dẫn ảnh
leo_img = "/home/davidntd/PycharmProjects/RealTime_DataEngineeringProject_Ballond'Or_FootBall_VotingSystem/image/leo.jpg"
cr7_img = "/home/davidntd/PycharmProjects/RealTime_DataEngineeringProject_Ballond'Or_FootBall_VotingSystem/image/cr7.jpeg"
ney_img = "/home/davidntd/PycharmProjects/RealTime_DataEngineeringProject_Ballond'Or_FootBall_VotingSystem/image/neymar.jpg"
iniesta_img = "/home/davidntd/PycharmProjects/RealTime_DataEngineeringProject_Ballond'Or_FootBall_VotingSystem/image/iniesta.jpg"

# Cấu hình Kafka Consumer
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'vote_consumer_group',
    'auto.offset.reset': 'latest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['aggregated_votes_per_candidate'])

table_placeholder = st.empty()
info_placeholder = st.empty()

newest_stats = {}

candidate_names = {
    'CR7': 'Cristiano Ronaldo',
    'M10': 'Lionel Messi',
    'N9': 'Neymar Jr.',
    'AI8': 'Andrés Iniesta'
}

def read_message():
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            return None
        if msg.error():
            raise KafkaException(msg.error())

        # Giải mã JSON từ Kafka
        result = json.loads(msg.value().decode('utf-8'))
        candidate_id = result['candidate_id']
        total_votes = result['total_votes']

        newest_stats[candidate_id] = total_votes
        df_vote_result = pd.DataFrame(newest_stats.items(), columns=["Candidate", "Total Votes"])
        df_vote_result["Full Name"] = df_vote_result["Candidate"].map(candidate_names)
        df_vote_result = df_vote_result[["Candidate", "Full Name", "Total Votes"]]
        return df_vote_result

    except Exception as e:
        print(f"Lỗi khi đọc tin nhắn Kafka: {e}")
        return None

def update_data():
    df_vote_result = read_message()
    if df_vote_result is None or df_vote_result.empty:
        return

    # process stats
    total_votes_overall = df_vote_result["Total Votes"].sum()
    max_row = df_vote_result.loc[df_vote_result["Total Votes"].idxmax()]
    max_candidate = max_row["Candidate"]
    max_votes = max_row["Total Votes"]

    with info_placeholder.container():
        st.markdown("""<h3 style='text-align: center; color: #FFA500;'>Voting Summary</h3>""", unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 1.5, 1])

        with col1:
            st.markdown(f"""<div style='background: #f0f0f0; padding: 10px; border-radius: 10px;'>
                            <h4>Total Votes: {total_votes_overall}</h4>
                            <h4>Total Candidates: {len(df_vote_result)}</h4>
                        </div>""", unsafe_allow_html=True)

        with col2:
            st.markdown("<h4 style='text-align: center;'>Leading Candidate</h4>", unsafe_allow_html=True)
            if max_candidate == 'CR7':
                st.image(cr7_img, use_container_width=True, caption="Cristiano Ronaldo")
            elif max_candidate == 'M10':
                st.image(leo_img, use_container_width=True, caption="Lionel Messi")
            elif max_candidate == 'N9':
                st.image(ney_img, use_container_width=True, caption="Neymar Jr.")
            elif max_candidate == 'AI8':
                st.image(iniesta_img, use_container_width=True, caption="Andrés Iniesta")

        with col3:
            st.markdown(f"""<div style='background: #f9f9f9; padding: 10px; border-radius: 10px;'>
                            <h4>{'Cristiano Ronaldo' if max_candidate == 'CR7' else 'Lionel Messi' if max_candidate == 'M10' else 'Neymar Jr.' if max_candidate == 'N9' else 'Andrés Iniesta'}</h4>
                            <h5>{'Real Madrid' if max_candidate == 'CR7' else 'FC Barcelona'}</h5>
                            <h4>Total Votes: {max_votes}</h4>
                        </div>""", unsafe_allow_html=True)

    table_placeholder.dataframe(df_vote_result.style.set_properties(**{'text-align': 'center'}))

if __name__ == '__main__':
    try:
        while True:
            update_data()
            time.sleep(1)
    except Exception as e:
        print(f"Lỗi xảy ra: {e}")
    finally:
        consumer.close()
