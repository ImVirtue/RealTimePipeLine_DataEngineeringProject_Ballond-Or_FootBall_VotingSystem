# Realtime Data Pipeline and Visualization of Ballon d'Or Voting Results Using Spark Structured Stream, Kafka, Streamlit

<img src="https://editorial.uefa.com/resources/028e-1b112bf31ef0-0dd2dd517d98-1000/ballon_d_or_photo.png" alt="Ballon d'Or" width="300"/>

## Table of Contents
- [Introduction](#introduction)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Data Pipeline](#data-pipeline)
- [Usage](#usage)
- [Visualizations](#visualizations)
- [Conclusion](#concly)

## Introduction
This project intends to simulate the creation of a real-time Ballon d'Or Voting system that manages all of the results of candidates (footballers) and the total of votes that they have. Storing data of voters in a PostgreSQL database, transform and load in real-time by Kafka and Spark Structured Streaming. Finally, visualize the result on the website hosted by Streamlit.

## Features
- **Automated ETL Pipeline**: Using Apache Spark and Kafka to automate the process of collecting data from API voters, transforming into a usable format.
- **Data Visualization**: Leveraging Streamlit to create interesting and real-time visualizations for the result of voting.
- **Docker Compose Setup**: The project uses Docker Compose to streamline the deployment and management of the required services, including Apache Kafka, Apache Spark, Apache ZooKeeper and PostgreSQL.

## Technologies Used
- **PostgreSQL**: To store transformed voter data.
- **Apache Kafka**: Processing data in real-time through passing and receiving messages in topics.
- **Apache ZooKeeper**: Managing Kafka broker.
- **Apache Spark**: Reading data from Kafka broker and aggregating them to find the result of voting.
- **Streamlit**: For visualizing the voting result.
- **Docker Compose**: To orchestrate the deployment of the above technologies.

## Data Pipeline

## Visualizations

## Conclusion
  
