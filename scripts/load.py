from kafka import KafkaProducer
import json
import pandas as pd
from extract import read_csv_files
from transform import compare_transitions

def send_to_kafka(df, topic):
    """Sends each row of the DataFrame to a Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON serializer
    )
    
    for _, row in df.iterrows():
        # Convert row to dictionary and send to Kafka
        producer.send(topic, row.to_dict())
    
    producer.flush()
    print(f"Sent {len(df)} records to Kafka topic '{topic}'")

if __name__ == "__main__":
    # Load NCAA and NFL data
    nfl_df = read_csv_files('/home/jx2jetpl4ne/Documents/etl_project/nfl', 'NFL')
    ncaa_df = read_csv_files('/home/jx2jetpl4ne/Documents/etl_project/ncaa', 'NCAA')

    # Compare transitions
    transitions = compare_transitions(ncaa_df, nfl_df)
    
    # Send the transformed data to Kafka
    send_to_kafka(transitions, 'etl-topic')
