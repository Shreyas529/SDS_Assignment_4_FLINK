import json
import time
import argparse
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from data_generation import generate_event_batch, create_ad_campaign_mappings
from config import *
import numpy as np
from mmpp import MMPPGenerator

from kafka.admin import KafkaAdminClient, NewTopic

def ensure_topic(topic_name=TOPIC_NAME, partitions=PARTITIONS, replication=REPLICATION_FACTOR, timestamp_type="LogAppendTime"):
    """Idempotently create topic if missing, with LogAppendTime timestamps."""
    admin_client = None
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
        print(f"Attempting to create topic '{topic_name}'...")

        topic_list = [
            NewTopic(
                name=topic_name,
                num_partitions=partitions,
                replication_factor=replication,
                topic_configs={
                    "message.timestamp.type": timestamp_type  # 👈 force broker timestamp
                }
            )
        ]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{topic_name}' created successfully with {timestamp_type}.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists. Skipping creation.")
    except Exception as e:
        print(f"Topic ensure error for '{topic_name}': {e}")
    finally:
        if admin_client:
            admin_client.close()


def clear_topic(topic = TOPIC_NAME, partitions=PARTITIONS, replication=REPLICATION_FACTOR, timestamp_type="LogAppendTime"):
    """Delete the topic and recreate it to ensure it is empty.

    Note: This requires the Kafka broker to have delete-topic enabled (default in Confluent images).
    """
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        print(f"Deleting topic '{topic}' for cleanup...")
        admin_client.delete_topics([topic])
    except UnknownTopicOrPartitionError:
        print("Topic already absent when attempting delete (OK).")
    except Exception as e:
        print(f"Warning: could not delete topic: {e}")
    finally:
        # Kafka deletes are async; give broker a moment
        time.sleep(2)
        admin_client.close()

    # Recreate fresh, empty topic
    ensure_topic(topic_name=topic, partitions=partitions, replication=replication, timestamp_type=timestamp_type)
    print(f"Topic '{topic}' cleared and recreated.")

def producer(producer_id, throughput, total_duration_sec, ad_campaign_map, base_event_time_sec):
    """
    Initializes and runs a Kafka producer instance.

    Args:sleep
        producer_id (str): A unique identifier for this producer instance (for logging).
        throughput (int): The number of events to generate per second.
        total_duration_sec (int): The total number of seconds to run the producer.
        ad_campaign_map (dict): The pre-generated {ad_id_uuid: campaign_id_int} map.
        base_event_time_sec (int): The base timestamp (in seconds) for event generation.
    """
    print(f"[{producer_id}] Starting...")

    # 1. Initialize the KafkaProducer for this process.
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

    mmpp = MMPPGenerator(avg_rate=throughput)

    for i in range(total_duration_sec):
        logical_second = base_event_time_sec + i

        number_of_events_per_second = throughput if DISTRIBUTION == "normal" else mmpp.sample()

        # 3. Generate a batch of events.
        event_batch = generate_event_batch(number_of_events_per_second, ad_campaign_map, logical_second, base_event_time_sec)
        # print("Window ID:", event_batch[0]['window_id'], "Logical Second:", logical_second)

        # 4. Send events to Kafka.
        for event in event_batch:
            key_bytes = event['ad_id'].encode('utf-8')

            # Add current system timestamp in seconds
            event['event_time_ns'] = time.time_ns()

            # Send event (producer timestamp ignored intentionally)
            producer.send(TOPIC_NAME, key=key_bytes, value=event)

    # Send end-of-stream control message so downstream consumers know this producer finished
    try:
        producer.send(TOPIC_NAME, key=b"END", value={"type": "END", "producer_id": producer_id})
        print(f"[{producer_id}] Sent END sentinel.")

    except Exception as e:
        print(f"[{producer_id}] Failed to send END sentinel: {e}")

    producer.close()
    print(f"[{producer_id}] Finished producing events.")


def main():
    import multiprocessing

    print("\n==============================")
    print("   STARTING KAFKA PRODUCERS   ")
    print("==============================\n")

    # Read base time once so all producers share same logical clock
    base_time = int(time.time())

    # Prepare campaign mapping once
    print("Generating ad -> campaign mappings...")
    ad_campaign_map = create_ad_campaign_mappings()

    # Ensure event topic exists (DO NOT clear automatically)
    print(f"Ensuring topic '{TOPIC_NAME}' exists...")
    ensure_topic(topic_name=EVENTS_TOPIC, timestamp_type="CreateTime")
    ensure_topic(topic_name=RESULT_TOPIC, partitions=1, timestamp_type="LogAppendTime")

    # Spawn multiple producers based on config
    producers = []
    for p in range(NUM_PRODUCERS):
        pid = f"producer-{p+1}"
        proc = multiprocessing.Process(
            target=producer,
            args=(
                pid,
                EVENTS_PER_PRODUCER,      # throughput
                DURATION_SECONDS,         # total duration
                ad_campaign_map,
                base_time
            )
        )
        producers.append(proc)
        proc.start()
        print(f"[{pid}] started.")

    # Wait for all producers to finish
    for proc in producers:
        proc.join()

    print("\n==============================")
    print("   PRODUCERS COMPLETED")
    print("==============================\n")


if __name__ == "__main__":
    main()