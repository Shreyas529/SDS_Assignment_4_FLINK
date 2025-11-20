import json
import time
from kafka import KafkaConsumer # type: ignore
import sys
import csv
import signal
from config import NUM_PRODUCERS, EVENTS_PER_PRODUCER, DISTRIBUTION as DIS

try:
    from config import BOOTSTRAP_SERVERS, RESULT_TOPIC
except ImportError:
    BOOTSTRAP_SERVERS = ['localhost:9092']
    RESULT_TOPIC = 'aggregated-results'

window_max_latencies = {}
consumer = None
shutdown = False

def write_latencies_to_csv(latency_data):
    filename = f"latency_report_{NUM_PRODUCERS}_{NUM_PRODUCERS * EVENTS_PER_PRODUCER}_{DIS}.csv"
    if not latency_data:
        print("No latency data to write.")
        return
    print(f"\nWriting max latencies to {filename}...")
    try:
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['window_id', 'max_latency_ms'])
            for window_id in sorted(latency_data.keys()):
                writer.writerow([window_id, latency_data[window_id]])
        print(f"Successfully wrote {len(latency_data)} window records.")
    except IOError as e:
        print(f"Error writing to CSV: {e}")

def handle_shutdown_signal(signum, frame):
    global shutdown
    shutdown = True
    print("\nShutdown signal received. Finalizing latency report...")

def latency_main():
    """Runs the Latency Calculator as a standalone script."""
    global window_max_latencies, consumer, shutdown

    # Catch CTRL+C clean shutdown
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)

    print(f"\n==============================")
    print(f"       LATENCY CALCULATOR     ")
    print(f"==============================")
    print(f"Subscribing to topic: {RESULT_TOPIC}")
    print(f"Bootstrap server(s): {BOOTSTRAP_SERVERS}\n")

    try:
        consumer = KafkaConsumer(
            RESULT_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=f"latency-calc-{int(time.time())}",
            value_deserializer=lambda x: x.decode('utf-8'),
            consumer_timeout_ms=2000
        )
    except Exception as e:
        print(f"CRITICAL: Unable to connect to Kafka: {e}")
        return

    print("Connected successfully. Waiting for result messages...\n")

    while not shutdown:
        message_batch = consumer.poll(timeout_ms=1000)
        if not message_batch:
            continue

        for partition, messages in message_batch.items():
            for message in messages:
                try:
                    payload = json.loads(message.value)
                except Exception:
                    continue

                result_ts = message.timestamp
                source_ts = payload.get("max_source_log_append_time_ms")
                wid = payload.get("window_id")

                print(f"Received Window ID: {wid} | Source TS: {source_ts} | Result TS: {result_ts}")

                if source_ts and result_ts and wid is not None:
                    latency = result_ts - source_ts // 1_000_000  # in milliseconds
                    curr_max = window_max_latencies.get(wid, -float('inf'))
                    if latency > curr_max:
                        window_max_latencies[wid] = latency
                        print(f"Window {wid} | Max Latency Updated → {latency} ms")

    # Shutdown Procedure
    if consumer:
        consumer.close()

    write_latencies_to_csv(window_max_latencies)
    print("\nLatency calculator stopped.\n")

if __name__ == "__main__":
    latency_main()
