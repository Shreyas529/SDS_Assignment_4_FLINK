"""
Updated driver.py to run Spark or Flink.
Aligns start time to 10s boundary to prevent window fracturing.
"""

import multiprocessing
import time
import argparse
from producer import producer, ensure_topic, clear_topic
from consumer import consumer
from data_generation import create_ad_campaign_mappings
from latency_calculator import latency_generator
from databaseOps import DatabaseOps
from config import *


def main():
    parser = argparse.ArgumentParser(description="Run streaming CTR pipeline")
    parser.add_argument(
        "--impl",
        choices=["spark", "flink", "none"],
        default="flink",
        help="Streaming implementation to use (spark/flink/none)"
    )
    parser.add_argument("--skip-consumers", action="store_true")
    parser.add_argument("--num-producers", type=int, default=NUM_PRODUCERS)
    parser.add_argument("--events-per-producer", type=int, default=EVENTS_PER_PRODUCER)
    parser.add_argument("--duration", type=int, default=DURATION_SECONDS)
    args = parser.parse_args()

    print("="*70 + f"\nSTREAMING CTR PIPELINE WITH {args.impl.upper()}\n" + "="*70)

    # Topic Setup
    print("Setting up Kafka topics...")
    ensure_topic(EVENTS_TOPIC, PARTITIONS, REPLICATION_FACTOR)
    ensure_topic(RESULT_TOPIC, 1, REPLICATION_FACTOR)
    clear_topic(EVENTS_TOPIC, PARTITIONS, REPLICATION_FACTOR)
    clear_topic(RESULT_TOPIC, 1, REPLICATION_FACTOR)
    
    if args.impl == "none":
        ensure_topic(WATERMARK_TOPIC, 1, REPLICATION_FACTOR)
        clear_topic(WATERMARK_TOPIC, 1, REPLICATION_FACTOR)

    # Data Setup
    print("Preparing ad campaign mappings...")
    ad_campaign_map = create_ad_campaign_mappings()
    
    if not args.skip_consumers:
        print("Setting up PostgreSQL database...")
        db_obj = DatabaseOps()
        db_obj.create_tables()
        db_obj.truncate_tables()
        db_obj.insert_mappings(ad_campaign_map)
        db_obj.close()

    # FIX: Align base time to 10s boundary to match Flink windows
    base_event_time = int(time.time() // 10 * 10) - 10
    print(f"Base Event Time aligned to: {base_event_time}")

    # Producers
    print(f"\nSpawning {args.num_producers} producer(s)...")
    producer_processes = []
    for i in range(args.num_producers):
        p = multiprocessing.Process(
            target=producer,
            args=(f"Producer-{i+1}", args.events_per_producer, args.duration, ad_campaign_map, base_event_time)
        )
        producer_processes.append(p)

    # Consumers
    consumer_processes = []
    if not args.skip_consumers:
        print(f"Spawning {NUM_CONSUMERS} consumer(s)...")
        for i in range(NUM_CONSUMERS):
            p = multiprocessing.Process(target=consumer, args=(f"Consumer-{i+1}",))
            consumer_processes.append(p)

    # Processor
    processing_process = None
    if args.impl == "spark":
        print("Starting Spark processor (Structured Streaming)...")
        from spark_processor import run_spark_ctr_processor
        processing_process = multiprocessing.Process(target=run_spark_ctr_processor)
    elif args.impl == "flink":
        print("Starting Flink processor (DataStream API)...")
        try:
            from flink_processor import run_flink_ctr_processor
            processing_process = multiprocessing.Process(target=run_flink_ctr_processor)
        except ImportError:
            print("ERROR: Flink libraries not found.")
            processing_process = None
    else:
        print("No processor selected.")

    time.sleep(2)

    # Latency Calculator
    print("Starting latency calculator...")
    latency_stop_event = multiprocessing.Event()
    latency_generator_process = multiprocessing.Process(
        target=latency_generator, 
        args=(latency_stop_event,)
    )

    # Start Processes
    print("\n" + "="*70 + "\nSTARTING PIPELINE\n" + "="*70)
    
    latency_generator_process.start()
    time.sleep(1)
    
    if processing_process:
        processing_process.start()
        # Give Flink/Spark time to initialize
        time.sleep(10) 

    for c in consumer_processes:
        c.start()
    time.sleep(2)

    print("Starting producers...")
    for p in producer_processes:
        p.start()

    print("\nProducers running...")
    for p in producer_processes:
        p.join()
    print("✓ All producers finished")

    if consumer_processes:
        print("Waiting for consumers...")
        for c in consumer_processes:
            c.join()
        print("✓ All consumers finished")

    if processing_process:
        print("Waiting for processing to complete (60s timeout)...")
        processing_process.join(timeout=60)
        if processing_process.is_alive():
            print("⚠ Terminating processing processor...")
            processing_process.terminate()
            processing_process.join()
        print("✓ Processing finished")

    print("Signaling latency calculator to stop and write CSV...")
    latency_stop_event.set() 
    latency_generator_process.join(timeout=10) 
    
    if latency_generator_process.is_alive():
        print("⚠ Latency calculator stuck, forcing termination...")
        latency_generator_process.terminate()
        latency_generator_process.join()
    else:
        print("✓ Latency calculator finished gracefully")

    print("\n" + "="*70 + "\nPIPELINE COMPLETED\n" + "="*70)
    print("Results available in: latency_report.csv")


if __name__ == "__main__":
    main()