from pyflink.table import TableEnvironment, EnvironmentSettings
from config import *
import time

# ============================
#   FLINK TABLE API PIPELINE
# ============================

print("Initializing Flink Table Environment...")
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# --- Global Flink & Kafka Settings ---
t_env.get_config().set("table.exec.source.idle-timeout", "10 s")
t_env.get_config().set(
    "pipeline.jars",
    f"file:///home/shreyasarun/Documents/Sem-7/SDS/Assignment-4/libs/flink-sql-connector-kafka-4.0.1-2.0.jar"
)
t_env.get_config().set("parallelism.default", "1")
t_env.get_config().set("rest.bind-address", "0.0.0.0")
t_env.get_config().set("rest.port", "8085")

# === Constants ===
KAFKA_BOOTSTRAP_SERVERS = BOOTSTRAP_SERVERS
INPUT_TOPIC = EVENTS_TOPIC
OUTPUT_TOPIC = RESULT_TOPIC

# Give Kafka time if producers just started
time.sleep(5)

# ======================================
# 1) SOURCE TABLE (with event_time_ns)
# ======================================

print(f"Configuring Kafka Source: {INPUT_TOPIC}")
t_env.execute_sql(f"""
CREATE TABLE kafka_source (
    user_id STRING,
    page_id STRING,
    ad_id STRING,
    campaign_id INT,
    ad_type STRING,
    event_type STRING,
    event_time_ns BIGINT,
    window_id BIGINT,
    ip_address STRING,

    -- Convert ns → TIMESTAMP (from producer clock)
    event_time AS CAST(TO_TIMESTAMP_LTZ(event_time_ns / 1000000, 3) AS TIMESTAMP(3)),

    -- Watermark based on the event_time
    WATERMARK FOR event_time AS event_time - INTERVAL '3' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = '{INPUT_TOPIC}',
  'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'flink_sql_consumer',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);
""")

# ======================================
# 2) SINK TABLE
# ======================================

print(f"Configuring Kafka Sink: {OUTPUT_TOPIC}")
t_env.execute_sql(f"""
CREATE TABLE kafka_sink (
    window_id BIGINT,
    window_start STRING,
    campaign_id INT,
    ctr DOUBLE,
    max_source_log_append_time_ms BIGINT -- using producer event_time_ns
) WITH (
  'connector' = 'kafka',
  'topic' = '{OUTPUT_TOPIC}',
  'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
  'format' = 'json'
);
""")

# ======================================
# 3) AGGREGATION + CTR + LATENCY
# ======================================

print("Executing aggregation and writing results...")

t_env.execute_sql(f"""
INSERT INTO kafka_sink
SELECT
    window_id,
    CAST(TUMBLE_START(event_time, INTERVAL '10' SECOND) AS STRING) AS window_start,
    campaign_id,

    -- CTR Calculation
    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) * 1.0 /
    (SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) + 1) AS ctr,

    -- Convert MAX(event_time) (TIMESTAMP) → BIGINT milliseconds
    MAX(event_time_ns) AS max_source_log_append_time_ms

FROM kafka_source
GROUP BY
    window_id,
    TUMBLE(event_time, INTERVAL '10' SECOND),
    campaign_id
""").wait()

print("Flink job started and writing to Kafka!")
print("➤ Source Topic:", INPUT_TOPIC)
print("➤ Result Topic:", OUTPUT_TOPIC)
