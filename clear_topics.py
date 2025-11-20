from producer import clear_topic
from config import *

clear_topic(topic=EVENTS_TOPIC, partitions=PARTITIONS, replication=REPLICATION_FACTOR, timestamp_type="CreateTime")
clear_topic(topic=RESULT_TOPIC, partitions=1, replication=REPLICATION_FACTOR, timestamp_type="LogAppendTime")
clear_topic(topic=WATERMARK_TOPIC, partitions=1, replication=REPLICATION_FACTOR)
