import uuid 
import random
from config import *
import numpy as np

def create_ad_campaign_mappings():
    """
    Generates a fixed mapping of ad_ids (as UUIDs) to campaign_ids (as Integers).
    Returns:
        A dictionary mapping {ad_id_uuid_str: campaign_id_int}.
    """
    # print("Creating ad-to-campaign mappings with UUIDs...")
    mappings = {}
    for campaign_int in range(1, NUM_CAMPAIGNS + 1):
        for _ in range(ADS_PER_CAMPAIGN):
            ad_id = str(uuid.uuid4())
            mappings[ad_id] = campaign_int
    # print(f"Generated {len(mappings)} unique ad mappings.")
    return mappings

# data_generation.py — patch for generate_event_batch
# (replace the existing generate_event_batch implementation)

def generate_event_batch(throughput, ad_campaign_map, logical_time_sec, base_event_time_sec):
    """
    Generates a batch of synthetic streaming events for a single logical second.
    Args:
        throughput (int): Fixed number of events to generate in this second.
        ad_campaign_map (dict): {ad_id_uuid: campaign_id_int}.
        logical_time_sec (int): The base timestamp (in seconds) for the batch.
        base_event_time_sec (int): Global starting reference time in seconds.
    Returns:
        A list of event dictionaries.
    """
    ad_ids = list(ad_campaign_map.keys())
    events = []
    base_event_time_ns = int(base_event_time_sec * 1e9)

    # Window length in ns (keep in sync with your Flink tumbling window)
    WINDOW_NS = 10 * 1_000_000_000

    # Generate exponential inter-arrival times
    inter_arrivals = np.random.exponential(scale=1.0, size=throughput)
    arrival_times = np.cumsum(inter_arrivals)
    # Normalize into [0,1)
    if len(arrival_times) > 0:
        arrival_times = arrival_times / arrival_times[-1]
    else:
        arrival_times = []
    offsets = arrival_times

    # compute base window index (so window_id is relative to the base window)
    base_window_idx = base_event_time_ns // WINDOW_NS

    for offset in offsets:
        ad_id = random.choice(ad_ids)
        campaign_id = ad_campaign_map.get(ad_id, -1)

        event_time_ns = int((logical_time_sec + offset) * 1e9)

        # stable window id relative to base:
        window_idx = event_time_ns // WINDOW_NS
        window_id = int(window_idx - base_window_idx)

        event = {
            'user_id': str(uuid.uuid4()),
            'page_id': str(uuid.uuid4()),
            'ad_id': ad_id,
            'campaign_id': campaign_id,
            'ad_type': random.choice(AD_TYPES),
            'event_type': random.choice(EVENT_TYPES),
            'event_time_ns': event_time_ns,
            'window_id': window_id,
            'ip_address': ".".join(str(random.randint(0, 255)) for _ in range(4))
        }
        events.append(event)

    return events


if __name__ == "__main__":
    # Test the data generation functions
    ad_campaign_map = create_ad_campaign_mappings()
    event_batch = generate_event_batch(100, ad_campaign_map, 2, 0)
    print(f"Generated {len(event_batch)} events.")
    if len(event_batch) > 0:
        print(f"Sample event: {event_batch[0]}")