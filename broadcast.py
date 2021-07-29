import os
import json
from google.cloud import pubsub_v1


def broadcast(broadcast_data):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))
    tables = [
        "TopCampaignContent",
        "CampaignSummary",
    ]

    for table in tables:
        data = {
            "table": table,
            "start": broadcast_data.get("start"),
            "end": broadcast_data.get("end"),
        }
        message_json = json.dumps(data)
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes).result()

    return {
        "message_sent": len(tables),
    }