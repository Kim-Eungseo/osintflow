import json
import uuid
from datetime import datetime

from osintflow.core import osint

COLLECTION_NAME = "osint.threatfox"
RANDOM_UUID = str(uuid.uuid4())


@osint.source_web("https://threatfox-api.abuse.ch/api/v1/", method='post', data='{"query": "get_iocs", "days": 1}')
@osint.dataflow(lambda data: json.loads(data)['data'])
@osint.store_mongo(configure="mongo_config.yaml")
@osint.store_log(
    configure="mongo_config.yaml",
    source_type=COLLECTION_NAME + ".log",
    _id=RANDOM_UUID,
    message="ended",
    execution_start_time=datetime.utcnow())
def get_threatfox():
    return osint.data


if __name__ == "__main__":
    get_threatfox()
    print("INFO: " + "threatfox done")
