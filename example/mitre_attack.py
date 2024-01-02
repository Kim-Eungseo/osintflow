import json
import uuid
from datetime import datetime

from osintflow.core import OsintflowJob

COLLECTION_NAME = "osint.mitre.ttp"

URL_PATH_DICT = {
    "enterprise_file": "enterprise-attack/enterprise-attack.json",
    "pre_attack_file": "pre-attack/pre-attack.json",
    "mobile_attack_file": "mobile-attack/mobile-attack.json",
    "ics_attack_file": "ics-attack/ics-attack.json"
}


def get_mitre_ttp(path):
    osint_job = OsintflowJob()
    random_uuid = str(uuid.uuid4())
    start_time = datetime.utcnow()

    @osint_job.source_web(
        "https://raw.githubusercontent.com/mitre/cti/master/<path>", data={"Path": "<path>"}
    )  # <path>을 통해 파라미터를 넘겨 받을 수 있음, data는 의미 없는 값이지만, 치환 예제 용도임.
    @osint_job.dataflow(
        lambda data: json.loads(data),
        lambda data: data['objects'])
    @osint_job.store_mongo(configure="mongo_config.yaml")
    @osint_job.store_log(
        configure="mongo_config.yaml",
        source_type=COLLECTION_NAME + ".log",
        _id=random_uuid,
        message="ended",
        execution_start_time=start_time)
    def _get_mitre_ttp(path):  # <path>로 들어가기 위한 파라미터
        return osint_job.data

    return _get_mitre_ttp(path=path)


if __name__ == "__main__":
    for source in list(URL_PATH_DICT.keys()):
        get_mitre_ttp(path=URL_PATH_DICT[source])  # path 파라미터 넘김, 단, named argument passing을 통해 넘겨야 함.
        print("INFO: " + "mitre ttp done")
