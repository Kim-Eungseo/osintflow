# osintflow
간단한 ETL 프레임워크, MongoDB(필수), MySQL(선택)

## Getting started

본 레포를 clone 받아서 setup.py가 있는 path에서 다음의 명령어를 실행하여 설치할 수 있습니다.    
```  
pip install .   
```

## Storage configuration   

### MySQL   
yaml 파일로 작성    
```  
mysql_host: ""
mysql_user: ""
mysql_password: ""
mysql_database: ""
mysql_table: ""
mysql_port: 1234
```  
    
### MongoDB
yaml 파일로 작성     
```  
storage_type: mongodb
mongodb_uri: mongodb://localhost:27017/
mongodb_database: my-database
mongodb_collection: my-collection
```  

## 핵심 Decorator 별 설명
**osint.source_web**: 이 메서드는 url, data, method, mode, coding, consumes, inherit_cookies, new_session 등 다양한 인수를 입력받습니다. 이 메서드는 데코레이터 함수를 반환하며, 이 함수는 다른 함수를 래핑하는 데 사용됩니다. 데코레이터 함수는 지정된 URL에 GET 또는 POST 요청을 수행하고, 데이터를 적절하게 전달하여 응답을 self.data에 저장합니다. 그런 다음 래핑된 함수를 입력 인수와 함께 호출하고 결과를 반환합니다.

**osint.dataflow:** 이 메서드는 임의 개수의 핸들러를 입력으로 받아, self.data에 파이프라인 스타일로 순차적으로 적용하고 결과를 반환합니다.

**osint.store_mongo**: 이 메서드는 config_path를 입력으로 받아, 래핑된 함수의 반환 값에서 데이터를 가져와 구성 파일에서 지정된 storage_type을 확인하고 MongoDB 또는 MySQL에 데이터를 Upsert합니다.

**osint.store_log**: 이 메서드는 config_path를 입력으로 받아, 래핑된 함수의 반환 값에서 데이터를 가져와 구성 MongoDBO에 로깅 겸 메타 데이터를 저장합니다.

## Example 1: Threatfox ETL 파이프라인 기본 예시
```python  
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
```  


## Example 2: MITRE ATT&CK 데이터 ETL 파이프라인 응용 예시
```python
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

```

