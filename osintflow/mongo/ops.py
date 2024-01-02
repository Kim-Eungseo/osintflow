import hashlib
import json
from typing import List, Dict

from deepdiff import DeepDiff
from pymongo import UpdateOne
from pymongo.collection import Collection
from pymongo.results import BulkWriteResult


def upsert_or_revoke(documents: List[Dict], collection: Collection, compare_field: str):
    # if compare_field == "_id":
    #     raise Exception("_id cannot be as compare_field")

    def fill_empty_id_field(document: dict):
        if "_id" in document:
            return document
        document['_id'] = hashlib.sha256(json.dumps(document).encode()).hexdigest()
        return document

    documents = [fill_empty_id_field(document) for document in documents]
    found_results = list(collection.find(
        {"custom_raw_data." + compare_field: {'$in': [document['custom_raw_data'][compare_field]
                                                      for document in documents]}, 'revoked': False}
    ))
    result_as_dict = {result['custom_raw_data'][compare_field]: result for result in found_results}

    if len(result_as_dict.keys()) != len(found_results):
        raise Exception("Duplicated key exists in compare_field")

    bulk_operations = []
    for document in documents:
        exist_doc = result_as_dict[document['custom_raw_data'][compare_field]] \
            if document['custom_raw_data'][compare_field] in result_as_dict else None

        if exist_doc:
            if 'revoked' in exist_doc:
                del exist_doc['revoked']
            if DeepDiff(exist_doc['custom_raw_data'], document['custom_raw_data']):
                bulk_operations.append(UpdateOne({'_id': exist_doc['_id']}, {'$set': {'revoked': True}}, upsert=True))
                document['revoked'] = False
                bulk_operations.append(UpdateOne({'_id': document['_id']}, {'$set': document}, upsert=True))
        else:
            document['revoked'] = False
            bulk_operations.append(UpdateOne({'_id': document['_id']}, {'$set': document}, upsert=True))

    if bulk_operations:
        return collection.bulk_write(bulk_operations, ordered=False)
    return BulkWriteResult({}, False)
