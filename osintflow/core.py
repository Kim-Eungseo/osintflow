import uuid
from datetime import datetime
from pangres import upsert, UnnamedIndexLevelsException
import pandas as pd
from sqlalchemy import create_engine
import requests
import yaml
from minio import Minio
from pymongo import MongoClient
import chardet
from pymongo.errors import InvalidOperation
from pymongo.results import BulkWriteResult

import osintflow.config as config
import osintflow.util as util
from osintflow.concurrent import _concurrent
from osintflow.mongo import ops as mongo_ops


class OsintflowJob:
    def __init__(self):
        self.data = None
        self._cookies = None
        self.stored_mongo_log: BulkWriteResult = None
        self._session = requests.Session()

    def register(self, func):
        exec(f'self.{func.__name__} = func', globals(), locals())

    def source_web(self, url, data=None, method='get', mode='t', coding='utf-8', consumes=None, inherit_cookies=False,
                   new_session=False, auth=None, headers=None, cookie=None, proxies=None, params=None):
        if cookie is not None:
            self._cookies = cookie
        if headers is None:
            headers = {}

        def wrapper(func):
            def inner_wrapper(*args, **kwargs):
                global response
                if new_session:
                    self._session = requests.Session()
                _url, _coding, _data = self._replace_all_params(kwargs, url, coding, data)
                headers.update(config.headers)
                if consumes is not None:
                    headers['Content-Type'] = consumes
                if method.lower() == 'post':
                    response = self._session.post(_url, headers=headers, data=_data,
                                                  cookies=self._cookies if inherit_cookies else None,
                                                  auth=auth, proxies=proxies)
                elif method.lower() == 'get':
                    response = self._session.get(
                        _url + (f'?{"&".join([k + "=" + _data[k] for k in _data])}' if _data is not None else ''),
                        headers=config.headers if headers is None else headers,
                        cookies=self._cookies if inherit_cookies else None, auth=auth, proxies=proxies, params=params)
                if not inherit_cookies:
                    self._cookies = response.cookies
                self.data = response.content
                if mode == 't':
                    try:
                        encoding = chardet.detect(self.data)['encoding']
                        if encoding is not None:
                            encoding = str(encoding)
                            self.data = self.data.decode(encoding)
                    except UnicodeDecodeError as e:
                        print(e)
                return func(*args, **kwargs)

            inner_wrapper.__name__ = func.__name__
            return inner_wrapper

        return wrapper

    def source_mongo(self, configure, query):
        if isinstance(configure, str):
            with open(configure, 'r') as f:
                configure = yaml.load(f, Loader=yaml.FullLoader)

        def wrapper(func):
            def inner_wrapper(*args, **kwargs):
                self.data = self._get_data_from_mongo(configure, query)
                return func(*args, **kwargs)

            return inner_wrapper

        return wrapper

    def _get_data_from_mongo(self, configure: dict, query: dict) -> list:
        with MongoClient(configure['mongodb_uri']) as client:
            try:
                db = client[configure['mongodb_source_database']]
                return list(db[configure['mongodb_source_collection']].find(query))

            except Exception as e:
                print("ERROR: " + datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"), e)

    def dataflow(self, *handlers):
        def wrapper(func):
            def inner_wrapper(*args, **kwargs):
                self.data = self._handle(self.data, *handlers, **kwargs)
                return func(*args, **kwargs)

            inner_wrapper.__name__ = func.__name__
            return inner_wrapper

        return wrapper

    def _handle(self, data, *handlers, **kwargs):
        for handler in handlers:
            if type(handler) == list:
                result = []
                for single in handler:
                    result.append(self._handle(data, single))
                data = result
            elif type(handler) == dict:
                result = {}
                for key in handler:
                    single = handler[key]
                    result_key, = self._replace_all_params(kwargs, self._handle(data, key))
                    result[result_key] = self._handle(
                        data, single)
                data = result
            elif callable(handler):
                data = handler(data)
            else:
                return handler
        return data

    def _replace_param(self, s, **kwargs):
        if type(s) == str:
            return util.parse_template(s, kwargs, globals(), locals())
        elif type(s) == list:
            result = []
            for item in s:
                result.append(self._replace_param(item, **kwargs))
            return result
        elif type(s) == dict:
            result = {}
            for key in s:
                result[self._replace_param(key, **kwargs)] = self._replace_param(s[key], **kwargs)
            return result

    def _replace_all_params(self, to_repl, *args):
        new_args = []
        for arg in args:
            new_args.append(self._replace_param(arg, **to_repl))
        return new_args

    def store_mongo(self, configure):
        if isinstance(configure, str):
            with open(configure, 'r') as f:
                configure = yaml.load(f, Loader=yaml.FullLoader)

        def wrapper(func):
            def inner_wrapper(*args, **kwargs):
                # Call the original function
                if self.data is None:
                    raise ValueError("No data to store")

                self.stored_mongo_log = self._store_mongo(configure, self.data)

                # Return the original function's result
                data = func(*args, **kwargs)
                self.data = data
                return data

            return inner_wrapper

        return wrapper

    def store_mysql(self,
                    configure,
                    dtype=None,  # https://github.com/ThibTrip/pangres/wiki/Upsert#11-creating-a-sql-table
                    compare_field=None,
                    if_row_exists="update",
                    table_name=None,
                    append=False):
        if isinstance(configure, str):
            with open(configure, 'r') as f:
                configure = yaml.load(f, Loader=yaml.FullLoader)

        def wrapper(func):
            def inner_wrapper(*args, **kwargs):
                # Call the original function
                if self.data is None:
                    raise ValueError("No data to store")

                if not append:
                    self.__upsert_mysql(configure,
                                        self.data,
                                        dtype=dtype,  # same logic as the parameter in pandas.to_sql
                                        compare_field=compare_field,
                                        if_row_exists=if_row_exists,
                                        table_name=table_name)
                else:
                    batch_size = 5000
                    for i in range(0, len(self.data), batch_size):
                        batch = self.data[i:i+batch_size]
                        self.__append_mysql(configure, batch, table_name)

                # Return the original function's result
                data = func(*args, **kwargs)
                self.data = data
                return data

            return inner_wrapper

        return wrapper

    def store_log(self, configure, **kwargs):
        params = kwargs
        if isinstance(configure, str):
            with open(configure, 'r') as f:
                configure = yaml.load(f, Loader=yaml.FullLoader)

        def wrapper(func):
            def inner_wrapper(*args, **kwargs):
                # Call the original function
                data = func(*args, **kwargs)
                if data is None:
                    raise ValueError("No data to store")

                upserted_count = 0
                try:
                    upserted_count = self.stored_mongo_log.upserted_count if self.stored_mongo_log is not None else 0
                except InvalidOperation as e:
                    print(e)

                log = {"_id": params['_id'] if '_id' in params else str(uuid.uuid4()),
                       "source_type": params['source_type'] if 'source_type' in params else "",
                       "timestamp": params['timestamp'] if 'timestamp' in params else datetime.utcnow(),
                       "timestamp_store": params[
                           'timestamp_store'] if 'timestamp_store' in params else datetime.utcnow(),
                       "owner": params['owner'] if 'owner' in params else "Ngseo Kim",
                       "log": {
                           "message": params['message'] if 'message' in params else "",
                           "stats": {
                               "crawl_counts": len(self.data) if self.data is not None else 0,
                               "upsert_counts": upserted_count
                           },
                           "execution_end_time":
                               params['execution_end_time_callable']()
                               if "execution_end_time_callable" in params else datetime.utcnow()
                       }}

                if "execution_start_time" in params:
                    log['log']['execution_start_time'] = params['execution_start_time']

                self._store_mongo(configure, log, logging=True)
                return data

            return inner_wrapper

        return wrapper

    def _store_mongo(self, configure, data, logging=None, **kwargs):
        if len(data) == 0:
            print("WARNING: no data")
            return

        with MongoClient(configure['mongodb_uri']) as client:
            db = client[configure['mongodb_database']]
            collection = db[configure['mongodb_collection']]

            if logging:
                collection.insert_one(data)
                return

            if type(data) == dict and data:
                return mongo_ops.upsert_or_revoke([data], collection,
                                                  configure['mongodb_compare_field'])

            elif type(data) == list and data:
                unique_dicts = [i for n, i in enumerate(data) if i not in data[n + 1:]]
                return mongo_ops.upsert_or_revoke(unique_dicts, collection,
                                                  configure['mongodb_compare_field'])
            else:
                raise ValueError("Invalid data to store: make sure data is not empty dict or empty list")

    def __append_mysql(self, configure, data, table_name=None):
        if len(data) == 0:
            print("WARNING: no data")
            return

        db_url = "mysql://{}:{}@{}:{}/{}".format(
            configure['mysql_user'],
            configure['mysql_password'],
            configure['mysql_host'],
            configure['mysql_port'],
            configure['mysql_database']
        )
        engine = create_engine(db_url)

        df: pd.DataFrame
        if type(data) == dict and data:
            df = pd.DataFrame([data])

        elif type(data) == list and data:
            df = pd.DataFrame(data)

        else:
            raise ValueError("Invalid data to store: make sure data is not an empty dict or an empty list")

        df.to_sql(configure['mysql_table'] if not table_name else table_name,
                  engine,
                  if_exists='append',
                  index=False)

    def __upsert_mysql(self,
                       configure,
                       data,
                       dtype=None,  # same logic as the parameter in pandas.to_sql
                       compare_field=None,
                       if_row_exists="update",
                       table_name=None,
                       logging=None,
                       **kwargs):
        if len(data) == 0:
            print("WARNING: no data")
            return

        db_url = "mysql://{}:{}@{}:{}/{}".format(
            configure['mysql_user'],
            configure['mysql_password'],
            configure['mysql_host'],
            configure['mysql_port'],
            configure['mysql_database']
        )
        engine = create_engine(db_url)

        df: pd.DataFrame
        if type(data) == pd.DataFrame:
            df = data

        elif type(data) == dict and data:
            df = pd.DataFrame([data])

        elif type(data) == list and data:
            df = pd.DataFrame(data)

        else:
            raise ValueError("Invalid data to store: make sure data is not an empty dict or an empty list")

        if compare_field:
            df.reset_index(drop=True, inplace=True)
            df.set_index(compare_field, inplace=True)
            df.index.set_names([compare_field], inplace=True)
        try:
            upsert(
                con=engine,
                df=df,
                table_name=configure['mysql_table'] if not table_name else table_name,
                if_row_exists=if_row_exists,
                dtype=dtype,  # same logic as the parameter in pandas.to_sql
                chunksize=1000,
                create_table=True  # create a new table if it does not exist
            )
        except UnnamedIndexLevelsException as e:
            raise ValueError("You should put compare column name to upsert on table")

    def _store_minio(self, configure, data):
        # TODO: need to check GDP storing convention
        client = Minio(
            configure['minio_endpoint'],
            access_key=configure['minio_access_key'],
            secret_key=configure['minio_secret_key'],
            secure=configure['minio_secure']
        )
        client.put_object(
            bucket_name=configure['minio_bucket'],
            object_name=data.get('filename', 'data.bin'),
            data=data,
            length=len(data),
        )


osint = OsintflowJob()
osint.thread = _concurrent.thread
osint.wait = _concurrent._wait
