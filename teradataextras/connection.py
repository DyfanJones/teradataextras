import boto3
import os
from hashlib import sha256
from pathlib import Path
import pandas as pd
from datetime import datetime as dt, timedelta
from urllib.parse import urlparse


class BaseConnector:
    """
    DESCRIPTION:
        Base class for all connections
    """

    dstk_env = ""
    s3_bucket = None
    s3_key = None
    s3_client = None

    def __init__(self, cache=False, cache_location=os.getcwd(), cache_expire=7, **kwargs):
        """
        DESCRIPTION:
            Initialize BaseConnector class
        PARAMETERS:
            cache:
                Optional Argument.
                Specifies to cache database results
                Types: str

            cache_location:
                Optional Argument.
                Specifies the cache location, can be s3 uri or local directory.
                Types: str

            cache_expire:
                Optional Argument.
                Clear down and cache older than expiry date.
                Types:
        """

        if not isinstance(cache, bool):
            ValueError(f"`cache` is class `{type(cache)}`. Please set `cache` to boolean")

        if not isinstance(cache_location, str):
            ValueError(
                f"`cache_location` is class `{type(cache_location)}`. Please set `cache_location` to str"
            )

        self.cache = cache
        self.cache_location = cache_location.rstrip("/")
        if is_s3_uri(cache_location):
            _, bucket, key, _, _, _ = urlparse(cache_location)
            self.s3_bucket = bucket
            self.s3_key = key.lstrip("/")
        if self.cache:
            self._clear_cache(cache_expire)

    def execute(self, sql):
        raise NotImplementedError

    def to_sql(self, **kwargs):
        raise NotImplementedError

    def exist(self, table):
        raise NotImplementedError

    def drop_table(self, table):
        raise NotImplementedError

    def delete_from(self, table):
        raise NotImplementedError

    def disconnect(self):
        raise NotImplementedError

    @property
    def is_valid(self):
        raise NotImplementedError

    def _hash(self, sql):
        hash = sha256()
        hash.update(sql.strip().lower().encode("utf8"))
        return hash.hexdigest()

    def _write_sql_cache(self, cache_id, sql):
        file_path = self._file_builder(cache_id, "sql")
        sql = sql.strip().lower()
        if self.s3_bucket:
            self.s3_client.put_object(Bucket=self.s3_bucket, Key=file_path, Body=sql)
        else:
            path = Path(os.path.dirname(file_path))
            path.mkdir(parents=True, exist_ok=True)
            with open(file_path, 'a') as sql_cache:
                sql_cache.write(sql + "\n")

    def _write_data_cache(self, cache_id, data):
        file_path = self._file_builder(cache_id, "parquet")
        if not self.s3_bucket:
            path = Path(os.path.dirname(file_path))
            path.mkdir(parents=True, exist_ok=True)
        else:
            file_path = f"s3://{self.s3_bucket}/{file_path}"
        data.to_parquet(file_path)

    def _check_sql_cache(self, cache_id):
        file_path = self._file_builder(cache_id, "sql")
        if self.s3_bucket:
            result = True
            try:
                self.s3_client.head_object(Bucket=self.s3_bucket, Key=file_path)
            except:
                result = False
        else:
            result = os.path.exists(file_path)

        return result

    def _read_cache(self, cache_id):
        file_path = self._file_builder(cache_id, "parquet")
        if self.s3_bucket:
            file_path = f"s3://{self.s3_bucket}/{file_path}"
        return pd.read_parquet(file_path)

    def _file_builder(self, cache_id, suffix="sql"):
        if self.s3_bucket:
            file_path = self.s3_key
        else:
            file_path = self.cache_location
        file_path = "/".join([file_path, "cache", cache_id])
        return ".".join([file_path, suffix])

    def _clear_cache(self, cache_expire):
        now = dt.utcnow()
        min_cache = now - timedelta(days=cache_expire)
        cache_path = "/".join([self.cache_location, "cache"])
        if self.s3_bucket:
            cache_list = self._s3_file_info(cache_path)
            cache_list = cache_list[
                pd.to_datetime(cache_list["mtime"], utc=True) < pd.to_datetime(min_cache, utc=True)
            ]
            self._s3_unlink(cache_list.path)
        else:
            cache_list = file_info(cache_path)
            cache_list = cache_list[cache_list["mtime"] < min_cache]
            [os.remove(f) for f in cache_list.path]

    def _s3_file_info(self, path):
        kwargs = {
            "Bucket": self.s3_bucket,
            "Prefix": path if path.endswith("/") else path + "/",
            "Delimiter": "/",
            "ContinuationToken": "",
        }
        resp = []
        while isinstance(kwargs['ContinuationToken'], str):
            if kwargs["ContinuationToken"] == "":
                kwargs.pop("ContinuationToken")
            batch = self.s3_client.list_objects_v2(**kwargs)
            kwargs["ContinuationToken"] = batch.get("NextContinuationToken", None)
            resp.append(batch)

        s3_files = []
        for i in resp:
            for c in i.get("Contents", {}):
                out = {"path": c.get("Key"), "mtime": c.get("LastModified")}
                s3_files.append(out)
        if s3_files:
            df = pd.DataFrame(s3_files)
        else:
            df = pd.DataFrame({"path": [], "mtime": []})
            df["mtime"] = df["mtime"].astype('datetime64[ns]')
        return df

    def _s3_unlink(self, path):
        if len(path) == 0:
            return
        path = [{"Key": i} for i in path]
        key_parts = split_vec(path, 1000, len(path))
        for prt in key_parts:
            self.s3_client.delete_objects(Bucket=self.s3_bucket, Delete={"Objects": prt})


def is_s3_uri(s3_uri):
    if s3_uri.startswith("s3://"):
        return True
    return False


def file_info(path):
    df = pd.DataFrame({"path": [], "mtime": []})
    df["mtime"] = df["mtime"].astype('datetime64[ns]')
    if not os.path.exists(path):
        return df
    cache_list = [
        os.path.join(path, file) for file in os.listdir(path) if os.path.isfile(os.path.join(path, file))
    ]
    cache_list = [{"path": file, "mtime": dt.fromtimestamp(os.path.getmtime(file))} for file in cache_list]
    if cache_list:
        df = pd.DataFrame(cache_list)
    return df


def split_vec(vec, length, max_length):
    start = [i for i in range(0, max_length, length)]
    end = start[1:]
    end.append(max_length)
    grp = zip(start, end)
    return [vec[i:j] for i, j in grp]
