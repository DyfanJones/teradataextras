# README #

**Disclaimer:** `teradataextras` is not a product of or supported by Teradata.

This is a thin wrapper around `teradataml`. This goal of this repo is to get better performance from teradata when working in python.

## installation
```
pip install git+ssh://git@github.com/DyfanJones/teradataextras.git
```

## Usage

```python
import pandas as pd

from teradataextras import Teradata
from getpass import getpass
```

```python
con = Teradata(host = "teradata", username = "user01", password = getpass())
```


```python
df = pd.read_parquet("s3://nyc-tlc/trip data/yellow_tripdata_2023-06.parquet")
df.shape
#> (3307234, 19)
```

## upload data from teradata

### Thin wrapper around teradaml copy_to_sql
```python
%%time
con.to_sql(
    df,
    table_name="dummy_tbl",
    if_exists = "replace"
)
# Failed to upload.

%%time
con.to_sql(
    df,
    table_name="dummy_tbl",
    if_exists = "replace",
    fastload = True
)
#> CPU times: user 2min 46s, sys: 9.64 s, total: 2min 56s
#> Wall time: 3min 39s
```

### New implementation for upload data to teradata
```python
%%time
con.to_sql_csv(
    df,
    table_name="dummy_tbl",
    if_exists = "replace"
)
#> CPU times: user 1min 24s, sys: 13.6 s, total: 1min 37s
#> Wall time: 2min 19s

%%time
con.to_sql_csv(
    df,
    table_name="dummy_tbl",
    if_exists = "replace",
    fastload = True
)
#> CPU times: user 1min 33s, sys: 9.98 s, total: 1min 43s
#> Wall time: 54.2 s
```

## retrieve data from teradata

### Basic wrapper around pd.read_sql_query
```python
%%time
df = con.query("select * from dummy_tbl")
#> CPU times: user 1min 59s, sys: 20.8 s, total: 2min 19s
#> Wall time: 2min 21s

%%time
df = con.query("select * from dummy_tbl", fastexport = True)
#> CPU times: user 1min 59s, sys: 19.6 s, total: 2min 18s
#> Wall time: 2min 17s
```

### New method for reading data from teradata
```python
%%time
df = con.query_csv("select * from dummy_tbl")
#> CPU times: user 22.6 s, sys: 6.42 s, total: 29.1 s
#> Wall time: 24.5 s

%%time
df = con.query_csv("select * from dummy_tbl", fastexport = True)
#> CPU times: user 21.7 s, sys: 5.44 s, total: 27.2 s
#> Wall time: 22.4 s
``` 
