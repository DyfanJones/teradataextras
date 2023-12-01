from __future__ import absolute_import

import json
import os
import tempfile
from datetime import datetime
from importlib.util import find_spec
from logging import getLogger
from random import sample
from string import ascii_letters
from urllib.parse import quote_plus

import pandas as pd
import polars as pl
from packaging import version
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.exc import OperationalError as sqlachemyOperationalError
from teradataml import (
    __version__ as tdml_version,
)
from teradataml import (
    create_context,
    get_context,
    remove_context,
)
from teradataml import (
    fastload as Fastload,
)
from teradataml.common.exceptions import TeradataMlException
from teradataml.common.messagecodes import ErrorInfoCodes, MessageCodes
from teradataml.common.messages import Messages
from teradataml.dataframe.copy_to import copy_to_sql
from teradatasqlalchemy import (
    BIGINT,
    BYTEINT,
    DATE,
    FLOAT,
    INTEGER,
    SMALLINT,
    TIMESTAMP,
    VARCHAR,
)
from teradatasqlalchemy import (
    __version__ as tdsql_version,
)
from teradatasqlalchemy.dialect import TDCreateTablePost as post

from teradataextras.connection import BaseConnector

logger = getLogger(__name__)


class Teradata(BaseConnector):
    """
    DESCRIPTION:
        Class to connect to Database
    """

    _metadata = None
    if_exists = None
    table_name = None

    def __init__(
        self,
        host=None,
        username=None,
        password=None,
        database=None,
        encryptdata=True,
        lob_support=True,
        **kwargs,
    ):
        """
        DESCRIPTION:
            Creates a connection to the Teradata Vantage using the teradatasql + teradatasqlalchemy DBAPI and dialect
            combination. Users can pass all required parameters (host, username, password) for establishing a connection to
            Vantage, or pass a sqlalchemy engine to the tdsqlengine parameter to override the default DBAPI and dialect
            combination.

        PARAMETERS:
            host:
                Optional Argument.
                Specifies the fully qualified domain name or IP address of the Teradata System.
                Types: str

            username:
                Optional Argument.
                Specifies the username for logging onto the Teradata Vantage.
                Types: str

            password:
                Optional Argument.
                Specifies the password required for the username.
                Types: str

            database:
                Optional Argument.
                Specifies the initial database to use after logon, instead of the user's default database.
                Types: str

            encryptdata:
                Optional Argument.
                Controls encryption of data exchanged between the driver and the database.
                Equivalent to the Teradata JDBC Driver ENCRYPTDATA connection parameter.
                Types: boolean

            lob_support:
                Optional Argument.
                Controls LOB support. Equivalent to the Teradata JDBC Driver LOB_SUPPORT connection parameter.
                Types: boolean

            **kwargs:
                Optional kwargs.
                Additional arguements for BaseConnector and create_context

        RETURNS:
            A Teradata sqlalchemy engine object.

        RAISES:
            TeradataMlException
        """

        super_kwargs = {}
        if "cache" in kwargs:
            super_kwargs["cache"] = kwargs.pop("cache")

        if "cache_location" in kwargs:
            super_kwargs["cache_location"] = kwargs.pop("cache_location")

        if "cache_expire" in kwargs:
            super_kwargs["cache_expire"] = kwargs.pop("cache_expire")

        create_context(
            host=host,
            username=username,
            password=quote_plus(password),
            database=database,
            encryptdata=encryptdata,
            lob_support=lob_support,
            **kwargs,
        )

        # build metadata
        self._metadata = {
            "dbname": type(self).__name__,
            "db_version": self.execute(
                "SELECT infodata FROM DBC.DBCINFO where infokey = 'version'"
            ).fetchone()[0],
            "session_info": self.execute("SELECT SESSION").fetchone()[0],
            "teradataml": tdml_version,
            "teradatasql": tdsql_version,
            "url": str(get_context().url),
        }
        super(Teradata, self).__init__(**super_kwargs)

    def query(self, sql, reset_cache=False, fastexport=False):
        """
        DESCRIPTION:
            Get query from Teradata and return pandas DataFrame.

        PARAMETERS:
            sql:
                Required Argument.
                Specifies the Teradata Vantage SQL query referenced by this DataFrame.
                Only "SELECT" queries are supported. Exception will be
                raised, if any other type of query is passed.
                Unsupported queries include:
                    1. DDL Queries like CREATE, DROP, ALTER etc.
                    2. DML Queries like INSERT, UPDATE, DELETE etc. except SELECT.
                    3. SELECT query with ORDER BY clause is not supported.
                Types: str

            reset_cache:
                Optional Argument.
                Reset data cache
                Types: boolean

            fastexport:
                Optional Argument.
                Specifies whether fastexport protocol
                Types: boolean

        RETURNS:
            A pandas DataFrame.
        """
        self._check_connection_error()

        # read cache
        if self.cache:
            cache_id = self._hash(sql)
            if not reset_cache:
                if self._check_sql_cache(cache_id):
                    return self._read_cache(cache_id)

        if fastexport:
            sql = "{0} {1}".format("{fn teradata_try_fastexport}", sql)

        try:
            engine = get_context()
            df = pd.read_sql_query(sql, engine)
        except sqlachemyOperationalError as err:
            logger.error(self._log_metadata())
            raise err

        # write to cache
        if self.cache:
            self._write_sql_cache(cache_id, sql.strip().lower())
            self._write_data_cache(cache_id, df)
        return df

    def query_csv(self, sql, csv_file_name=None, cache_time=0, fastexport=False):
        """
        DESCRIPTION:
            Get query from Teradata and return pandas DataFrame.

        PARAMETERS:
            sql:
                Required Argument.
                Specifies the Teradata Vantage SQL query referenced by this DataFrame.
                Only "SELECT" queries are supported. Exception will be
                raised, if any other type of query is passed.
                Unsupported queries include:
                    1. DDL Queries like CREATE, DROP, ALTER etc.
                    2. DML Queries like INSERT, UPDATE, DELETE etc. except SELECT.
                    3. SELECT query with ORDER BY clause is not supported.
                Types: str

            csv_file_name:
                Optional Argument.
                Location for Teradata to output csv results.
                Types: str

            cache_time:
                Optional Argument.
                Number of minutes since last query execution.
                Types: Int

            fastexport:
                Optional Argument.
                Specifies whether fastexport protocol
                Types: boolean

        RETURNS:
            A pandas DataFrame.
        """
        method = "pyarrow"
        if not find_spec("pyarrow") or version.parse(pd.__version__) < version.parse("1.4.0"):
            method = "python"

        if csv_file_name:
            if cache_time > 0 and os.path.exists(csv_file_name):
                file_info = os.path.getctime(csv_file_name)
                file_age = (datetime.now() - datetime.fromtimestamp(file_info)).total_seconds()
                cache_time < file_age / 60
                return pd.read_csv(csv_file_name, engine=method)

        use_temp_file = False
        if not csv_file_name:
            temp_file = tempfile.NamedTemporaryFile()
            csv_file_name = temp_file.name
            use_temp_file = True
            logger.debug(f"Create temporary CSV File: {csv_file_name}")

        fast_export = "{fn teradata_try_fastexport}" if fastexport else ""
        write_csv = f"{{fn teradata_write_csv({csv_file_name})}}"
        query = f"{fast_export}{write_csv}{sql}"

        try:
            engine = get_context()
            cur = engine.execute(query)
            cur.fetchall()
            cur.close()
        except sqlachemyOperationalError as err:
            logger.error(self._log_metadata())
            raise err

        try:
            return pd.read_csv(csv_file_name, engine=method)
        finally:
            if use_temp_file:
                temp_file.close()

    def execute(self, sql):
        """
        DESCRIPTION:
            Execute sql query on Teradata.

        PARAMETERS:
            sql:
                Required Argument.
                SQL query to be executed on Teradata
                Types: str

        RETURNS:
            sqlalchemy.engine.result.ResultProxy
        """

        try:
            return get_context().execute(sql)
        except sqlachemyOperationalError as err:
            logger.error(self._log_metadata())
            raise err

    def to_sql(
        self,
        df,
        table_name,
        if_exists="fail",
        primary_index=None,
        temporary=False,
        schema_name=None,
        types=None,
        primary_time_index_name=None,
        timecode_column=None,
        timebucket_duration=None,
        timezero_date=None,
        columns_list=None,
        sequence_column=None,
        seq_max=None,
        set_table=False,
        fastload=False,
        batch_size=None,
    ):
        """
        DESCRIPTION:
            Writes records stored in a teradataml DataFrame to Teradata Vantage.

        PARAMETERS:
            df:
                Required Argument.
                Specifies the Pandas DataFrame object to be saved in Vantage.
                Types: pandas.DataFrame

            table_name:
                Required Argument.
                Specifies the name of the table to be created in Teradata Vantage.
                Types: str

            if_exists:
                Optional Argument.
                Specifies the action to take when table already exists in Teradata Vantage.
                Default Value: 'fail'
                Permitted Values: 'fail', 'replace', 'append'
                    - fail: If table exists, do nothing.
                    - replace: If table exists, drop it, recreate it, and insert data.
                    - append: If table exists, insert data. Create if does not exist.
                Types: str

                Note: Replacing a table with the contents of a teradataml DataFrame based on
                      the same underlying table is not supported.

            primary_index:
                Optional Argument.
                Creates Teradata Table(s) with Primary index column(s) when specified.
                When None, No Primary Index Teradata tables are created.
                Default Value: None
                Types: str or List of Strings (str)
                    Example:
                        primary_index = 'my_primary_index'
                        primary_index = ['my_primary_index1', 'my_primary_index2', 'my_primary_index3']

            temporary:
                Optional Argument.
                Creates Teradata SQL tables as permanent or volatile.
                When True,
                1. volatile tables are created, and
                2. schema_name is ignored.
                When False, permanent tables are created.
                Default Value: False
                Types: boolean

            schema_name:
                Optional Argument.
                Specifies the name of the SQL schema in Teradata Vantage to write to.
                Default Value: None (Use default Teradata Vantage schema).
                Types: str

                Note: schema_name will be ignored when temporary=True.

            types:
                Optional Argument.
                Specifies required data-types for requested columns to be saved in Vantage.
                Types: Python dictionary ({column_name1: sqlalchemy_teradata.types, ... column_nameN: sqlalchemy_teradata.types})
                Default: None
                    Example:
                        # Import known sqlalchemy_teradata.types
                        from sqlalchemy_teradata import DECIMAL, VARCHAR
                        types = {"column_name1": DECIMAL(8,2), "column2": VARCHAR(20)}

                        # Import sqlalchemy_teradata to get types
                        import sqlalchemy_teradata as sql_td
                        types = {"column_name1": sql_td.DECIMAL(8,2), "column2": sql_td.VARCHAR(20)}

                Note:
                    1. This argument accepts a dictionary of columns names and their required teradatasqlalchemy types
                       as key-value pairs, allowing to specify a subset of the columns of a specific type.
                       When only a subset of all columns are provided, the column types for the rest are retained.
                       When types argument is not provided, the column types are retained.
                    2. This argument does not have any effect when the table specified using table_name and schema_name
                       exists and if_exists = 'append'.

            primary_time_index_name:
                Optional Argument.
                Specifies a name for the Primary Time Index (PTI) when the table
                to be created must be a PTI table.
                Types: String

                Note: This argument is not required or used when the table to be created
                      is not a PTI table. It will be ignored if specified without the timecode_column.

            timecode_column:
                Optional Argument.
                Required when the DataFrame must be saved as a PTI table.
                Specifies the column in the DataFrame that reflects the form
                of the timestamp data in the time series.
                This column will be the TD_TIMECODE column in the table created.
                It should be of SQL type TIMESTAMP(n), TIMESTAMP(n) WITH TIMEZONE, or DATE,
                corresponding to Python types datetime.datetime or datetime.date.
                Types: String

                Note: When you specify this parameter, an attempt to create a PTI table
                      will be made. This argument is not required when the table to be created
                      is not a PTI table. If this argument is specified, primary_index will be ignored.

            timebucket_duration:
                Optional Argument.
                Required if columns_list is not specified or is None.
                Used when the DataFrame must be saved as a PTI table.
                Specifies a duration that serves to break up the time continuum in
                the time series data into discrete groups or buckets.
                Specified using the formal form time_unit(n), where n is a positive
                integer, and time_unit can be any of the following:
                CAL_YEARS, CAL_MONTHS, CAL_DAYS, WEEKS, DAYS, HOURS, MINUTES,
                SECONDS, MILLISECONDS, or MICROSECONDS.
                Types:  String

                Note: This argument is not required or used when the table to be created
                      is not a PTI table. It will be ignored if specified without the timecode_column.

            timezero_date:
                Optional Argument.
                Used when the DataFrame must be saved as a PTI table.
                Specifies the earliest time series data that the PTI table will accept;
                a date that precedes the earliest date in the time series data.
                Value specified must be of the following format: DATE 'YYYY-MM-DD'
                Default Value: DATE '1970-01-01'.
                Types: String

                Note: This argument is not required or used when the table to be created
                      is not a PTI table. It will be ignored if specified without the timecode_column.

            columns_list:
                Optional Argument.
                Required if timebucket_duration is not specified.
                Used when the DataFrame must be saved as a PTI table.
                Specifies a list of one or more PTI table column names.
                Types: String or list of Strings

                Note: This argument is not required or used when the table to be created
                      is not a PTI table. It will be ignored if specified without the timecode_column.

            sequence_column:
                Optional Argument.
                Used when the DataFrame must be saved as a PTI table.
                Specifies the column of type Integer containing the unique identifier for
                time series data readings when they are not unique in time.
                * When specified, implies SEQUENCED, meaning more than one reading from the same
                  sensor may have the same timestamp.
                  This column will be the TD_SEQNO column in the table created.
                * When not specified, implies NONSEQUENCED, meaning there is only one sensor reading
                  per timestamp.
                  This is the default.
                Types: str

                Note: This argument is not required or used when the table to be created
                      is not a PTI table. It will be ignored if specified without the timecode_column.

            seq_max:
                Optional Argument.
                Used when the DataFrame must be saved as a PTI table.
                Specifies the maximum number of sensor data rows that can have the
                same timestamp. Can be used when 'sequenced' is True.
                Accepted range:  1 - 2147483647.
                Default Value: 20000.
                Types: int

                Note: This argument is not required or used when the table to be created
                      is not a PTI table. It will be ignored if specified without the timecode_column.

            set_table:
                Optional Argument.
                Specifies a flag to determine whether to create a SET or a MULTISET table.
                When True, a SET table is created.
                When False, a MULTISET table is created.
                Default value: False
                Types: boolean

                Note: 1. Specifying set_table=True also requires specifying primary_index or timecode_column.
                      2. Creating SET table (set_table=True) may result in loss of duplicate rows.
                      3. This argument has no effect if the table already exists and if_exists='append'.

            fastload:
                Optional Argument.
                Writes records from a Pandas DataFrame to Teradata Vantage
                using Fastload. FastLoad API can be used to quickly load large amounts of data
                in an empty table on Vantage.
                Types: boolean

            batch_size:
                Optional Argument.
                Specifies the number of rows to be loaded in a batch. For better performance,
                recommended batch size is at least 100,000. batch_size must be a positive integer.
                If this argument is None, there are two cases based on the number of
                rows, say N in the dataframe 'df' as explained below:
                If N is greater than 100,000, the rows are divided into batches of
                equal size with each batch having at least 100,000 rows (except the
                last batch which might have more rows). If N is less than 100,000, the
                rows are inserted in one batch after notifying the user that insertion
                happens with degradation of performance.
                If this argument is not None, the rows are inserted in batches of size
                given in the argument, irrespective of the recommended batch size.
                The last batch will have rows less than the batch size specified, if the
                number of rows is not an integral multiples of the argument batch_size.
                Default Value: None
                Types: int

        RETURNS:
            A dict containing the following attributes:
                1. errors_dataframe: It is a Pandas DataFrame containing error messages
                thrown by fastload. DataFrame is empty if there are no errors.
                2. warnings_dataframe: It is a Pandas DataFrame containing warning messages
                thrown by fastload. DataFrame is empty if there are no warnings.
                3. errors_table: Name of the table containing errors. It is None, if
                argument save_errors is False.
                4. warnings_table: Name of the table containing warnings. It is None, if
                argument save_errors is False.

        RAISES:
            TeradataMlException
        """
        self._check_connection_error()

        # If no primary index is set then get first column
        if not primary_index:
            primary_index = df.columns[0]

        if fastload:
            try:
                return Fastload(
                    df=df,
                    table_name=table_name,
                    schema_name=schema_name,
                    if_exists=if_exists,
                    index=False,
                    index_label=None,
                    primary_index=primary_index,
                    types=types,
                    batch_size=batch_size,
                    save_errors=False,
                )
            except sqlachemyOperationalError as err:
                logger.error(self._log_metadata())
                raise err
        else:
            try:
                copy_to_sql(
                    df=df,
                    table_name=table_name,
                    schema_name=schema_name,
                    index=False,
                    index_label=None,
                    temporary=temporary,
                    primary_index=primary_index,
                    if_exists=if_exists,
                    types=types,
                    primary_time_index_name=primary_time_index_name,
                    timecode_column=timecode_column,
                    timebucket_duration=timebucket_duration,
                    timezero_date=timezero_date,
                    columns_list=columns_list,
                    sequence_column=sequence_column,
                    seq_max=seq_max,
                    set_table=set_table,
                )
                return {
                    "errors_dataframe": pd.DataFrame(),
                    "warnings_dataframe": pd.DataFrame(),
                    "errors_table": "",
                    "warnings_table": "",
                }
            except sqlachemyOperationalError as err:
                logger.error(self._log_metadata())
                raise err

    def to_sql_csv(
        self,
        df,
        table_name,
        if_exists="fail",
        primary_index=None,
        schema_name=None,
        types=None,
        set_table=False,
        fastload=False,
    ):
        """
        DESCRIPTION:
            Writes records stored in a teradataml DataFrame to Teradata Vantage.

        PARAMETERS:
            df:
                Required Argument.
                Specifies the Pandas DataFrame or Polars DataFrame object to be saved in Vantage.
                Types: pandas.DataFrame or polars.DataFrame

            table_name:
                Required Argument.
                Specifies the name of the table to be created in Teradata Vantage.
                Types: str

            if_exists:
                Optional Argument.
                Specifies the action to take when table already exists in Teradata Vantage.
                Default Value: 'fail'
                Permitted Values: 'fail', 'replace', 'append'
                    - fail: If table exists, do nothing.
                    - replace: If table exists, drop it, recreate it, and insert data.
                    - append: If table exists, insert data. Create if does not exist.
                Types: str

                Note: Replacing a table with the contents of a teradataml DataFrame based on
                        the same underlying table is not supported.

            primary_index:
                Optional Argument.
                Creates Teradata Table(s) with Primary index column(s) when specified.
                When None, No Primary Index Teradata tables are created.
                Default Value: None
                Types: str or List of Strings (str)
                    Example:
                        primary_index = 'my_primary_index'
                        primary_index = ['my_primary_index1', 'my_primary_index2', 'my_primary_index3']

            schema_name:
                Optional Argument.
                Specifies the name of the SQL schema in Teradata Vantage to write to.
                Default Value: None (Use default Teradata Vantage schema).
                Types: str

                Note: schema_name will be ignored when temporary=True.

            types:
                Optional Argument.
                Specifies required data-types for requested columns to be saved in Vantage.
                Types: Python dictionary ({column_name1: sqlalchemy_teradata.types, ... column_nameN: sqlalchemy_teradata.types})
                Default: None
                    Example:
                        # Import known sqlalchemy_teradata.types
                        from sqlalchemy_teradata import DECIMAL, VARCHAR
                        types = {"column_name1": DECIMAL(8,2), "column2": VARCHAR(20)}

                        # Import sqlalchemy_teradata to get types
                        import sqlalchemy_teradata as sql_td
                        types = {"column_name1": sql_td.DECIMAL(8,2), "column2": sql_td.VARCHAR(20)}

                Note:
                    1. This argument accepts a dictionary of columns names and their required teradatasqlalchemy types
                        as key-value pairs, allowing to specify a subset of the columns of a specific type.
                        When only a subset of all columns are provided, the column types for the rest are retained.
                        When types argument is not provided, the column types are retained.
                    2. This argument does not have any effect when the table specified using table_name and schema_name
                        exists and if_exists = 'append'.

            set_table:
                Optional Argument.
                Specifies a flag to determine whether to create a SET or a MULTISET table.
                When True, a SET table is created.
                When False, a MULTISET table is created.
                Default value: False
                Types: boolean

                Note: 1. Specifying set_table=True also requires specifying primary_index or timecode_column.
                        2. Creating SET table (set_table=True) may result in loss of duplicate rows.
                        3. This argument has no effect if the table already exists and if_exists='append'.

            fastload:
                Optional Argument.
                Writes records from a Pandas DataFrame to Teradata Vantage
                using Fastload. FastLoad API can be used to quickly load large amounts of data
                in an empty table on Vantage.
                Types: boolean

        RETURNS:
            None

        RAISES:
            TeradataMlException
        """

        self._check_connection_error()
        con = get_context()

        self.if_exists = if_exists.lower()
        if self.if_exists not in ["fail", "replace", "append"]:
            raise ValueError(
                f'`if_exists` is not set to one of the following: ["fail", "replace", "append"].'
            )

        # convert pandas dataframe to polars
        if isinstance(df, pd.DataFrame):
            df = pl.from_pandas(df)

        self.table_name = f"{schema_name}.{table_name}" if schema_name else f"{table_name}"
        temp_table_name = None
        table_exists = self.exist(self.table_name)

        self._check_table_exists(table_exists)

        if not table_exists or self.if_exists == "replace":
            self._create_table(df, table_name, con, primary_index, schema_name, set_table, types)

        if fastload and self.if_exists == "append":
            temp_name = ''.join(sample(ascii_letters + "_", 30))
            self._create_table(df, temp_name, con, primary_index, schema_name, set_table, types)
            temp_table_name = f"{schema_name}.{temp_name}" if schema_name else f"{temp_name}"

        td_table_name = temp_table_name or self.table_name

        with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=True) as temp_file:
            # Write the DataFrame to the temporary file in CSV format
            df.write_csv(temp_file.name, datetime_format="%Y-%m-%d %H:%M:%S")
            logger.debug(f"Creating temporary CSV file: {temp_file.name}")

            insert_query = self._build_prepared_statement(df, td_table_name, temp_file.name, fastload)

            logger.debug(f"Uploading CSV to table: {td_table_name}")
            self.execute(insert_query)

        if fastload and self.if_exists == "append":
            logger.debug(f"Transferring data from {td_table_name} to {self.table_name}")
            self.execute(f"insert into {self.table_name} select * from {td_table_name}")
            logger.debug(f"Dropping staging table: {temp_table_name}")
            self.drop_table(td_table_name)

        return None

    def exist(self, table):
        """
        DESCRIPTION:
            A function to identify if table exits on teradata

        PARAMETERS:
            table:
            A table to be checked if exists in Teradata
            Types: str

        RETURNS:
            Boolean
        """
        self._check_connection_error()
        tbl = table.split(".", 1)
        if len(tbl) == 1:
            try:
                database = get_context().execute("select database").fetchone()
                result = (
                    get_context()
                    .execute(
                        f"select 1 from dbc.TablesV where TABLENAME = '{tbl[0]}' and DataBaseName = '{database[0]}'"
                    )
                    .fetchone()
                )
            except sqlachemyOperationalError as err:
                logger.error(self._log_metadata())
                raise err
        else:
            try:
                result = (
                    get_context()
                    .execute(
                        f"select 1 from dbc.TablesV where TABLENAME = '{tbl[1]}' and DataBaseName = '{tbl[0]}'"
                    )
                    .fetchone()
                )
            except sqlachemyOperationalError as err:
                logger.error(self._log_metadata())
                raise err
        return (result is not None) and (result[0] == 1)

    def drop_table(self, table):
        """
        DESCRIPTION:
            A function to drop table from teradata

        PARAMETERS:
            table:
                Required Argument.
                Drop table from teradata.
                Types: str

        RETURNS:
            None.
        """
        try:
            self._check_connection_error()
            get_context().execute(f"drop table {table}")
        except sqlachemyOperationalError as err:
            logger.error(self._log_metadata())
            raise err
        return None

    def delete_from(self, table):
        """
        DESCRIPTION:
            A function to delete contents from teradata

        PARAMETERS:
            table:
                Required Argument.
                A table in teradata to get its content deleted
                Types: str

        RETURNS:
            None.
        """
        try:
            self._check_connection_error()
            get_context().execute(f"delete from {table}")
        except sqlachemyOperationalError as err:
            logger.error(self._log_metadata())
            raise err
        return None

    def disconnect(self):
        """
        DESCRIPTION:
            Removes the current context associated with the Teradata Vantage connection.

        PARAMETERS:
            None.

        RETURNS:
            None.

        RAISES:
            None.
        """
        if self.is_valid:
            remove_context()

    @property
    def is_valid(self):
        """
        DESCRIPTION:
            Checks if there is a current connection to Teradata.
        """
        return get_context() is not None

    def _check_connection_error(self):
        if not self.is_valid:
            msg = "Connection to Teradata is not valid. Please connect to Teradata and try again."
            error_msg = "{}({}) {}".format(
                "[Teradata][teradataml]",
                ErrorInfoCodes.INVALID_CONTEXT_CONNECTION.value,
                msg,
            )
            raise TeradataMlException(error_msg, MessageCodes.INVALID_CONTEXT_CONNECTION)

    def _get_sqlalchemy_mapping(self, key):
        default_varchar_size = 1024
        polar_teradata_types_map = {
            pl.Utf8: VARCHAR(default_varchar_size, charset="UNICODE"),
            pl.Int8: BYTEINT,
            pl.Int16: SMALLINT,
            pl.Int32: INTEGER,
            pl.Int64: BIGINT,
            pl.Float32: FLOAT,
            pl.Float64: FLOAT,
            pl.Boolean: VARCHAR(5, charset="UNICODE"),
            pl.Date: DATE,
            pl.Datetime: TIMESTAMP,
            pl.Duration: VARCHAR(default_varchar_size, charset="UNICODE"),
            pl.Time: VARCHAR(default_varchar_size, charset="UNICODE"),
            pl.Object: VARCHAR(default_varchar_size, charset="UNICODE"),
        }
        default_value = VARCHAR(default_varchar_size, charset="UNICODE")
        return polar_teradata_types_map.get(key, default_value)

    def _extract_column_info(self, df, types=None):
        types = types or {}
        col_names = df.columns
        schema = dict(df.schema)

        col_types = [
            types.get(col_name, self._get_sqlalchemy_mapping(value.base_type()))
            for col_name, value in schema.items()
        ]

        return col_names, col_types

    def _create_table_object(self, df, table_name, con, primary_index, schema_name, set_table, types):
        # Dictionary to append special flags, can be extended to add Fallback, Journalling, Log etc.
        post_params = {}
        prefix = []
        pti = post(opts=post_params)
        if not set_table:
            prefix.append("multiset")
        else:
            prefix.append("set")

        meta = MetaData()
        meta.bind = con

        col_names, col_types = self._extract_column_info(df, types)

        if primary_index is not None:
            if isinstance(primary_index, list):
                pti = pti.primary_index(unique=False, cols=primary_index)
            elif isinstance(primary_index, str):
                pti = pti.primary_index(unique=False, cols=[primary_index])
        else:
            pti = pti.no_primary_index()

        # Create default Table construct with parameter dictionary
        table = Table(
            table_name,
            meta,
            *(Column(col_name, col_type) for col_name, col_type in zip(col_names, col_types)),
            teradatasql_post_create=pti,
            prefixes=prefix,
            schema=schema_name,
        )
        return table

    def _build_prepared_statement(self, df, table_name, file_name, fastload):
        query = "{fastload}{readcsv} insert into {tablename} ({values})"
        fastload = "{fn teradata_require_fastload}" if fastload else ""
        values = ", ".join(["?" for i in range(df.shape[1])])
        read_csv = f"{{fn teradata_read_csv({file_name})}}"
        built_query = query.format(
            fastload=fastload,
            readcsv=read_csv,
            tablename=table_name,
            values=values,
        )
        return built_query

    def _check_table_exists(self, is_table_exists):
        # Raise an exception when the table exists and if_exists = 'fail'
        if is_table_exists and self.if_exists == "fail":
            raise TeradataMlException(
                Messages.get_message(MessageCodes.TABLE_ALREADY_EXISTS, self.table_name),
                MessageCodes.TABLE_ALREADY_EXISTS,
            )

    def _create_table(self, df, table_name, con, primary_index, schema_name, set_table, types):
        check_table_name = f"{schema_name}.{table_name}" if schema_name else f"{table_name}"
        table_exists = self.exist(check_table_name)
        if table_exists:
            logger.debug(f'Dropping table: "{schema_name}"."{table_name}"')
            self.drop_table(check_table_name)
        table = self._create_table_object(df, table_name, con, primary_index, schema_name, set_table, types)
        if table is not None:
            # If the table need to be replaced, let's drop the existing table first
            try:
                logger.debug(f'Creating table: "{schema_name}"."{table_name}"')
                table.create()
            except sqlachemyOperationalError as err:
                logger.error(self._log_metadata())
                raise TeradataMlException(
                    Messages.get_message(MessageCodes.TABLE_OBJECT_CREATION_FAILED) + "\n" + str(err),
                    MessageCodes.TABLE_OBJECT_CREATION_FAILED,
                )
        else:
            logger.error(self._log_metadata())
            raise TeradataMlException(
                Messages.get_message(MessageCodes.TABLE_OBJECT_CREATION_FAILED),
                MessageCodes.TABLE_OBJECT_CREATION_FAILED,
            )

    def _log_metadata(self):
        metadata = json.dumps(self._metadata, indent=4)
        return f"Connection class metadata:\n{metadata}"
