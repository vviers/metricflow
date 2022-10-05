import logging
import textwrap
import time

import pytest

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlClient
from metricflow.protocols.sql_request import SqlRequestTagSet
from metricflow.sql_clients.sql_utils import make_df
from metricflow.test.compare_df import assert_dataframes_equal
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState

logger = logging.getLogger(__name__)


def create_table_with_n_rows(sql_client: SqlClient, schema_name: str, num_rows: int) -> SqlTable:
    """Create a table with a specific number of rows."""
    sql_table = SqlTable(
        schema_name=schema_name,
        table_name=f"table_with_{num_rows}_rows",
    )
    sql_client.drop_table(sql_table)
    sql_client.create_table_from_dataframe(
        sql_table=sql_table,
        df=make_df(sql_client=sql_client, columns=["example_string"], data=(("foo",) for _ in range(num_rows))),
    )
    return sql_table


def test_async_query(sql_client: SqlClient, mf_test_session_state: MetricFlowTestSessionState) -> None:  # noqa: D
    request_id = sql_client.async_query("SELECT 1 AS foo")
    result = sql_client.async_request_result(request_id)
    assert_dataframes_equal(
        actual=result.df,
        expected=make_df(sql_client=sql_client, columns=["foo"], data=((1,),)),
    )
    assert result.exception is None


def test_async_execute(sql_client: SqlClient, mf_test_session_state: MetricFlowTestSessionState) -> None:  # noqa: D
    request_id = sql_client.async_execute("SELECT 1 AS foo")
    result = sql_client.async_request_result(request_id)
    assert result.exception is None


def test_cancel_request(sql_client: SqlClient, mf_test_session_state: MetricFlowTestSessionState) -> None:  # noqa: D
    if not sql_client.sql_engine_attributes.cancel_submitted_queries_supported:
        pytest.skip("Cancellation not yet supported in this SQL engine")
    # Execute a query that will be slow, giving the test the opportunity to cancel it.
    table_with_1000_rows = create_table_with_n_rows(sql_client, mf_test_session_state.mf_system_schema, num_rows=1000)
    table_with_100_rows = create_table_with_n_rows(sql_client, mf_test_session_state.mf_system_schema, num_rows=100)

    request_id = sql_client.async_execute(
        textwrap.dedent(
            f"""
            SELECT MAX({sql_client.sql_engine_attributes.random_function_name}()) AS max_value
            FROM {table_with_1000_rows.sql} a
            CROSS JOIN {table_with_1000_rows.sql} b
            CROSS JOIN {table_with_1000_rows.sql} c
            -- CROSS JOIN {table_with_100_rows.sql} d
            """
        )
    )
    # Need to wait a little bit as some clients like BQ doesn't show the query as running right away.
    time.sleep(2)
    num_cancelled = sql_client.cancel_request(SqlRequestTagSet.create_from_request_id(request_id))
    sql_client.async_request_result(request_id)
    assert num_cancelled == 1


def test_databricks(sql_client: SqlClient, mf_test_session_state: MetricFlowTestSessionState) -> None:
    # Execute a query that will be slow, giving the test the opportunity to cancel it.
    table_with_1000_rows = create_table_with_n_rows(sql_client, mf_test_session_state.mf_system_schema, num_rows=1000)
    table_with_100_rows = create_table_with_n_rows(sql_client, mf_test_session_state.mf_system_schema, num_rows=100)

    request_id = sql_client.async_execute(
        textwrap.dedent(
            f"""
                SELECT MAX(random_value) AS max_value
                FROM (
                    SELECT {sql_client.sql_engine_attributes.random_function_name}() AS random_value
                    FROM {table_with_1000_rows.sql} a
                    CROSS JOIN {table_with_1000_rows.sql} b
                    CROSS JOIN {table_with_1000_rows.sql} c
                    -- CROSS JOIN {table_with_100_rows.sql} d
                ) subq
                """
        )
    )
    time.sleep(2)
    sql_client.cancel_request(SqlRequestTagSet())
    time.sleep(2)
    sql_client.cancel_request(SqlRequestTagSet())