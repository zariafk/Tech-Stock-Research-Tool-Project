"""Unit tests for the load script."""

from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from load import get_existing_ids, insert_dataframe, load_main


@pytest.fixture()
def mock_conn():
    """Creates a mock PostgreSQL connection with a cursor context manager."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cursor


class TestGetExistingIds:
    """Tests for get_existing_ids."""

    def test_returns_set_of_ids(self, mock_conn):
        conn, cursor = mock_conn
        cursor.fetchall.return_value = [("abc",), ("def",), ("ghi",)]

        result = get_existing_ids(conn, "fact_posts", "id")

        assert result == {"abc", "def", "ghi"}
        cursor.execute.assert_called_once_with("SELECT id FROM fact_posts")

    def test_returns_empty_set_when_no_rows(self, mock_conn):
        conn, cursor = mock_conn
        cursor.fetchall.return_value = []

        result = get_existing_ids(conn, "fact_posts", "id")

        assert result == set()

    def test_uses_correct_table_and_column(self, mock_conn):
        conn, cursor = mock_conn
        cursor.fetchall.return_value = [("t5_abc",)]

        get_existing_ids(conn, "dim_subreddits", "subreddit_id")

        cursor.execute.assert_called_once_with(
            "SELECT subreddit_id FROM dim_subreddits"
        )


class TestInsertDataframe:
    """Tests for insert_dataframe."""

    def test_inserts_rows(self, mock_conn):
        conn, cursor = mock_conn
        df = pd.DataFrame([
            {"id": "abc", "title": "Post 1"},
            {"id": "def", "title": "Post 2"},
        ])

        with patch("load.psycopg2.extras.execute_batch") as mock_batch:
            insert_dataframe(conn, df, "fact_posts")

            mock_batch.assert_called_once()
            query = mock_batch.call_args[0][1]
            rows = mock_batch.call_args[0][2]

            assert "INSERT INTO fact_posts" in query
            assert "id, title" in query
            assert len(rows) == 2

        conn.commit.assert_called_once()

    def test_skips_empty_dataframe(self, mock_conn):
        conn, cursor = mock_conn
        df = pd.DataFrame()

        with patch("load.psycopg2.extras.execute_batch") as mock_batch:
            insert_dataframe(conn, df, "fact_posts")
            mock_batch.assert_not_called()

        conn.commit.assert_not_called()


class TestLoadMain:
    """Tests for load_main."""

    def test_inserts_non_empty_tables(self, mock_conn):
        conn, _ = mock_conn
        tables = {
            "fact_posts": pd.DataFrame([{"id": "abc", "title": "Post"}]),
            "dim_subreddits": pd.DataFrame([{"subreddit_id": "t5_1"}]),
        }

        with patch("load.insert_dataframe") as mock_insert:
            load_main(tables, conn=conn)

            assert mock_insert.call_count == 2

    def test_skips_empty_tables(self, mock_conn):
        conn, _ = mock_conn
        tables = {
            "fact_posts": pd.DataFrame([{"id": "abc"}]),
            "dim_subreddits": pd.DataFrame(),
        }

        with patch("load.insert_dataframe") as mock_insert:
            load_main(tables, conn=conn)

            mock_insert.assert_called_once()
            assert mock_insert.call_args[0][2] == "fact_posts"

    def test_handles_all_empty_tables(self, mock_conn):
        conn, _ = mock_conn
        tables = {
            "fact_posts": pd.DataFrame(),
            "dim_subreddits": pd.DataFrame(),
        }

        with patch("load.insert_dataframe") as mock_insert:
            load_main(tables, conn=conn)
            mock_insert.assert_not_called()
