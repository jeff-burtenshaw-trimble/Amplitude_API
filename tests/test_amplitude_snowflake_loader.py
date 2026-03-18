"""Unit tests for amplitude_snowflake_loader.py.

Snowflake connectivity and HTTP calls are fully mocked so these tests run
without any external dependencies.
"""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch, call

import pytest

from amplitude_snowflake_loader import (
    AMPLITUDE_BATCH_URL,
    MAX_BATCH_SIZE,
    AmplitudeEventFormatter,
    AmplitudeUploader,
    SnowflakeEventLoader,
    _to_epoch_ms,
    run,
)


# ---------------------------------------------------------------------------
# _to_epoch_ms helpers
# ---------------------------------------------------------------------------

class TestToEpochMs:
    def test_integer_passthrough(self):
        assert _to_epoch_ms(1_000_000) == 1_000_000

    def test_float_truncated(self):
        assert _to_epoch_ms(1_000_000.9) == 1_000_000

    def test_datetime(self):
        dt = datetime.datetime(2023, 1, 1, 0, 0, 0,
                               tzinfo=datetime.timezone.utc)
        assert _to_epoch_ms(dt) == int(dt.timestamp() * 1000)

    def test_date_midnight_utc(self):
        d = datetime.date(2023, 1, 1)
        expected = int(
            datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
            .timestamp()
            * 1000
        )
        assert _to_epoch_ms(d) == expected

    def test_none_returns_none(self):
        assert _to_epoch_ms(None) is None

    def test_unconvertible_returns_none(self):
        assert _to_epoch_ms("not-a-date") is None


# ---------------------------------------------------------------------------
# AmplitudeEventFormatter
# ---------------------------------------------------------------------------

class TestAmplitudeEventFormatter:
    def _make_basic_row(self, **overrides):
        row = {
            "user_id": "user_001",
            "event_type": "page_view",
            "time": 1_700_000_000_000,
        }
        row.update(overrides)
        return row

    def test_basic_row(self):
        fmt = AmplitudeEventFormatter()
        event = fmt.format_row(self._make_basic_row())
        assert event["user_id"] == "user_001"
        assert event["event_type"] == "page_view"
        assert event["time"] == 1_700_000_000_000

    def test_datetime_time_field_converted(self):
        dt = datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
        fmt = AmplitudeEventFormatter()
        event = fmt.format_row(self._make_basic_row(time=dt))
        assert event["time"] == int(dt.timestamp() * 1000)

    def test_unknown_columns_go_to_event_properties(self):
        fmt = AmplitudeEventFormatter()
        event = fmt.format_row(
            self._make_basic_row(page_url="/home", button_color="blue")
        )
        assert event["event_properties"]["page_url"] == "/home"
        assert event["event_properties"]["button_color"] == "blue"

    def test_extra_properties_merged(self):
        fmt = AmplitudeEventFormatter(extra_properties={"source": "pipeline"})
        event = fmt.format_row(self._make_basic_row())
        assert event["event_properties"]["source"] == "pipeline"

    def test_extra_properties_overridden_by_row(self):
        """Row-level event_properties take precedence over extra_properties."""
        fmt = AmplitudeEventFormatter(extra_properties={"source": "pipeline"})
        row = self._make_basic_row()
        row["event_properties"] = {"source": "row_value"}
        event = fmt.format_row(row)
        assert event["event_properties"]["source"] == "row_value"

    def test_none_values_excluded(self):
        fmt = AmplitudeEventFormatter()
        event = fmt.format_row(self._make_basic_row(os_name=None))
        assert "os_name" not in event

    def test_missing_user_id_and_device_id_raises(self):
        fmt = AmplitudeEventFormatter()
        with pytest.raises(ValueError, match="user_id.*device_id"):
            fmt.format_row({"event_type": "click", "time": 0})

    def test_device_id_satisfies_identity_requirement(self):
        fmt = AmplitudeEventFormatter()
        event = fmt.format_row(
            {"device_id": "DEV123", "event_type": "click", "time": 0}
        )
        assert event["device_id"] == "DEV123"

    def test_missing_event_type_raises(self):
        fmt = AmplitudeEventFormatter()
        with pytest.raises(ValueError, match="event_type"):
            fmt.format_row({"user_id": "u1", "time": 0})

    def test_product_id_canonical_name(self):
        fmt = AmplitudeEventFormatter()
        event = fmt.format_row(
            self._make_basic_row(product_id="prod_99")
        )
        assert event.get("productId") == "prod_99"

    def test_revenue_type_canonical_name(self):
        fmt = AmplitudeEventFormatter()
        event = fmt.format_row(
            self._make_basic_row(revenue_type="purchase")
        )
        assert event.get("revenueType") == "purchase"

    def test_case_insensitive_column_matching(self):
        fmt = AmplitudeEventFormatter()
        event = fmt.format_row(
            {
                "USER_ID": "u1",
                "EVENT_TYPE": "click",
                "OS_NAME": "iOS",
                "time": 0,
            }
        )
        assert event["user_id"] == "u1"
        assert event["event_type"] == "click"
        assert event["os_name"] == "iOS"

    def test_format_rows_yields_all(self):
        fmt = AmplitudeEventFormatter()
        rows = [self._make_basic_row(user_id=f"u{i}") for i in range(5)]
        events = list(fmt.format_rows(rows))
        assert len(events) == 5
        assert [e["user_id"] for e in events] == [f"u{i}" for i in range(5)]


# ---------------------------------------------------------------------------
# AmplitudeUploader
# ---------------------------------------------------------------------------

class TestAmplitudeUploader:
    def _make_event(self, user_id="u1"):
        return {"user_id": user_id, "event_type": "click"}

    def test_single_batch_posted(self):
        session = MagicMock()
        session.post.return_value.status_code = 200
        uploader = AmplitudeUploader(
            api_key="KEY", session=session, batch_size=10
        )
        total = uploader.upload([self._make_event()])
        assert total == 1
        session.post.assert_called_once()
        payload = session.post.call_args[1]["json"]
        assert payload["api_key"] == "KEY"
        assert len(payload["events"]) == 1

    def test_multiple_batches_when_exceeds_batch_size(self):
        session = MagicMock()
        session.post.return_value.status_code = 200
        uploader = AmplitudeUploader(
            api_key="KEY", session=session, batch_size=3
        )
        events = [self._make_event(f"u{i}") for i in range(7)]
        total = uploader.upload(events)
        assert total == 7
        # 7 events with batch_size=3 → ceil(7/3) = 3 POST calls
        assert session.post.call_count == 3

    def test_empty_events_no_post(self):
        session = MagicMock()
        uploader = AmplitudeUploader(api_key="KEY", session=session)
        total = uploader.upload([])
        assert total == 0
        session.post.assert_not_called()

    def test_http_error_propagates(self):
        session = MagicMock()
        session.post.return_value.raise_for_status.side_effect = Exception(
            "HTTP 400"
        )
        uploader = AmplitudeUploader(api_key="KEY", session=session)
        with pytest.raises(Exception, match="HTTP 400"):
            uploader.upload([self._make_event()])

    def test_invalid_batch_size_raises(self):
        with pytest.raises(ValueError):
            AmplitudeUploader(api_key="KEY", batch_size=0)
        with pytest.raises(ValueError):
            AmplitudeUploader(api_key="KEY", batch_size=MAX_BATCH_SIZE + 1)

    def test_correct_url_used(self):
        session = MagicMock()
        session.post.return_value.status_code = 200
        uploader = AmplitudeUploader(api_key="KEY", session=session)
        uploader.upload([self._make_event()])
        url_called = session.post.call_args[0][0]
        assert url_called == AMPLITUDE_BATCH_URL


# ---------------------------------------------------------------------------
# SnowflakeEventLoader
# ---------------------------------------------------------------------------

class TestSnowflakeEventLoader:
    def _make_loader(self):
        return SnowflakeEventLoader(
            account="acct",
            user="user",
            password="pass",
            database="DB",
            schema="SCH",
            warehouse="WH",
        )

    def test_fetch_yields_rows(self):
        rows = [
            {"user_id": "u1", "event_type": "click", "time": 0},
            {"user_id": "u2", "event_type": "view", "time": 1},
        ]

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.__iter__ = MagicMock(return_value=iter(rows))

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor

        with patch("snowflake.connector.connect", return_value=mock_conn):
            loader = self._make_loader()
            result = list(loader.fetch("SELECT * FROM events"))

        assert result == rows
        mock_cursor.execute.assert_called_once_with("SELECT * FROM events")

    def test_connect_kwargs_forwarded(self):
        loader = SnowflakeEventLoader(
            account="acct",
            user="user",
            password="pass",
            database="DB",
            schema="SCH",
            warehouse="WH",
            role="ANALYST",
        )
        assert loader._connect_kwargs["role"] == "ANALYST"
        assert loader._connect_kwargs["account"] == "acct"


# ---------------------------------------------------------------------------
# run() integration (all external calls mocked)
# ---------------------------------------------------------------------------

class TestRun:
    def test_run_end_to_end(self):
        rows = [
            {"user_id": "u1", "event_type": "purchase", "time": 1_700_000_000_000},
            {"user_id": "u2", "event_type": "signup", "time": 1_700_000_001_000},
        ]

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.__iter__ = MagicMock(return_value=iter(rows))

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor

        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("snowflake.connector.connect", return_value=mock_conn), \
             patch("requests.Session.post", return_value=mock_response):
            total = run(
                snowflake_config={
                    "account": "acct",
                    "user": "user",
                    "password": "pass",
                    "database": "DB",
                    "schema": "SCH",
                    "warehouse": "WH",
                },
                query="SELECT * FROM events",
                amplitude_api_key="MY_KEY",
            )

        assert total == 2
