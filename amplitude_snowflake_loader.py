"""Load event data from a Snowflake table and upload it to the
Amplitude Batch Event Upload API.

Typical usage
-------------
>>> from amplitude_snowflake_loader import run
>>> run(
...     snowflake_config={
...         "account": "myaccount",
...         "user": "myuser",
...         "password": "mypassword",
...         "database": "MYDB",
...         "schema": "PUBLIC",
...         "warehouse": "COMPUTE_WH",
...     },
...     query="SELECT * FROM events",
...     amplitude_api_key="MY_AMPLITUDE_API_KEY",
... )
"""

from __future__ import annotations

import datetime
import logging
from typing import Any, Dict, Generator, Iterable, List, Optional

import requests
import snowflake.connector

logger = logging.getLogger(__name__)

# Amplitude Batch Event Upload endpoint
AMPLITUDE_BATCH_URL = "https://api2.amplitude.com/batch"

# Maximum events per batch as per the Amplitude API docs
MAX_BATCH_SIZE = 2000

# Amplitude event fields that are mapped directly from Snowflake columns when
# the column name (case-insensitive) matches.  All other columns are placed
# inside the nested ``event_properties`` dict.
AMPLITUDE_TOP_LEVEL_FIELDS = {
    "user_id",
    "device_id",
    "event_type",
    "time",
    "app_version",
    "platform",
    "os_name",
    "os_version",
    "device_brand",
    "device_manufacturer",
    "device_model",
    "carrier",
    "country",
    "region",
    "city",
    "dma",
    "language",
    "price",
    "quantity",
    "revenue",
    "product_id",
    "productid",
    "revenue_type",
    "revenuetype",
    "location_lat",
    "location_lng",
    "ip",
    "idfa",
    "idfv",
    "adid",
    "android_id",
    "event_id",
    "session_id",
    "insert_id",
    "event_properties",
    "user_properties",
    "groups",
}

# Canonical name mapping for common camel-case / alternate names
_FIELD_ALIASES: Dict[str, str] = {
    "productid": "productId",
    "revenuetype": "revenueType",
    "product_id": "productId",
    "revenue_type": "revenueType",
}


def _to_epoch_ms(value: Any) -> Optional[int]:
    """Convert a value to epoch milliseconds if it is a date/datetime object.

    Returns the value unchanged if it is already an integer or float (assumed
    to already represent epoch milliseconds), and returns ``None`` if the
    value cannot be converted.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime.datetime):
        return int(value.timestamp() * 1000)
    if isinstance(value, datetime.date):
        # Treat a bare date as midnight UTC
        dt = datetime.datetime(value.year, value.month, value.day,
                               tzinfo=datetime.timezone.utc)
        return int(dt.timestamp() * 1000)
    return None


class SnowflakeEventLoader:
    """Fetch event rows from a Snowflake table or query.

    Parameters
    ----------
    account:
        Snowflake account identifier (e.g. ``"xy12345.us-east-1"``).
    user:
        Snowflake username.
    password:
        Snowflake password.
    database:
        Default database.
    schema:
        Default schema.
    warehouse:
        Virtual warehouse to use.
    role:
        Optional role to use.
    """

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        database: str,
        schema: str,
        warehouse: str,
        role: Optional[str] = None,
    ) -> None:
        self._connect_kwargs: Dict[str, Any] = {
            "account": account,
            "user": user,
            "password": password,
            "database": database,
            "schema": schema,
            "warehouse": warehouse,
        }
        if role:
            self._connect_kwargs["role"] = role

    def fetch(
        self, query: str
    ) -> Generator[Dict[str, Any], None, None]:
        """Execute *query* and yield each row as a ``{column: value}`` dict.

        Parameters
        ----------
        query:
            SQL query to execute against Snowflake.
        """
        logger.info("Connecting to Snowflake …")
        with snowflake.connector.connect(**self._connect_kwargs) as conn:
            with conn.cursor(snowflake.connector.DictCursor) as cur:
                logger.info("Executing query: %s", query)
                cur.execute(query)
                for row in cur:
                    yield dict(row)


class AmplitudeEventFormatter:
    """Format a Snowflake row dict into an Amplitude event dict.

    The formatter uses the following rules:

    * Columns whose names (case-insensitive) are in
      :data:`AMPLITUDE_TOP_LEVEL_FIELDS` are mapped to the corresponding
      top-level Amplitude event keys.
    * The ``time`` column is converted to epoch milliseconds automatically.
    * All remaining columns are collected into ``event_properties``.
      If the row already contains an ``event_properties`` column it is
      *merged* with the auto-collected properties.

    Parameters
    ----------
    extra_properties:
        Additional key/value pairs that are merged into every event's
        ``event_properties`` dict.
    """

    def __init__(
        self,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._extra_properties: Dict[str, Any] = extra_properties or {}

    def format_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a single Snowflake row into an Amplitude event dict.

        Parameters
        ----------
        row:
            A single row returned by :class:`SnowflakeEventLoader`.

        Returns
        -------
        dict
            An Amplitude event object ready to be included in the ``events``
            array of the Batch Event Upload payload.

        Raises
        ------
        ValueError
            If the row contains neither a ``user_id`` nor a ``device_id``
            column, both of which are required by the Amplitude API.
        ValueError
            If the row does not contain an ``event_type`` column, which is
            required by the Amplitude API.
        """
        event: Dict[str, Any] = {}
        extra_props: Dict[str, Any] = dict(self._extra_properties)

        for raw_key, value in row.items():
            lower_key = raw_key.lower()

            if lower_key in AMPLITUDE_TOP_LEVEL_FIELDS:
                # Use canonical camelCase name when applicable
                canonical = _FIELD_ALIASES.get(lower_key, lower_key)

                if lower_key == "time":
                    converted = _to_epoch_ms(value)
                    if converted is not None:
                        event["time"] = converted
                elif lower_key == "event_properties":
                    # Will be merged below
                    if isinstance(value, dict):
                        extra_props.update(value)
                else:
                    if value is not None:
                        event[canonical] = value
            else:
                # Non-Amplitude column → goes into event_properties
                if value is not None:
                    extra_props[raw_key] = value

        if extra_props:
            event["event_properties"] = extra_props

        # Validate required fields
        if not event.get("user_id") and not event.get("device_id"):
            raise ValueError(
                "Row must contain at least one of 'user_id' or 'device_id'. "
                f"Row keys: {list(row.keys())}"
            )
        if not event.get("event_type"):
            raise ValueError(
                "Row must contain an 'event_type' column. "
                f"Row keys: {list(row.keys())}"
            )

        return event

    def format_rows(
        self, rows: Iterable[Dict[str, Any]]
    ) -> Generator[Dict[str, Any], None, None]:
        """Yield formatted Amplitude event dicts for each row in *rows*."""
        for row in rows:
            yield self.format_row(row)


class AmplitudeUploader:
    """Upload formatted Amplitude events via the Batch Event Upload API.

    Parameters
    ----------
    api_key:
        Your Amplitude project API key.
    batch_size:
        Number of events to include in each HTTP POST request.  Must not
        exceed :data:`MAX_BATCH_SIZE` (2000).
    url:
        Amplitude batch upload endpoint.  Override only for testing.
    session:
        Optional :class:`requests.Session` to use.  Useful for injecting a
        mock in tests.
    """

    def __init__(
        self,
        api_key: str,
        batch_size: int = MAX_BATCH_SIZE,
        url: str = AMPLITUDE_BATCH_URL,
        session: Optional[requests.Session] = None,
    ) -> None:
        if batch_size < 1 or batch_size > MAX_BATCH_SIZE:
            raise ValueError(
                f"batch_size must be between 1 and {MAX_BATCH_SIZE}, "
                f"got {batch_size}."
            )
        self._api_key = api_key
        self._batch_size = batch_size
        self._url = url
        self._session = session or requests.Session()

    def _post_batch(self, events: List[Dict[str, Any]]) -> None:
        """POST a single batch of events to Amplitude."""
        payload = {"api_key": self._api_key, "events": events}
        logger.debug("Uploading batch of %d events …", len(events))
        response = self._session.post(self._url, json=payload, timeout=30)
        response.raise_for_status()
        logger.info(
            "Batch of %d events uploaded (HTTP %s).",
            len(events),
            response.status_code,
        )

    def upload(self, events: Iterable[Dict[str, Any]]) -> int:
        """Upload all *events*, batching automatically.

        Parameters
        ----------
        events:
            Iterable of formatted Amplitude event dicts (as returned by
            :class:`AmplitudeEventFormatter`).

        Returns
        -------
        int
            Total number of events successfully uploaded.
        """
        batch: List[Dict[str, Any]] = []
        total = 0
        for event in events:
            batch.append(event)
            if len(batch) >= self._batch_size:
                self._post_batch(batch)
                total += len(batch)
                batch = []
        if batch:
            self._post_batch(batch)
            total += len(batch)
        logger.info("Upload complete. Total events uploaded: %d", total)
        return total


def run(
    snowflake_config: Dict[str, Any],
    query: str,
    amplitude_api_key: str,
    batch_size: int = MAX_BATCH_SIZE,
    extra_event_properties: Optional[Dict[str, Any]] = None,
) -> int:
    """Load events from Snowflake and upload them to Amplitude.

    This is a convenience wrapper that ties together
    :class:`SnowflakeEventLoader`, :class:`AmplitudeEventFormatter`, and
    :class:`AmplitudeUploader`.

    Parameters
    ----------
    snowflake_config:
        Keyword arguments forwarded to :class:`SnowflakeEventLoader`.
        Required keys: ``account``, ``user``, ``password``, ``database``,
        ``schema``, ``warehouse``.  Optional key: ``role``.
    query:
        SQL query to execute against Snowflake (e.g.
        ``"SELECT * FROM events_table"``).
    amplitude_api_key:
        Your Amplitude project API key.
    batch_size:
        Events per HTTP batch (default 2000, maximum 2000).
    extra_event_properties:
        Optional dict of properties to add to every event's
        ``event_properties``.

    Returns
    -------
    int
        Total number of events uploaded.
    """
    loader = SnowflakeEventLoader(**snowflake_config)
    formatter = AmplitudeEventFormatter(extra_properties=extra_event_properties)
    uploader = AmplitudeUploader(
        api_key=amplitude_api_key, batch_size=batch_size
    )

    rows = loader.fetch(query)
    events = formatter.format_rows(rows)
    return uploader.upload(events)
