# Amplitude_API

Load event data from a Snowflake table and upload it to the
[Amplitude Batch Event Upload API](https://www.docs.developers.amplitude.com/analytics/apis/batch-event-upload-api/)
via HTTP POST.

---

## Overview

`amplitude_snowflake_loader.py` provides three focused classes and a
convenience `run()` function:

| Class | Responsibility |
|---|---|
| `SnowflakeEventLoader` | Connect to Snowflake and stream rows from any SQL query |
| `AmplitudeEventFormatter` | Map row columns to Amplitude event fields; non-Amplitude columns go into `event_properties` |
| `AmplitudeUploader` | Batch and POST events to `https://api2.amplitude.com/batch` |

---

## Requirements

```
Python >= 3.8
snowflake-connector-python >= 3.0.0
requests >= 2.28.0
```

Install with:

```bash
pip install -r requirements.txt
```

---

## Quick start

```python
from amplitude_snowflake_loader import run

run(
    snowflake_config={
        "account":   "xy12345.us-east-1",   # Snowflake account identifier
        "user":      "my_user",
        "password":  "my_password",
        "database":  "ANALYTICS",
        "schema":    "PUBLIC",
        "warehouse": "COMPUTE_WH",
        # "role":    "ANALYST",              # optional
    },
    query="SELECT * FROM events WHERE event_date = CURRENT_DATE",
    amplitude_api_key="MY_AMPLITUDE_API_KEY",
)
```

---

## Column mapping

The formatter maps Snowflake column names to Amplitude event fields
**case-insensitively**:

| Snowflake column (any case) | Amplitude field |
|---|---|
| `user_id` | `user_id` *(required unless `device_id` present)* |
| `device_id` | `device_id` *(required unless `user_id` present)* |
| `event_type` | `event_type` *(required)* |
| `time` | `time` — automatically converted from `datetime`/`date` to epoch ms |
| `app_version`, `platform`, `os_name`, `os_version` | top-level fields |
| `device_brand`, `device_manufacturer`, `device_model` | top-level fields |
| `carrier`, `country`, `region`, `city`, `dma`, `language` | top-level fields |
| `price`, `quantity`, `revenue`, `product_id`, `revenue_type` | top-level fields (camelCase applied automatically) |
| `location_lat`, `location_lng`, `ip` | top-level fields |
| `idfa`, `idfv`, `adid`, `android_id` | top-level fields |
| `event_id`, `session_id`, `insert_id` | top-level fields |
| `event_properties` | merged with auto-collected extra columns |
| `user_properties`, `groups` | top-level nested dicts |
| **any other column** | added to `event_properties` |

### Static extra properties

Pass `extra_event_properties` to `run()` to inject the same properties into
every event:

```python
run(
    ...,
    extra_event_properties={"data_source": "snowflake_pipeline"},
)
```

---

## Advanced usage

```python
from amplitude_snowflake_loader import (
    SnowflakeEventLoader,
    AmplitudeEventFormatter,
    AmplitudeUploader,
)

loader = SnowflakeEventLoader(
    account="xy12345.us-east-1",
    user="my_user",
    password="my_password",
    database="ANALYTICS",
    schema="PUBLIC",
    warehouse="COMPUTE_WH",
)

formatter = AmplitudeEventFormatter(
    extra_properties={"pipeline_version": "2.0"},
)

uploader = AmplitudeUploader(
    api_key="MY_AMPLITUDE_API_KEY",
    batch_size=500,   # override the 2000-event default
)

rows   = loader.fetch("SELECT * FROM events")
events = formatter.format_rows(rows)
total  = uploader.upload(events)

print(f"Uploaded {total} events to Amplitude.")
```

---

## Running the tests

```bash
pip install pytest
pytest tests/ -v
```