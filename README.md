# Home Assistant SQLite to InfluxDB Script

This script transfers historical data from a Home Assistant SQLite database to InfluxDB.
It retrieves the earliest records from the InfluxDB bucket and extracts the states, attributes, and friendly names from the Home Assistant database for records prior to that.
Created using ChatGPT and tested with Home Assistant Core 2024.10.1 and InfluxDB v2.7.10.
Follow the steps below to set up the environment and run the script.

## Prerequisites

- Python 3.6 or higher
- A SQLite database file you wish to import data from
- An InfluxDB instance running and accessible

## Installation

### Step 1: Clone the Repository

Clone the repository or download the script files to your local machine.

```bash
git clone https://github.com/eldigo/ha-sqllite-2-influxdb
cd ha-sqllite-2-influxdb
```

### Step 2: Create a Virtual Environment

Create a Python virtual environment to isolate the project dependencies.

```bash
python3 -m venv myenv
```

### Step 3: Activate the Virtual Environment

Activate the virtual environment:

```bash
source myenv/bin/activate
```

### Step 4: Install Requirements

Install the required packages using the `requirements.txt` file provided.

```bash
pip install -r requirements.txt
```

### Step 5: Configure Environment Variables

Copy the `.env.example` file to a new file named `.env` and fill in the required values. You can use the following command:

```bash
cp .env.example .env
```

Open the `.env` file in a text editor and provide the necessary configurations for your InfluxDB connection.

```plaintext
SQLITE_DB=/path/to/home-assistant_v2.db
INFLUXDB_URL=http://localhost:8086
INFLUXDB_TOKEN=your_token
INFLUXDB_ORG=your_organization
INFLUXDB_BUCKET=your_bucket

BATCH_SIZE=5000
DEBUG_MODE=false
SOURCE_TAG=HA
INCLUDE_ATTRIBUTES=true
EXCLUDE_ENTITY_ID_REGEX=
INFLUX_SOURCE_FILTER=
```

| Variable | Purpose |
| --- | --- |
| `SQLITE_DB` | Path to the Home Assistant SQLite DB (opened read-only). |
| `INFLUXDB_URL` / `INFLUXDB_TOKEN` / `INFLUXDB_ORG` / `INFLUXDB_BUCKET` | InfluxDB v2 connection. |
| `BATCH_SIZE` | Rows fetched from SQLite and written to Influx per batch. |
| `DEBUG_MODE` | When true, writes points one-by-one and logs the offending point on failure. |
| `DRY_RUN` | When true, prints the row count and date range that would be exported and exits without writing. |
| `EXPORT_WINDOW_DAYS` | Size of the time window the SQLite read is chunked into. Default 30; smaller windows give more frequent progress logs. |

### What to expect during a run

The export streams SQLite in `EXPORT_WINDOW_DAYS`-day windows so the `last_updated_ts` index can be used for both the range filter and the ordering — without windowing, a single `ORDER BY` over hundreds of millions of rows can stall for hours before the first row is written. Per-window progress lines (`Chunk K/N: ...`) appear as work proceeds, plus a `still working — Xs elapsed` heartbeat every 30 s if anything goes quiet.

InfluxDB's data directory size fluctuates on its own: background TSM compaction rewrites and snappy-compresses shards, so size dips are normal even mid-run and don't indicate data loss. Confirm writes via `influx query` or the UI rather than `du`.
| `SOURCE_TAG` | Value written to the `source` tag on every point. |
| `INCLUDE_ATTRIBUTES` | When true, HA attributes are written as additional fields. |
| `EXCLUDE_ENTITY_ID_REGEX` | Optional regex; matching `entity_id`s are skipped. |
| `INFLUX_SOURCE_FILTER` | Optional `source` tag value used when looking up the oldest point already in the bucket. Must not contain quotes or backslashes. |

## Usage

Run the script using the following command:

```bash
python3 sqllite2influxdb.py
```

Make sure that your SQLite database file is correctly specified in the `.env` file, and that your InfluxDB instance is running and accessible.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
