import json
import logging
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone
from math import isfinite

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
LOG_LEVEL = logging.DEBUG if DEBUG_MODE else logging.INFO
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")

SQLITE_DB = os.getenv("SQLITE_DB")
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
SOURCE_TAG = os.getenv("SOURCE_TAG", "HA")
INCLUDE_ATTRIBUTES = os.getenv("INCLUDE_ATTRIBUTES", "true").lower() == "true"
EXCLUDE_ENTITY_ID_REGEX = os.getenv("EXCLUDE_ENTITY_ID_REGEX", "").strip()
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
EXPORT_WINDOW_DAYS = int(os.getenv("EXPORT_WINDOW_DAYS", "30"))

INFLUX_SOURCE_FILTER = os.getenv("INFLUX_SOURCE_FILTER", "").strip()
if INFLUX_SOURCE_FILTER and re.search(r'["\\]', INFLUX_SOURCE_FILTER):
    logging.error("INFLUX_SOURCE_FILTER must not contain quotes or backslashes.")
    raise SystemExit(1)

required_env_vars = {
    "SQLITE_DB": SQLITE_DB,
    "INFLUXDB_URL": INFLUXDB_URL,
    "INFLUXDB_TOKEN": INFLUXDB_TOKEN,
    "INFLUXDB_ORG": INFLUXDB_ORG,
    "INFLUXDB_BUCKET": INFLUXDB_BUCKET,
}

missing = [k for k, v in required_env_vars.items() if not v]
if missing:
    logging.error("Missing required environment variables: %s", ", ".join(missing))
    raise SystemExit(1)

exclude_pattern = re.compile(EXCLUDE_ENTITY_ID_REGEX) if EXCLUDE_ENTITY_ID_REGEX else None


def connect_to_sqlite(db_path):
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cursor = conn.cursor()
        cursor.execute("PRAGMA query_only = ON;")
        logging.info("Connected to SQLite read-only: %s", db_path)
        return conn, cursor
    except sqlite3.Error as e:
        logging.error("SQLite connection error: %s", e)
        raise SystemExit(1)


def connect_to_influxdb(url, token, org):
    try:
        client = InfluxDBClient(url=url, token=token, org=org)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        query_api = client.query_api()
        logging.info("Connected to InfluxDB: %s", url)
        return client, write_api, query_api
    except Exception as e:
        logging.error("InfluxDB connection error: %s", e)
        raise SystemExit(1)


def get_oldest_influx_epoch(query_api):
    """
    Return the earliest _time in the bucket as Unix epoch seconds.
    Uses first() so Influx can push the aggregation down instead of
    materializing and sorting every point in the bucket.
    """
    try:
        source_filter = ""
        if INFLUX_SOURCE_FILTER:
            source_filter = f'|> filter(fn: (r) => r["source"] == "{INFLUX_SOURCE_FILTER}")'

        query = f'''
from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: 0)
  {source_filter}
  |> first()
  |> keep(columns: ["_time"])
  |> group()
  |> sort(columns: ["_time"], desc: false)
  |> limit(n: 1)
'''
        result = query_api.query(org=INFLUXDB_ORG, query=query)

        for table in result:
            for record in table.records:
                ts = record.get_time()
                if ts is not None:
                    epoch = ts.timestamp()
                    logging.info("Oldest InfluxDB timestamp found: %s (epoch=%s)", ts.isoformat(), epoch)
                    return epoch

        logging.info("No existing points found in InfluxDB bucket/filter.")
        return None

    except Exception as e:
        logging.error("Error querying oldest InfluxDB timestamp: %s", e)
        return None


def build_sqlite_query(has_lower=False, has_upper=False):
    base_query = """
SELECT
    s.state,
    sm.entity_id,
    s.last_updated_ts,
    sa.shared_attrs
FROM states s
LEFT JOIN state_attributes sa
    ON sa.attributes_id = s.attributes_id
JOIN states_meta sm
    ON sm.metadata_id = s.metadata_id
WHERE s.last_updated_ts IS NOT NULL
"""

    if has_lower:
        base_query += " AND s.last_updated_ts >= ?"
    if has_upper:
        base_query += " AND s.last_updated_ts < ?"

    base_query += " ORDER BY s.last_updated_ts ASC"
    return base_query


def get_sqlite_range(cursor, upper_epoch):
    sql = "SELECT MIN(last_updated_ts), MAX(last_updated_ts) FROM states WHERE last_updated_ts IS NOT NULL"
    if upper_epoch is not None:
        sql += " AND last_updated_ts < ?"
        cursor.execute(sql, (upper_epoch,))
    else:
        cursor.execute(sql)
    return cursor.fetchone()


def compute_windows(min_ts, end_ts, window_days):
    step = window_days * 86400
    windows = []
    lo = float(min_ts)
    end = float(end_ts)
    while lo < end:
        hi = min(lo + step, end)
        windows.append((lo, hi))
        lo = hi
    return windows


def start_heartbeat(get_state):
    stop = threading.Event()

    def beat():
        start = time.monotonic()
        while not stop.wait(30):
            elapsed = int(time.monotonic() - start)
            logging.info("still working — %ds elapsed, %s", elapsed, get_state())

    threading.Thread(target=beat, daemon=True).start()
    return stop


def summarize_sqlite_range(cursor, oldest_epoch):
    sql = """
SELECT COUNT(*), MIN(last_updated_ts), MAX(last_updated_ts)
FROM states
WHERE last_updated_ts IS NOT NULL
"""
    if oldest_epoch is not None:
        sql += " AND last_updated_ts < ?"
        cursor.execute(sql, (oldest_epoch,))
    else:
        cursor.execute(sql)

    count, min_ts, max_ts = cursor.fetchone()

    logging.info("DRY RUN — no data will be written.")
    if oldest_epoch is not None:
        cutoff_iso = datetime.fromtimestamp(oldest_epoch, tz=timezone.utc).isoformat()
        logging.info("Cutoff:    %s (epoch=%s)", cutoff_iso, oldest_epoch)
    else:
        logging.info("Cutoff:    none — full history would be exported")

    logging.info("Rows:      %s", f"{count:,}")

    if count and min_ts is not None and max_ts is not None:
        min_iso = datetime.fromtimestamp(float(min_ts), tz=timezone.utc).isoformat()
        max_iso = datetime.fromtimestamp(float(max_ts), tz=timezone.utc).isoformat()
        logging.info("Range:     %s -> %s", min_iso, max_iso)
    else:
        logging.info("Range:     n/a (no rows match)")

    logging.info(
        "Note: count/range are pre-filter; EXCLUDE_ENTITY_ID_REGEX and state filters "
        "are applied per-row at write time and will reduce the actual point count."
    )


def parse_attributes(shared_attrs):
    if not shared_attrs:
        return {}
    try:
        return json.loads(shared_attrs)
    except (TypeError, json.JSONDecodeError) as e:
        logging.warning("Failed to parse shared_attrs JSON: %s", e)
        return {}


def should_exclude_entity(entity_id):
    return bool(exclude_pattern and exclude_pattern.search(entity_id))


def as_float(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        num = float(str(value).strip())
        if isfinite(num):
            return num
        return None
    except Exception:
        return None


def normalize_field_key(key):
    return str(key).strip().replace(" ", "_")


def make_point(row):
    state, entity_id, last_updated_ts, shared_attrs = row

    if not entity_id:
        return None

    if should_exclude_entity(entity_id):
        return None

    if state in ["unknown", "unavailable", "None", "", None]:
        return None

    if "." in entity_id:
        domain, entity_id_short = entity_id.split(".", 1)
    else:
        domain, entity_id_short = "unknown", entity_id

    attrs = parse_attributes(shared_attrs)
    friendly_name = attrs.get("friendly_name", entity_id_short)
    unit_of_measurement = attrs.get("unit_of_measurement", "count") or "count"

    try:
        ts = datetime.fromtimestamp(float(last_updated_ts), tz=timezone.utc)
    except Exception as e:
        logging.warning("Invalid timestamp for %s: %s (%s)", entity_id, last_updated_ts, e)
        return None

    point = (
        Point(unit_of_measurement)
        .tag("source", SOURCE_TAG)
        .tag("domain", domain)
        .tag("entity_id", entity_id_short)
        .tag("friendly_name", str(friendly_name))
        .time(ts)
    )

    numeric_state = as_float(state)
    if numeric_state is not None:
        point.field("value", numeric_state)
    else:
        point.field("state", str(state))

    if INCLUDE_ATTRIBUTES:
        for key, value in attrs.items():
            if key in {"friendly_name", "unit_of_measurement", "id", "id_str", "update_available"}:
                continue

            field_key = normalize_field_key(key)
            if not field_key:
                continue

            try:
                if isinstance(value, bool):
                    point.field(field_key, value)
                else:
                    numeric_value = as_float(value)
                    if numeric_value is not None:
                        point.field(field_key, numeric_value)
                    elif isinstance(value, (str, int, float)):
                        point.field(field_key, str(value))
                    else:
                        continue
            except Exception as e:
                logging.warning(
                    "Skipping attribute '%s' for '%s' due to type/write issue: %s",
                    key,
                    entity_id,
                    e,
                )

    return point


def batch_insert_to_influx(write_api, rows):
    points = []

    for row in rows:
        point = make_point(row)
        if point is not None:
            points.append(point)

    if not points:
        logging.info("No points to write in this batch.")
        return

    if DEBUG_MODE:
        written = 0
        for point in points:
            try:
                write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
                written += 1
            except Exception as e:
                logging.error("Error writing point to InfluxDB: %s. Point: %s", e, point.to_line_protocol())
        logging.info("Wrote %s/%s points to InfluxDB (debug mode)", written, len(points))
        return

    try:
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
        logging.info("Wrote %s points to InfluxDB", len(points))
    except Exception as e:
        logging.error("Error writing batch to InfluxDB: %s", e)
        raise


def main():
    conn, cursor = connect_to_sqlite(SQLITE_DB)
    client, write_api, query_api = connect_to_influxdb(INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG)

    oldest_epoch = get_oldest_influx_epoch(query_api)

    rows_fetched = 0
    exit_code = 0

    if DRY_RUN:
        try:
            summarize_sqlite_range(cursor, oldest_epoch)
        except sqlite3.Error as e:
            logging.error("SQLite query error during dry run: %s", e)
            exit_code = 1
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            try:
                write_api.close()
            except Exception:
                pass
            try:
                client.close()
            except Exception:
                pass
            logging.info("Closed SQLite and InfluxDB connections.")
        if exit_code:
            raise SystemExit(exit_code)
        return

    sql = build_sqlite_query(has_lower=True, has_upper=True)
    logging.debug("SQLite query: %s", " ".join(sql.split()))

    state = {"chunk": "init", "rows": 0}
    hb_stop = start_heartbeat(lambda: f"chunk={state['chunk']}, rows={state['rows']}")

    try:
        logging.info("Computing SQLite time range...")
        min_ts, max_ts = get_sqlite_range(cursor, oldest_epoch)

        if min_ts is None:
            logging.info("No rows to export.")
        else:
            end_ts = float(oldest_epoch) if oldest_epoch is not None else float(max_ts) + 1.0
            windows = compute_windows(min_ts, end_ts, EXPORT_WINDOW_DAYS)
            logging.info(
                "Export range: %s -> %s in %s window(s) of %sd",
                datetime.fromtimestamp(float(min_ts), tz=timezone.utc).isoformat(),
                datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat(),
                len(windows),
                EXPORT_WINDOW_DAYS,
            )

            cursor.execute("EXPLAIN QUERY PLAN " + sql, (windows[0][0], windows[0][1]))
            for plan_row in cursor.fetchall():
                logging.info("plan: %s", plan_row)

            for idx, (lo, hi) in enumerate(windows, start=1):
                lo_iso = datetime.fromtimestamp(lo, tz=timezone.utc).isoformat()
                hi_iso = datetime.fromtimestamp(hi, tz=timezone.utc).isoformat()
                state["chunk"] = f"{idx}/{len(windows)}"
                logging.info("Chunk %s: %s -> %s", state["chunk"], lo_iso, hi_iso)

                cursor.execute(sql, (lo, hi))
                chunk_rows = 0
                while True:
                    rows = cursor.fetchmany(BATCH_SIZE)
                    if not rows:
                        break
                    batch_insert_to_influx(write_api, rows)
                    chunk_rows += len(rows)
                    rows_fetched += len(rows)
                    state["rows"] = rows_fetched
                    logging.info("  chunk %s: %s rows (total %s)", state["chunk"], chunk_rows, rows_fetched)

                logging.info("Chunk %s done: %s rows", state["chunk"], chunk_rows)

            logging.info("Data export complete. Total SQLite rows fetched: %s", rows_fetched)

    except sqlite3.Error as e:
        logging.error("SQLite query error: %s", e)
        exit_code = 1
    except Exception as e:
        logging.error("Aborting after batch write failure: %s", e)
        exit_code = 1
    finally:
        hb_stop.set()
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        try:
            write_api.close()
        except Exception:
            pass
        try:
            client.close()
        except Exception:
            pass

        logging.info("Closed SQLite and InfluxDB connections.")

    if exit_code:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
