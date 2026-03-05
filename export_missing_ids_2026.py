#!/usr/bin/env python3
import argparse
import configparser
import csv
import os
import sqlite3
import sys
import tempfile
from urllib.parse import parse_qs, unquote, urlparse


def chunked(iterable, size):
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export Matomo missing-id view + usage-db prefixes to CSV for offline OAI backfill."
    )
    parser.add_argument(
        "-c",
        "--config_file_path",
        default="config.ini",
        help="config file path (default: config.ini)",
    )
    parser.add_argument(
        "--view-name",
        default="missing_ids_2026",
        help="Matomo view name to export (default: missing_ids_2026)",
    )
    parser.add_argument(
        "--out-events",
        default="missing_ids_2026_events.csv",
        help="output CSV for events (idsite,idaction_url,action_url)",
    )
    parser.add_argument(
        "--no-dedupe-events",
        action="store_true",
        help="disable event deduplication by (idsite,idaction_url,action_url)",
    )
    parser.add_argument(
        "--action-batch-size",
        type=int,
        default=5000,
        help="batch size for action name lookups in two-pass mode (default: 5000)",
    )
    parser.add_argument(
        "--out-prefix-map",
        default="missing_ids_2026_prefix_map.csv",
        help="output CSV for prefix map (site_id,identifier_prefix)",
    )
    parser.add_argument(
        "--source-table",
        default="source",
        help="usage-db source table name (default: source)",
    )
    parser.add_argument(
        "--only-repository",
        action="store_true",
        help="export only repository rows from usage-db (type='R')",
    )
    return parser.parse_args()


def parse_sqlalchemy_mysql_uri(uri):
    import pymysql

    parsed = urlparse(uri)
    if parsed.scheme not in {"mysql", "mysql+pymysql"}:
        raise ValueError("Only mysql/mysql+pymysql URIs are supported")

    username = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    host = parsed.hostname
    port = parsed.port or 3306
    database = (parsed.path or "").lstrip("/")
    query = parse_qs(parsed.query)
    charset = query.get("charset", ["utf8mb4"])[0]

    if not host or not database:
        raise ValueError("Invalid SQLALCHEMY_DATABASE_URI (missing host or database)")

    return {
        "host": host,
        "port": port,
        "user": username,
        "passwd": password,
        "db": database,
        "charset": charset,
        "cursorclass": pymysql.cursors.DictCursor,
    }


def load_config(config_path):
    import pymysql

    config = configparser.ConfigParser()
    loaded = config.read(config_path)
    if not loaded:
        raise FileNotFoundError(f"Could not read config file: {config_path}")

    matomo_cfg = {
        "host": config.get("MATOMO_DB", "HOST"),
        "port": config.getint("MATOMO_DB", "PORT", fallback=3306),
        "user": config.get("MATOMO_DB", "USERNAME"),
        "passwd": config.get("MATOMO_DB", "PASSWORD"),
        "db": config.get("MATOMO_DB", "DATABASE"),
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
    }
    usage_uri = config.get("USAGE_STATS_DB", "SQLALCHEMY_DATABASE_URI")
    usage_cfg = parse_sqlalchemy_mysql_uri(usage_uri)
    return matomo_cfg, usage_cfg


def export_events(matomo_cfg, view_name, out_events, dedupe_events=True, action_batch_size=5000):
    import pymysql
    import pymysql.cursors

    pass1_query = f"SELECT idsite, idaction_url FROM `{view_name}`"
    total_rows = 0
    written_rows = 0

    if dedupe_events:
        tmp_fd, tmp_path = tempfile.mkstemp(prefix="missing_ids_dedupe_", suffix=".sqlite3")
        os.close(tmp_fd)
        dedupe_db = sqlite3.connect(tmp_path)
        try:
            dedupe_db.execute("PRAGMA journal_mode = OFF")
            dedupe_db.execute("PRAGMA synchronous = OFF")
            dedupe_db.execute("PRAGMA temp_store = MEMORY")
            dedupe_db.execute(
                """
                CREATE TABLE event_keys (
                    idsite TEXT NOT NULL,
                    idaction_url TEXT NOT NULL,
                    PRIMARY KEY (idsite, idaction_url)
                )
                """
            )
            dedupe_db.execute(
                """
                CREATE TABLE action_map (
                    idaction_url TEXT PRIMARY KEY,
                    action_url TEXT
                )
                """
            )

            stream_cfg = dict(matomo_cfg)
            stream_cfg["cursorclass"] = pymysql.cursors.SSCursor
            print("Pass 1/2: reading view and deduplicating keys (idsite,idaction_url)...")
            with pymysql.connect(**stream_cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute(pass1_query)
                    for idsite, idaction_url in cur:
                        total_rows += 1
                        dedupe_db.execute(
                            "INSERT OR IGNORE INTO event_keys (idsite, idaction_url) VALUES (?, ?)",
                            (
                                "" if idsite is None else str(idsite),
                                "" if idaction_url is None else str(idaction_url),
                            ),
                        )
                        if total_rows % 50000 == 0:
                            dedupe_db.commit()
                            print(f"  pass1 scanned rows: {total_rows}")
            dedupe_db.commit()

            unique_pairs = dedupe_db.execute("SELECT COUNT(*) FROM event_keys").fetchone()[0]
            print(f"Pass 1/2 done: {total_rows} rows scanned, {unique_pairs} unique (idsite,idaction_url)")

            print("Pass 2/2: resolving action names by unique idaction_url...")
            action_ids_iter = (
                row[0]
                for row in dedupe_db.execute(
                    "SELECT DISTINCT idaction_url FROM event_keys WHERE idaction_url <> ''"
                )
            )

            lookup_cfg = dict(matomo_cfg)
            lookup_cfg["cursorclass"] = pymysql.cursors.DictCursor
            looked_up_ids = 0
            with pymysql.connect(**lookup_cfg) as conn:
                with conn.cursor() as cur:
                    for action_chunk in chunked(action_ids_iter, action_batch_size):
                        placeholders = ",".join(["%s"] * len(action_chunk))
                        query = f"SELECT idaction, name FROM matomo_log_action WHERE idaction IN ({placeholders})"
                        cur.execute(query, action_chunk)
                        rows = cur.fetchall()
                        dedupe_db.executemany(
                            "INSERT OR REPLACE INTO action_map (idaction_url, action_url) VALUES (?, ?)",
                            [
                                (str(row.get("idaction")), "" if row.get("name") is None else str(row.get("name")))
                                for row in rows
                            ],
                        )
                        looked_up_ids += len(action_chunk)
                        if looked_up_ids % 50000 == 0:
                            dedupe_db.commit()
                            print(f"  pass2 looked up idaction_url: {looked_up_ids}")
            dedupe_db.commit()

            with open(out_events, "w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=["idsite", "idaction_url", "action_url"])
                writer.writeheader()
                for idsite, idaction_url, action_url in dedupe_db.execute(
                    """
                    SELECT e.idsite, e.idaction_url, COALESCE(a.action_url, '') AS action_url
                    FROM event_keys e
                    LEFT JOIN action_map a ON a.idaction_url = e.idaction_url
                    ORDER BY e.idsite, e.idaction_url
                    """
                ):
                    writer.writerow(
                        {
                            "idsite": idsite,
                            "idaction_url": idaction_url,
                            "action_url": action_url,
                        }
                    )
                    written_rows += 1
        finally:
            dedupe_db.close()
            try:
                os.remove(tmp_path)
            except OSError:
                pass
    else:
        query = (
            f"SELECT idsite, idaction_url, action_name AS action_url "
            f"FROM `{view_name}` "
            f"ORDER BY idsite, idaction_url"
        )
        stream_cfg = dict(matomo_cfg)
        stream_cfg["cursorclass"] = pymysql.cursors.SSCursor
        print("Single-pass mode: exporting events without deduplication...")
        with pymysql.connect(**stream_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                with open(out_events, "w", newline="", encoding="utf-8") as fh:
                    writer = csv.DictWriter(fh, fieldnames=["idsite", "idaction_url", "action_url"])
                    writer.writeheader()
                    for idsite, idaction_url, action_url in cur:
                        total_rows += 1
                        writer.writerow(
                            {
                                "idsite": idsite,
                                "idaction_url": idaction_url,
                                "action_url": action_url,
                            }
                        )
                        written_rows += 1

    return total_rows, written_rows


def export_prefix_map(usage_cfg, source_table, out_prefix_map, only_repository):
    import pymysql

    where = " WHERE type = 'R'" if only_repository else ""
    query = (
        f"SELECT site_id, identifier_prefix "
        f"FROM `{source_table}`"
        f"{where} "
        f"ORDER BY site_id"
    )
    with pymysql.connect(**usage_cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    with open(out_prefix_map, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["site_id", "identifier_prefix"])
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "site_id": row.get("site_id"),
                    "identifier_prefix": row.get("identifier_prefix"),
                }
            )
    return len(rows)


def main():
    args = parse_args()
    matomo_cfg, usage_cfg = load_config(args.config_file_path)

    dedupe_events = not args.no_dedupe_events
    event_rows, event_rows_out = export_events(
        matomo_cfg,
        args.view_name,
        args.out_events,
        dedupe_events,
        args.action_batch_size,
    )
    prefix_rows = export_prefix_map(usage_cfg, args.source_table, args.out_prefix_map, args.only_repository)

    print("Export completed")
    if dedupe_events:
        print(f"  events rows: {event_rows} (deduped to {event_rows_out}) -> {args.out_events}")
    else:
        print(f"  events rows: {event_rows} -> {args.out_events}")
    print(f"  prefix rows: {prefix_rows} -> {args.out_prefix_map}")
    print("Next step example:")
    print(
        "  python3 backfill_oai_identifier.py "
        f"--events {args.out_events} --prefix-map {args.out_prefix_map} --dry-run"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
