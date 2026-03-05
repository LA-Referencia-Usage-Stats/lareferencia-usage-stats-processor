#!/usr/bin/env python3
"""
Offline backfill helper for Matomo custom_var_v1 (OAI identifier).

Input 1: events CSV/TSV with at least:
  - idsite
  - idaction_url
  - action_url
  - optional custom_var_v1

Input 2: prefix map CSV/TSV with:
  - site_id
  - identifier_prefix

Output:
  - Full audit CSV with status per row
  - Deduplicated updates CSV by (idsite, idaction_url)
  - Unresolved rows CSV
  - Missing-prefix sites CSV (deduplicated by idsite)
  - SQL script to apply updates in Matomo
"""

import argparse
import csv
import os
import re
import sqlite3
import sys
import tempfile
from collections import defaultdict
from urllib.parse import unquote


HANDLE_PATTERNS = [
    re.compile(r"(?:https?://)?(?:hdl\.handle\.net|handle\.net)/([0-9]+/[A-Za-z0-9._-]+)", re.IGNORECASE),
    re.compile(r"/bitstream/(?:handle/)?([0-9]+/[A-Za-z0-9._-]+)", re.IGNORECASE),
    re.compile(r"/handle/([0-9]+/[A-Za-z0-9._-]+)", re.IGNORECASE),
]

OAI_IDENTIFIER_PATTERN = re.compile(r"^(oai:)(?:https?://)?([^/]+)(?:/[^:]+)*(:.*)$", re.IGNORECASE)


def normalize_site_id(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return str(int(float(text)))
    except ValueError:
        return text


def normalize_prefix(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    text = text.rstrip(":/")
    if text.lower().startswith("oai:"):
        text = "oai:" + text[4:]
    else:
        text = "oai:" + text
    return text


def normalize_oai_identifier(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    decoded = unquote(text)
    match = OAI_IDENTIFIER_PATTERN.match(decoded)
    if match:
        return match.group(1).lower() + match.group(2) + match.group(3)
    return decoded


def extract_handle(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None

    decoded = unquote(text)
    for pattern in HANDLE_PATTERNS:
        match = pattern.search(decoded)
        if match:
            return match.group(1)
    return None


def open_reader(path, delimiter):
    handle = open(path, "r", newline="", encoding="utf-8")
    return handle, csv.DictReader(handle, delimiter=delimiter)


def validate_columns(reader, path, required_columns):
    missing = [column for column in required_columns if column not in reader.fieldnames]
    if missing:
        raise ValueError(
            "Missing required columns in %s: %s. Available columns: %s"
            % (path, ", ".join(missing), ", ".join(reader.fieldnames or []))
        )


def load_prefix_map(path, delimiter, site_column, prefix_column):
    fh, reader = open_reader(path, delimiter)
    try:
        validate_columns(reader, path, [site_column, prefix_column])
        prefixes = {}
        duplicates = 0
        for row in reader:
            site_id = normalize_site_id(row.get(site_column))
            prefix = normalize_prefix(row.get(prefix_column))
            if site_id is None or prefix is None:
                continue
            if site_id in prefixes and prefixes[site_id] != prefix:
                duplicates += 1
            prefixes[site_id] = prefix
        return prefixes, duplicates
    finally:
        fh.close()


def sqlite_connect(temp_db_path):
    cleanup = False
    db_path = temp_db_path
    if not db_path:
        fd, db_path = tempfile.mkstemp(prefix="backfill_oai_", suffix=".sqlite3")
        os.close(fd)
        cleanup = True

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA temp_store = MEMORY")
    return conn, db_path, cleanup


def parse_args():
    parser = argparse.ArgumentParser(
        description="Offline reconstruction of OAI identifiers for Matomo rows with missing custom_var_v1."
    )
    parser.add_argument("--events", required=True, help="events file (CSV/TSV)")
    parser.add_argument("--prefix-map", required=True, help="prefix map file (CSV/TSV)")

    parser.add_argument("--events-sep", default=",", help="events delimiter (default: ,)")
    parser.add_argument("--prefix-sep", default=",", help="prefix map delimiter (default: ,)")

    parser.add_argument("--events-site-col", default="idsite", help="events site column")
    parser.add_argument("--events-idaction-col", default="idaction_url", help="events idaction_url column")
    parser.add_argument("--events-action-url-col", default="action_url", help="events action_url column")
    parser.add_argument(
        "--events-current-oai-col",
        default="custom_var_v1",
        help="events current OAI column (optional, default: custom_var_v1)",
    )

    parser.add_argument("--prefix-site-col", default="site_id", help="prefix map site column")
    parser.add_argument("--prefix-value-col", default="identifier_prefix", help="prefix map identifier prefix column")

    parser.add_argument("--out-all", default="backfill_oai_audit.csv", help="full audit output file")
    parser.add_argument(
        "--out-updates",
        default="backfill_oai_updates_by_action.csv",
        help="deduplicated updates output by (idsite,idaction_url)",
    )
    parser.add_argument("--out-unresolved", default="backfill_oai_unresolved.csv", help="unresolved rows output file")
    parser.add_argument(
        "--out-missing-prefix-sites",
        default="backfill_missing_prefix_sites.csv",
        help="missing prefix sites output file (deduplicated by idsite)",
    )
    parser.add_argument(
        "--out-sql-update",
        default="backfill_oai_apply_updates.sql",
        help="sql script output file for applying updates in Matomo",
    )

    parser.add_argument("--temp-db-path", default=None, help="optional sqlite temp db path")
    parser.add_argument("--progress-every", type=int, default=50000, help="progress interval rows (default: 50000)")

    parser.add_argument("--sql-target-table", default="matomo_log_link_visit_action", help="Matomo target table")
    parser.add_argument("--sql-target-column", default="custom_var_v1", help="Matomo target identifier column")
    parser.add_argument(
        "--sql-date-from",
        default=None,
        help="optional lower bound server_time (inclusive), e.g. 2026-01-01 00:00:00",
    )
    parser.add_argument(
        "--sql-date-to",
        default=None,
        help="optional upper bound server_time (exclusive), e.g. 2026-03-01 00:00:00",
    )
    parser.add_argument(
        "--sql-null-only",
        action="store_true",
        help="sql update filter only NULL target column (default: NULL or empty string)",
    )

    parser.add_argument("--dry-run", action="store_true", help="compute and print summary only")
    return parser.parse_args()


def sql_quote(value):
    if value is None:
        return ""
    return str(value).replace("\\", "\\\\").replace("'", "''")


def write_sql_script(args):
    updates_csv_abs = os.path.abspath(args.out_updates)
    table_name = args.sql_target_table
    target_column = args.sql_target_column
    tmp_table = "tmp_backfill_oai_updates"

    where_filters = []
    if args.sql_date_from:
        where_filters.append("mlva.server_time >= '%s'" % sql_quote(args.sql_date_from))
    if args.sql_date_to:
        where_filters.append("mlva.server_time < '%s'" % sql_quote(args.sql_date_to))
    if args.sql_null_only:
        where_filters.append("mlva.%s IS NULL" % target_column)
    else:
        where_filters.append("(mlva.%s IS NULL OR mlva.%s = '')" % (target_column, target_column))

    where_clause = " AND\n  ".join(where_filters)

    sql = f"""-- Generated by backfill_oai_identifier.py
-- Load deduplicated updates and apply to Matomo.

DROP TEMPORARY TABLE IF EXISTS {tmp_table};
CREATE TEMPORARY TABLE {tmp_table} (
  idsite BIGINT NOT NULL,
  idaction_url BIGINT NOT NULL,
  reconstructed_oai_identifier VARCHAR(1024) NOT NULL,
  rows_to_update BIGINT NULL,
  PRIMARY KEY (idsite, idaction_url, reconstructed_oai_identifier)
);

LOAD DATA LOCAL INFILE '{sql_quote(updates_csv_abs)}'
INTO TABLE {tmp_table}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\\n'
IGNORE 1 LINES
(idsite, idaction_url, reconstructed_oai_identifier, rows_to_update);

UPDATE {table_name} mlva
JOIN {tmp_table} u
  ON u.idsite = mlva.idsite
 AND u.idaction_url = mlva.idaction_url
SET mlva.{target_column} = u.reconstructed_oai_identifier
WHERE
  {where_clause};

SELECT ROW_COUNT() AS updated_rows;
"""

    with open(args.out_sql_update, "w", encoding="utf-8") as fh:
        fh.write(sql)


def main():
    args = parse_args()

    prefix_map, conflicting_prefixes = load_prefix_map(
        args.prefix_map,
        args.prefix_sep,
        args.prefix_site_col,
        args.prefix_value_col,
    )

    print("Loaded prefix map entries:", len(prefix_map))
    if conflicting_prefixes > 0:
        print("Warning: conflicting prefixes found for same site_id:", conflicting_prefixes)

    conn, temp_db_path, cleanup_temp_db = sqlite_connect(args.temp_db_path)
    out_all_fh = None
    out_unresolved_fh = None
    out_updates_fh = None
    out_missing_prefix_sites_fh = None

    try:
        print("Assuming deduplicated input by (idsite,idaction_url).")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS update_counts (
                idsite TEXT NOT NULL,
                idaction_url TEXT NOT NULL,
                reconstructed_oai_identifier TEXT NOT NULL,
                rows_to_update INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (idsite, idaction_url, reconstructed_oai_identifier)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS missing_prefix_row_counts (
                idsite TEXT PRIMARY KEY,
                rows_missing_prefix INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS missing_prefix_actions (
                idsite TEXT NOT NULL,
                idaction_url TEXT NOT NULL,
                PRIMARY KEY (idsite, idaction_url)
            )
            """
        )
        conn.commit()

        events_fh, events_reader = open_reader(args.events, args.events_sep)
        try:
            validate_columns(
                events_reader,
                args.events,
                [args.events_site_col, args.events_idaction_col, args.events_action_url_col],
            )

            if not args.dry_run:
                all_fieldnames = list(events_reader.fieldnames or []) + [
                    "prefix_norm",
                    "handle",
                    "reconstructed_oai_identifier",
                    "status",
                ]
                unresolved_fieldnames = list(events_reader.fieldnames or []) + [
                    "prefix_norm",
                    "handle",
                    "status",
                ]
                updates_fieldnames = ["idsite", "idaction_url", "reconstructed_oai_identifier", "rows_to_update"]
                missing_prefix_sites_fieldnames = ["idsite", "rows_missing_prefix", "distinct_idaction_url"]

                out_all_fh = open(args.out_all, "w", newline="", encoding="utf-8")
                out_unresolved_fh = open(args.out_unresolved, "w", newline="", encoding="utf-8")
                out_updates_fh = open(args.out_updates, "w", newline="", encoding="utf-8")
                out_missing_prefix_sites_fh = open(args.out_missing_prefix_sites, "w", newline="", encoding="utf-8")

                all_writer = csv.DictWriter(out_all_fh, fieldnames=all_fieldnames)
                unresolved_writer = csv.DictWriter(out_unresolved_fh, fieldnames=unresolved_fieldnames)
                updates_writer = csv.DictWriter(out_updates_fh, fieldnames=updates_fieldnames)
                missing_prefix_sites_writer = csv.DictWriter(
                    out_missing_prefix_sites_fh, fieldnames=missing_prefix_sites_fieldnames
                )

                all_writer.writeheader()
                unresolved_writer.writeheader()
                updates_writer.writeheader()
                missing_prefix_sites_writer.writeheader()
            else:
                all_writer = None
                unresolved_writer = None
                updates_writer = None
                missing_prefix_sites_writer = None

            print("Classifying rows and preparing outputs...")
            summary = defaultdict(int)
            processed_rows = 0

            for row in events_reader:
                processed_rows += 1
                site_id = normalize_site_id(row.get(args.events_site_col))
                idaction = str(row.get(args.events_idaction_col, "")).strip()
                action_url = row.get(args.events_action_url_col)
                current_identifier = row.get(args.events_current_oai_col)

                prefix = prefix_map.get(site_id)
                handle = extract_handle(action_url)
                reconstructed = "%s:%s" % (prefix, handle) if prefix and handle else None
                reconstructed_norm = normalize_oai_identifier(reconstructed)
                current_norm = normalize_oai_identifier(current_identifier)

                status = None
                canonical_reconstructed = reconstructed_norm

                if current_norm:
                    if reconstructed_norm and current_norm != reconstructed_norm:
                        status = "existing_mismatch"
                    else:
                        status = "already_present"
                elif prefix is None:
                    status = "missing_prefix"
                elif handle is None:
                    status = "unresolved_url"
                elif site_id is None or idaction == "":
                    status = "unresolved_url"
                else:
                    status = "reconstructed"
                    canonical_reconstructed = reconstructed_norm

                summary[status] += 1

                if status == "reconstructed":
                    conn.execute(
                        "INSERT OR IGNORE INTO update_counts (idsite, idaction_url, reconstructed_oai_identifier, rows_to_update) VALUES (?, ?, ?, 0)",
                        (site_id, idaction, canonical_reconstructed),
                    )
                    conn.execute(
                        "UPDATE update_counts SET rows_to_update = rows_to_update + 1 WHERE idsite = ? AND idaction_url = ? AND reconstructed_oai_identifier = ?",
                        (site_id, idaction, canonical_reconstructed),
                    )
                elif status == "missing_prefix":
                    site_key = site_id if site_id is not None else ""
                    conn.execute(
                        "INSERT OR IGNORE INTO missing_prefix_row_counts (idsite, rows_missing_prefix) VALUES (?, 0)",
                        (site_key,),
                    )
                    conn.execute(
                        "UPDATE missing_prefix_row_counts SET rows_missing_prefix = rows_missing_prefix + 1 WHERE idsite = ?",
                        (site_key,),
                    )
                    if idaction:
                        conn.execute(
                            "INSERT OR IGNORE INTO missing_prefix_actions (idsite, idaction_url) VALUES (?, ?)",
                            (site_key, idaction),
                        )

                if not args.dry_run:
                    all_row = dict(row)
                    all_row["prefix_norm"] = prefix or ""
                    all_row["handle"] = handle or ""
                    all_row["reconstructed_oai_identifier"] = canonical_reconstructed or ""
                    all_row["status"] = status
                    all_writer.writerow(all_row)

                    if status not in ("reconstructed", "already_present"):
                        unresolved_row = dict(row)
                        unresolved_row["prefix_norm"] = prefix or ""
                        unresolved_row["handle"] = handle or ""
                        unresolved_row["status"] = status
                        unresolved_writer.writerow(unresolved_row)

                if processed_rows % args.progress_every == 0:
                    conn.commit()
                    print("  pass2 processed rows:", processed_rows)

            conn.commit()

            if not args.dry_run:
                for row in conn.execute(
                    """
                    SELECT idsite, idaction_url, reconstructed_oai_identifier, rows_to_update
                    FROM update_counts
                    ORDER BY idsite, idaction_url
                    """
                ):
                    updates_writer.writerow(
                        {
                            "idsite": row[0],
                            "idaction_url": row[1],
                            "reconstructed_oai_identifier": row[2],
                            "rows_to_update": row[3],
                        }
                    )

                for row in conn.execute(
                    """
                    SELECT
                        r.idsite,
                        r.rows_missing_prefix,
                        COUNT(a.idaction_url) AS distinct_idaction_url
                    FROM missing_prefix_row_counts r
                    LEFT JOIN missing_prefix_actions a ON a.idsite = r.idsite
                    GROUP BY r.idsite, r.rows_missing_prefix
                    ORDER BY r.idsite
                    """
                ):
                    missing_prefix_sites_writer.writerow(
                        {
                            "idsite": row[0],
                            "rows_missing_prefix": row[1],
                            "distinct_idaction_url": row[2],
                        }
                    )

                write_sql_script(args)

            missing_prefix_sites_count = conn.execute(
                "SELECT COUNT(*) FROM missing_prefix_row_counts"
            ).fetchone()[0]

            print("Summary:")
            for key in [
                "reconstructed",
                "already_present",
                "existing_mismatch",
                "missing_prefix",
                "unresolved_url",
            ]:
                print("  %s: %s" % (key, summary[key]))
            print("  missing_prefix_sites:", missing_prefix_sites_count)

            if not args.dry_run:
                print("Wrote full audit:", args.out_all)
                print("Wrote unresolved rows:", args.out_unresolved)
                print("Wrote updates by action:", args.out_updates)
                print("Wrote missing-prefix sites:", args.out_missing_prefix_sites)
                print("Wrote sql update script:", args.out_sql_update)
            else:
                print("Dry run: no output files written.")

        finally:
            events_fh.close()

    finally:
        conn.close()
        if cleanup_temp_db:
            try:
                os.remove(temp_db_path)
            except OSError:
                pass
        if out_all_fh:
            out_all_fh.close()
        if out_unresolved_fh:
            out_unresolved_fh.close()
        if out_updates_fh:
            out_updates_fh.close()
        if out_missing_prefix_sites_fh:
            out_missing_prefix_sites_fh.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
