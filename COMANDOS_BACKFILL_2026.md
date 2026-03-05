```bash
cd /workspace/lareferencia-usage-stats-processor
rm -f missing_ids_2026_events.csv missing_ids_2026_prefix_map.csv backfill_oai_audit.csv backfill_oai_updates_by_action.csv backfill_oai_unresolved.csv backfill_missing_prefix_sites.csv backfill_oai_apply_updates.sql
python export_missing_ids_2026.py -c config.ini
python backfill_oai_identifier.py --events missing_ids_2026_events.csv --prefix-map missing_ids_2026_prefix_map.csv --sql-date-from '2026-01-01 00:00:00' --sql-date-to '2026-03-01 00:00:00' --dry-run
python backfill_oai_identifier.py --events missing_ids_2026_events.csv --prefix-map missing_ids_2026_prefix_map.csv --sql-date-from '2026-01-01 00:00:00' --sql-date-to '2026-03-01 00:00:00'
ls -lh missing_ids_2026_events.csv missing_ids_2026_prefix_map.csv backfill_oai_audit.csv backfill_oai_updates_by_action.csv backfill_oai_unresolved.csv backfill_missing_prefix_sites.csv backfill_oai_apply_updates.sql
mysql --local-infile=1 -h <MATOMO_HOST> -P <MATOMO_PORT> -u <MATOMO_USER> -p <MATOMO_DB> < backfill_oai_apply_updates.sql
```
