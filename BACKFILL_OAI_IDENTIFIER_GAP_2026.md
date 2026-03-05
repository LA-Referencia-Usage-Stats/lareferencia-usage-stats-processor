# Backfill de `oai_identifier` para gap Matomo (enero-febrero 2026)

## 1. Problema

Durante la migracion a Matomo v5, se perdio temporalmente el registro de la Custom Variable que guarda el identificador OAI para repositorios (`custom_var_v1`).

Impacto:
- Ventana afectada: `2026-01-01` a `2026-02-28` (inclusive).
- Tipo de fuente mas afectado: repositorios (`source.type = 'R'`).
- Consecuencia funcional: el pipeline no puede agrupar/normalizar correctamente por item cuando falta `oai_identifier`.

## 2. Hipotesis de recuperacion

Aunque falte `custom_var_v1`, se puede reconstruir en la mayoria de casos porque:
- En eventos Matomo existe `idsite`.
- En la base de admin (`usage-db`) existe mapeo `site_id -> identifier_prefix` en tabla `source`.
- En `matomo_log_action.name` (URL de accion) suele venir el `handle`:
  - `https://hdl.handle.net/<prefix>/<suffix>`
  - `.../bitstream/handle/<prefix>/<suffix>/...`
  - `.../handle/<prefix>/<suffix>`

Con eso:
- `oai_identifier_reconstruido = normalize(prefix) + ":" + handle_extraido`
- Ejemplo:
  - `identifier_prefix = oai:rabida.uhu.es`
  - URL: `hdl.handle.net/10810/68868`
  - Resultado: `oai:rabida.uhu.es:10810/68868`

## 3. Restricciones y decisiones

- No sobrescribir valores existentes en `custom_var_v1` durante update final.
- Tratar mapeo por clave compuesta `(idsite, idaction_url)`, no solo `idaction_url`.
- Se asume input deduplicado por `(idsite, idaction_url)` desde export; no se valida ambiguedad en runtime.
- Dejar casos no resolubles para revision manual (sin inventar identificador).

## 4. Solucion implementada (offline)

Se agrego script:
- `/Users/lmatas/source/lareferencia-stats/lareferencia-usage-stats-processor/backfill_oai_identifier.py`

Objetivo del script:
- Trabajar offline con dumps (CSV/TSV), sin tocar Matomo directamente.
- Reconstruir candidatos de `oai_identifier`.
- Generar artefactos para auditoria y update posterior.

Entradas minimas:
- `events`:
  - `idsite`
  - `idaction_url`
  - `action_url`
  - opcional `custom_var_v1`
- `prefix-map`:
  - `site_id`
  - `identifier_prefix`

Algoritmo:
1. Carga mapa `site_id -> identifier_prefix` y normaliza prefijo:
   - si no empieza con `oai:`, lo agrega.
   - limpia `:` o `/` al final.
2. Recorre eventos y extrae `handle` desde `action_url` con regex:
   - `hdl.handle.net/...`
   - `/bitstream/(handle/)?...`
   - `/handle/...`
3. Construye candidato `oai_identifier`.
4. Evalua estado de cada fila.
5. Deduplica updates por `(idsite, idaction_url, reconstructed_oai_identifier)`.
6. Genera script SQL para aplicar update por `JOIN` en Matomo.

Implementacion de performance:
- Se asume que el input de eventos ya viene deduplicado por `(idsite,idaction_url)` desde el export.
- El script procesa en una pasada de clasificacion y usa SQLite temporal para contadores/salidas sin crecer RAM.
- El progreso se reporta cada N filas (`--progress-every`).

Estados producidos:
- `reconstructed`
- `already_present`
- `existing_mismatch`
- `missing_prefix`
- `unresolved_url`

## 5. Uso del script

Dry-run:

```bash
python3 /Users/lmatas/source/lareferencia-stats/lareferencia-usage-stats-processor/backfill_oai_identifier.py \
  --events /path/events.csv \
  --prefix-map /path/site_prefix.csv \
  --dry-run
```

Ejecucion real (genera archivos):

```bash
python3 /Users/lmatas/source/lareferencia-stats/lareferencia-usage-stats-processor/backfill_oai_identifier.py \
  --events /path/events.csv \
  --prefix-map /path/site_prefix.csv \
  --out-all /path/backfill_oai_audit.csv \
  --out-updates /path/backfill_oai_updates_by_action.csv \
  --out-unresolved /path/backfill_oai_unresolved.csv
```

Opciones utiles:
- Separador: `--events-sep`, `--prefix-sep`
- Nombres de columnas:
  - `--events-site-col`
  - `--events-idaction-col`
  - `--events-action-url-col`
  - `--events-current-oai-col`
  - `--prefix-site-col`
  - `--prefix-value-col`
- Escalabilidad:
  - `--temp-db-path` para fijar archivo SQLite temporal
  - `--progress-every`
- SQL de aplicacion:
  - `--out-sql-update`
  - `--sql-target-table`
  - `--sql-target-column`
  - `--sql-date-from`
  - `--sql-date-to`
  - `--sql-null-only`

## 6. Artefactos de salida y para que sirven

- `backfill_oai_audit.csv`
  - Dataset completo con diagnostico por fila.
- `backfill_oai_updates_by_action.csv`
  - Update candidates deduplicados por `(idsite,idaction_url)`.
  - Es el insumo principal para un update SQL masivo.
- `backfill_oai_unresolved.csv`
  - Casos que requieren intervencion manual o nuevas reglas.
- `backfill_missing_prefix_sites.csv`
  - Lista deduplicada de `idsite` sin `identifier_prefix` declarado.
  - Incluye conteo de filas afectadas y cantidad de `idaction_url` distintos.
- `backfill_oai_apply_updates.sql`
  - SQL listo para cargar `backfill_oai_updates_by_action.csv` y ejecutar `UPDATE ... JOIN`.

## 7. Estrategia de update en Matomo (fase posterior)

Recomendacion de secuencia:
1. Crear backup de filas objetivo (enero-febrero 2026 con `custom_var_v1` vacio).
2. Cargar `backfill_oai_updates_by_action.csv` a una tabla temporal.
3. Ejecutar `UPDATE ... JOIN` por `(idsite, idaction_url)`.
4. Restringir update a filas con `custom_var_v1` vacio.
5. Auditar conteos antes/despues.

Importante:
- No actualizar registros fuera de la ventana temporal sin justificacion.
- No pisar valores de `custom_var_v1` ya existentes.

## 8. Validaciones recomendadas (antes de actualizar)

Chequeo opcional de ambiguedad sobre datos historicos:

```sql
SELECT idsite, idaction_url, COUNT(DISTINCT custom_var_v1) AS ids_distintos
FROM matomo_log_link_visit_action
WHERE custom_var_v1 IS NOT NULL AND custom_var_v1 <> ''
GROUP BY idsite, idaction_url
HAVING COUNT(DISTINCT custom_var_v1) > 1;
```

Si el resultado devuelve filas, hay que revisar esos `idaction_url` antes de aplicar update masivo.

## 9. Relacion con el pipeline actual

En el processor:
- Para repositorio (`R`), el identificador crudo se toma de `custom_var_v1`.
- Luego se renombra a `oai_identifier`.
- Posteriormente se normaliza/reemplaza en `IdentifierFilterStage`.

Archivos clave:
- `/Users/lmatas/source/lareferencia-stats/lareferencia-usage-stats-processor/stages/s3parquet_istage.py`
- `/Users/lmatas/source/lareferencia-stats/lareferencia-usage-stats-processor/stages/identifier_fstage.py`

## 10. Tareas pendientes para otro agente IA

1. Implementar tabla de excepciones por `idsite` para patrones URL especiales.
2. Agregar tests unitarios de extraccion de handle con URLs reales del gap.
3. Agregar modo de lectura directa desde DB (Matomo + usage-db), manteniendo `--dry-run`.
4. Construir reporte final de cobertura por sitio:
   - `% reconstruido`
   - `% no resuelto`
   - `% ambiguo` (si se reintroduce una capa de validacion de ambiguedad)

## 11. Riesgos conocidos

- URLs de accion sin handle visible no son reconstruibles automaticamente.
- Un mismo `idaction_url` puede representar mas de un item en casos raros.
- Prefijos en admin incorrectos/desactualizados introducen identificadores errados.

## 12. Criterio de exito

Se considera exitoso cuando:
- Se recupera la mayor parte de filas faltantes en la ventana afectada.
- No se pisan valores validos existentes.
- Los casos no resueltos quedan explicitamente listados para tratamiento manual.
