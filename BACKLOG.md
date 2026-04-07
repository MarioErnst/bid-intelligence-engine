# Backlog — Bid Intelligence Engine

Issues conocidos, ordenados por severidad. Los resueltos quedan documentados para referencia.

---

## ✅ RESUELTOS

| ID | Descripción | Cómo se resolvió |
|---|---|---|
| B-01 | `fecha_cierre` NULL en ~359 licitaciones | Nuevo script `fix_fechas_cierre.py`: 3-5 API calls al endpoint de listado en lugar de 359 calls al de detalle. Tarda ~10s |
| B-03 | `precio_p25` NULL para productos con < 5 obs | Umbral bajado a 3 en `compute_benchmarks.py` (--min-obs 3 por defecto) |
| B-04 | RUT con puntos (`76.930.423-1`) en licitaciones_mercado y ofertas_sasf | Migración SQL `normalize_rut_remove_dots` — eliminados puntos en 90k+ filas |
| B-05 | SASF_RUT hardcodeado en 6 scripts distintos | Centralizado en `src/core/config.py`, todos los scripts importan desde ahí |
| B-06 | Scoring weights y pricing thresholds hardcodeados | Añadidos a `src/core/config.py` (W_MATCH, THRESH_ALTA, FACTOR_AGRESIVO, etc.) |
| B-07 | Sin retry en upserts a Supabase | `safe_upsert()` con retry exponencial en `src/db/supabase_client.py`, usada en todos los scripts |
| B-08 | `--dias` filter en match_report.py excluía NULL fecha_cierre | Filtro movido a Python-side: NULL se trata como "abierta" |
| B-09 | Sin validación de columnas en ETL al parsear CSV | Validación de columnas críticas al inicio de `parse_csv()` en `etl_mercado_bulk.py` y `etl_sasf_from_bulk.py` |
| B-10 | N+1 queries en compute_match_scores.py | `load_existing_scores()` carga SET de códigos en 1 query al inicio |
| B-11 | pricing_report.py limite de 10 ítems no configurable | Argumento `--max-items N` añadido |
| B-12 | RUT proveedor hardcodeado, no configurable por env | `PROVEEDOR_RUT` en `.env` como override; fallback a `SASF_RUT` de config |

---

## 🔴 ABIERTOS CRÍTICOS

### B-02 · `monto_estimado` NULL para muchas licitaciones
- **Causa raíz**: la API de ChileCompra no siempre retorna `MontoEstimado`.
- **Impacto**: `match_report.py` muestra `"Monto estimado: N/D"`. La cobertura %
  del presupuesto en `pricing_report.py` no se puede calcular cuando falta.
- **Acción**: considerar parsear el PDF de bases de licitación con Vertex AI para
  extraer el monto cuando no lo devuelva la API. Complejidad: alta.

---

## 🟢 ABIERTOS MEJORAS (nice-to-have, no afectan funcionalidad core)

### B-13 · Los ZIPs cacheados de etl_sasf_from_bulk.py ocupan espacio en disco
- **Causa**: `data/cache/` guarda los ZIPs mensuales (~50–200 MB cada uno).
- **Acción**: agregar flag `--clear-cache-after` o limpiar automáticamente
  los ZIPs > 30 días de antigüedad al inicio del script. Ya existe `--no-cache`.

---

*Última actualización: 2026-04-07 — todos los items críticos resueltos excepto B-02 (monto_estimado, requiere integración Vertex AI)*
