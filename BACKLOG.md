# Backlog — Bid Intelligence Engine

Issues conocidos, ordenados por severidad. Para cada uno: causa raíz, impacto y acción sugerida.

---

## 🔴 CRÍTICOS (afectan datos o causan N/D visibles)

### B-01 · `fecha_cierre` NULL en ~359/359 licitaciones en DB
- **Estado**: parcialmente resuelto. `parse_date()` ya fue corregido para el formato
  `DD/MM/YYYY HH:MM:SS` de ChileCompra.
- **Causa raíz**: las 359 licitaciones existentes fueron fetcheadas antes del fix.
- **Impacto**: el filtro `--dias` de `match_report.py` no funciona para registros viejos
  (usa `fecha_cierre` en DB, que es NULL). El cómputo de "días restantes" muestra `?`.
- **Acción**: re-ejecutar `python3 scripts/fetch_open_licitaciones.py --force` para
  reescribir los registros con la fecha correcta. Tarda ~3h por los rate limits de la API.

---

### B-02 · `monto_estimado` NULL para muchas licitaciones
- **Estado**: abierto.
- **Causa raíz**: la API de ChileCompra no siempre retorna `MontoEstimado`.
  Aparece como `$0` o ausente en la respuesta JSON.
- **Impacto**: `match_report.py` muestra `"Monto estimado: N/D"`. La cobertura %
  del presupuesto en `pricing_report.py` no se puede calcular.
- **Acción**: no hay fix sin la fuente de datos. Considerar parsear el PDF de bases
  de licitación con Vertex AI para extraer el monto cuando falte en la API.

---

### B-03 · `precio_p25` y `precio_mediana` NULL para productos con < 5 observaciones
- **Estado**: abierto.
- **Causa raíz**: `compute_benchmarks.py` filtra por `n_observaciones >= 5` como mínimo
  estadístico. Productos con poca historia de mercado no tienen benchmark.
- **Impacto**: en `pricing_report.py` aparece `"$       N/D"` en columnas P25 y AGRESIVO.
  `compute_pricing.py` registra el ítem como `n_items_sin_precio`.
- **Acción**: bajar el umbral a `n_observaciones >= 3` o usar el precio promedio como
  proxy cuando no hay suficientes observaciones. Evaluar trade-off calidad/cobertura.

---

### B-04 · RUT de SASF en dos formatos distintos en la DB
- **Estado**: abierto.
- **Causa raíz**: `etl_sasf_from_bulk.py` normaliza el RUT a `"76930423-1"` (sin puntos),
  pero `etl_sasf_batch.py` puede guardarlo como `"76.930.423-1"` (con puntos) dependiendo
  del contenido del Excel origen.
- **Impacto**: queries que filtren por `rut_proveedor_sasf` podrían duplicar o perder filas.
- **Acción**: ejecutar una migración SQL de normalización:
  ```sql
  UPDATE ofertas_sasf
  SET rut_proveedor_sasf = REPLACE(rut_proveedor_sasf, '.', '')
  WHERE rut_proveedor_sasf LIKE '%.%';
  ```
  Agregar validación en `etl_sasf_batch.py` antes del upsert para forzar el formato sin puntos.

---

## 🟡 MEDIOS (calidad de código, mantenibilidad)

### B-05 · Constantes SASF_RUT y UNSPSC42 duplicadas en 6 archivos
- **Estado**: las constantes fueron centralizadas en `src/core/config.py` como
  `SASF_RUT`, `UNSPSC42_MIN`, `UNSPSC42_MAX`.
- **Acción pendiente**: reemplazar las definiciones locales en cada script con imports:
  ```python
  from src.core.config import SASF_RUT, UNSPSC42_MIN, UNSPSC42_MAX
  ```
  Archivos afectados: `fetch_open_licitaciones.py`, `compute_match_scores.py`,
  `compute_pricing.py`, `pricing_report.py`, `compute_loss_diagnostics.py`,
  `loss_report.py`, `etl_mercado_bulk.py`, `etl_sasf_from_bulk.py`.

---

### B-06 · Pesos de scoring hardcodeados (no configurables)
- **Estado**: abierto.
- **Causa raíz**: `W_MATCH=0.45`, `W_WIN_RATE=0.25`, `W_EXPERIENCIA=0.20`, `W_MERCADO=0.10`
  en `compute_match_scores.py` y los thresholds `THRESH_ALTA=60`, `THRESH_MEDIA=35`
  son valores de diseño sin validación empírica.
- **Acción**: mover a `src/core/config.py` o a un archivo `config/scoring.yaml`
  para facilitar ajustes sin tocar código.

---

### B-07 · Falta retry/error handling en upserts a Supabase
- **Estado**: abierto.
- **Causa raíz**: todos los upserts en `compute_*.py` no tienen try/except.
  Si Supabase devuelve un error de red o constraint violation, el script falla sin
  mensaje descriptivo.
- **Acción**: envolver cada `.execute()` en un bloque try/except que loguee el error
  y continúe con el siguiente batch. Añadir función helper `_safe_upsert()` en
  `src/db/supabase_client.py`.

---

### B-08 · `match_report.py --dias` no funciona con `fecha_cierre` NULL
- **Estado**: abierto (relacionado con B-01).
- **Causa raíz**: el filtro `--dias` llama a `q.gte("fecha_cierre", hoy_str)` en la DB.
  Supabase excluye filas con NULL en la comparación, por lo que retorna 0 resultados.
- **Acción**: aplicar el mismo patrón de filtro Python-side que `compute_match_scores.py`:
  cargar todas las filas y filtrar `fecha_cierre >= hoy OR fecha_cierre IS NULL`.

---

### B-09 · `etl_sasf_from_bulk.py` — columnas del CSV de ChileCompra sin validar
- **Estado**: abierto.
- **Causa raíz**: el `COLUMN_MAP` de `etl_mercado_bulk.py` asume nombres exactos de
  columnas en los ZIPs de ChileCompra. Si ChileCompra cambia los headers (ha ocurrido),
  el ETL falla silenciosamente o produce columnas vacías.
- **Acción**: agregar al inicio del ETL un assert que verifique que las columnas
  esperadas están presentes. Loguear warning con los headers reales si hay discrepancia.

---

## 🟢 MEJORAS (nice-to-have)

### B-10 · `compute_match_scores.py` hace N llamadas a DB para verificar `score_already_exists`
- **Causa raíz**: un SELECT por licitación para chequear idempotencia (O(N) queries).
- **Acción**: cargar todos los `codigo_licitacion` existentes en un SET al inicio y
  filtrar en Python. Reduce de N a 1 query.

### B-11 · `pricing_report.py` muestra máximo 10 ítems por defecto
- **Causa**: diseño conservador.
- **Acción**: exponer `--max-items N` como argumento CLI.

### B-12 · Sin soporte para múltiples proveedores (solo SASF)
- **Causa**: `SASF_RUT` hardcodeado en todos los scripts.
- **Acción**: todos los scripts ya aceptan `--rut` como argumento. Hacer el default
  configurable desde `.env` (`PROVEEDOR_RUT`).

### B-13 · Los ZIPs del mercado (etl_mercado_bulk.py) no se limpian tras procesar
- **Causa**: los archivos se descomprimen en `data/cache/` y quedan ahí.
- **Acción**: agregar `shutil.rmtree(tmp_dir)` al finalizar cada mes.

---

*Última actualización: 2026-04-07*
