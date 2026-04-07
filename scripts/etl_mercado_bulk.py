"""
etl_mercado_bulk.py — Descarga datos masivos de ChileCompra y carga en Supabase.

Descarga archivos ZIP de licitaciones de ChileCompra para el período
enero 2025 - enero 2026, filtra UNSPSC 42 (equipamiento médico),
y carga en la tabla licitaciones_mercado.

URL pattern confirmado (sin cero en el mes):
    https://transparenciachc.blob.core.windows.net/lic-da/{year}-{month}.zip

Uso:
    python scripts/etl_mercado_bulk.py
    python scripts/etl_mercado_bulk.py --desde 2025-3 --hasta 2025-6
    python scripts/etl_mercado_bulk.py --force   # re-procesa meses ya cargados

IMPORTANTE: En la primera ejecución, el script imprime las columnas reales
del CSV antes de procesar. Verifica que COLUMN_MAP sea correcto.
"""

import argparse
import io
import logging
import os
import sys
import zipfile
from pathlib import Path
from typing import Optional

import httpx
import polars as pl
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.db.supabase_client import get_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

BASE_URL = "https://transparenciachc.blob.core.windows.net/lic-da"

# Período por defecto: enero 2025 → enero 2026
DEFAULT_MONTHS = [
    (2025, 1), (2025, 2), (2025, 3), (2025, 4),
    (2025, 5), (2025, 6), (2025, 7), (2025, 8),
    (2025, 9), (2025, 10), (2025, 11), (2025, 12),
    (2026, 1),
]

# UNSPSC 42: Medical Equipment and Accessories and Supplies
UNSPSC42_MIN = 42_000_000
UNSPSC42_MAX = 42_999_999

# Mapeo de columnas ChileCompra → schema de licitaciones_mercado
# Columnas reales verificadas desde el CSV de enero 2025
COLUMN_MAP = {
    # Identificación licitación
    "CodigoExterno":               "codigo_licitacion",
    "Nombre":                      "nombre_licitacion",
    "Estado":                      "estado",
    "Tipo":                        "tipo_licitacion",
    "FechaPublicacion":            "fecha_publicacion",
    "FechaCierre":                 "fecha_cierre",
    "FechaAdjudicacion":           "fecha_adjudicacion",
    # Item (nombres reales del CSV)
    "Correlativo":                 "numero_item",
    "Nombre linea Adquisicion":    "nombre_item",
    "CodigoProductoONU":           "codigo_onu",
    "Nombre producto genrico":     "descripcion_onu",
    "Cantidad":                    "cantidad",
    "UnidadMedida":                "unidad_medida",
    # Oferta adjudicada (ganador)
    # El CSV tiene una fila por oferta; "Oferta seleccionada" marca la ganadora
    # MontoUnitarioOferta = precio unitario ofertado
    # MontoLineaAdjudica = monto total de la línea adjudicada
    "MontoUnitarioOferta":         "precio_unitario_ganador",
    "MontoLineaAdjudica":          "monto_total_adjudicado",
    "RutProveedor":                "rut_ganador",
    "NombreProveedor":             "nombre_ganador",
    # Organismo comprador
    "RutUnidad":                   "rut_unidad_compradora",
    "NombreUnidad":                "nombre_unidad_compradora",
    "RegionUnidad":                "region",
    "sector":                      "sector",
}

# Columna que indica si la oferta fue la ganadora (filtrar solo ganadoras)
COL_OFERTA_SELECCIONADA = "Oferta seleccionada"

# Columnas de fecha que deben convertirse a string ISO
DATE_COLS = {"fecha_publicacion", "fecha_cierre", "fecha_adjudicacion"}

BATCH_SIZE = 500  # filas por request a Supabase


# ---------------------------------------------------------------------------
# Descarga
# ---------------------------------------------------------------------------

def build_url(year: int, month: int) -> str:
    """Sin zero-padding en el mes: 2025-1.zip, 2025-12.zip"""
    return f"{BASE_URL}/{year}-{month}.zip"


def download_zip(year: int, month: int, retries: int = 3) -> Optional[bytes]:
    url = build_url(year, month)
    log.info(f"  Descargando {url} ...")
    for attempt in range(1, retries + 1):
        try:
            response = httpx.get(url, timeout=180, follow_redirects=True)
            response.raise_for_status()
            log.info(f"  ✓ Descargado ({len(response.content) / 1_048_576:.1f} MB)")
            return response.content
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                log.warning(f"  404 — No hay datos para {year}-{month}")
                return None
            log.warning(f"  Intento {attempt}/{retries} fallido: {e}")
        except Exception as e:
            log.warning(f"  Intento {attempt}/{retries} fallido: {e}")
    log.error(f"  ❌ No se pudo descargar {url} después de {retries} intentos")
    return None


def extract_csv_from_zip(zip_bytes: bytes) -> Optional[bytes]:
    """Extrae el CSV más grande del ZIP (o el que tenga 'licitacion' en el nombre)."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            log.error("  El ZIP no contiene archivos CSV")
            return None

        # Preferir el que tiene "licitacion" en el nombre
        licit_csvs = [n for n in csv_names if "licitacion" in n.lower()]
        target = licit_csvs[0] if licit_csvs else max(csv_names, key=lambda n: zf.getinfo(n).file_size)
        log.info(f"  CSV seleccionado: {target}")
        return zf.read(target)


# ---------------------------------------------------------------------------
# Parseo y filtrado
# ---------------------------------------------------------------------------

def parse_csv(csv_bytes: bytes, year: int, month: int, first_run: bool = False) -> pl.DataFrame:
    """Parsea el CSV con polars, filtra ofertas ganadoras y UNSPSC 42."""
    # Intenta latin-1 primero (más común en ChileCompra), luego UTF-8
    df = None
    for encoding in ("latin1", "utf-8"):
        try:
            df = pl.read_csv(
                io.BytesIO(csv_bytes),
                separator=";",
                encoding=encoding,
                infer_schema_length=5000,
                null_values=["", "NULL", "null", "N/A", "NA"],
                ignore_errors=True,
                truncate_ragged_lines=True,
            )
            break
        except Exception as e:
            if encoding == "utf-8":
                log.error(f"  No se pudo parsear CSV: {e}")
                return pl.DataFrame()

    if df is None:
        return pl.DataFrame()

    if first_run:
        log.info(f"  COLUMNAS REALES DEL CSV: {df.columns}")

    # Filtrar solo ofertas ganadoras/seleccionadas antes de cualquier otra cosa
    # El CSV tiene una fila por oferta; solo queremos la oferta adjudicada
    if COL_OFERTA_SELECCIONADA in df.columns:
        df = df.filter(pl.col(COL_OFERTA_SELECCIONADA).cast(pl.Utf8) == "Seleccionada")
        log.info(f"  Filas tras filtro 'Oferta seleccionada=1': {len(df)}")
    else:
        # Fallback: filtrar por Estado de la licitación = Adjudicada
        if "Estado" in df.columns:
            df = df.filter(pl.col("Estado").cast(pl.Utf8).str.contains("(?i)adjudic"))
            log.info(f"  Filas tras filtro Estado=Adjudicada: {len(df)}")

    if df.is_empty():
        return pl.DataFrame()

    # Renombrar columnas según COLUMN_MAP
    rename_map = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    missing = [k for k in COLUMN_MAP if k not in df.columns]
    if missing:
        log.warning(f"  Columnas no encontradas (menores): {missing}")

    df = df.rename(rename_map)

    # Seleccionar solo columnas conocidas
    keep_cols = [v for v in COLUMN_MAP.values() if v in df.columns]
    df = df.select(keep_cols)

    # Filtrar UNSPSC 42
    if "codigo_onu" in df.columns:
        df = df.with_columns(
            pl.col("codigo_onu").cast(pl.Int64, strict=False)
        ).filter(
            pl.col("codigo_onu").is_between(UNSPSC42_MIN, UNSPSC42_MAX)
        )
    else:
        log.warning("  Columna codigo_onu no encontrada — no se puede filtrar UNSPSC 42")
        return pl.DataFrame()

    # numero_item como entero
    if "numero_item" in df.columns:
        df = df.with_columns(
            pl.col("numero_item").cast(pl.Int64, strict=False)
        )

    # Precios como float
    for col in ("precio_unitario_ganador", "monto_total_adjudicado", "cantidad"):
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).cast(pl.Float64, strict=False)
            )

    # Fechas: convertir a string ISO yyyy-mm-dd
    for col in DATE_COLS:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).cast(pl.Utf8, strict=False)
            )

    # Metadatos
    mes_proceso = f"{year}-{month:02d}"
    source_url = build_url(year, month)
    df = df.with_columns([
        pl.lit(mes_proceso).alias("mes_proceso"),
        pl.lit(source_url).alias("fuente_zip"),
    ])

    return df


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def sanitize_row(row: dict) -> dict:
    """Limpia NaN, infinitos y tipos no serializables."""
    import math
    clean = {}
    for k, v in row.items():
        if v is None:
            clean[k] = None
        elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            clean[k] = None
        else:
            clean[k] = v
    return clean


def upsert_batch(supabase, rows: list[dict]):
    for i in range(0, len(rows), BATCH_SIZE):
        batch = [sanitize_row(r) for r in rows[i : i + BATCH_SIZE]]
        supabase.table("licitaciones_mercado").upsert(
            batch,
            on_conflict="codigo_licitacion,numero_item,mes_proceso",
        ).execute()
        log.info(f"    Upserted {min(i + BATCH_SIZE, len(rows))}/{len(rows)} filas")


def already_loaded(supabase, mes_proceso: str) -> bool:
    """Devuelve True si el mes ya tiene datos en Supabase."""
    result = (
        supabase.table("licitaciones_mercado")
        .select("id", count="exact")
        .eq("mes_proceso", mes_proceso)
        .limit(1)
        .execute()
    )
    return bool(result.count and result.count > 0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_month_arg(s: str) -> tuple[int, int]:
    """Parsea 'YYYY-M' o 'YYYY-MM' → (year, month)."""
    parts = s.split("-")
    return int(parts[0]), int(parts[1])


def main():
    parser = argparse.ArgumentParser(description="ETL bulk ChileCompra UNSPSC 42 → Supabase")
    parser.add_argument("--desde", default=None,
                        help="Mes inicial (YYYY-M), ej: 2025-1. Default: 2025-1")
    parser.add_argument("--hasta", default=None,
                        help="Mes final (YYYY-M), ej: 2026-1. Default: 2026-1")
    parser.add_argument("--force", action="store_true",
                        help="Re-procesa meses que ya están cargados en Supabase")
    args = parser.parse_args()

    load_dotenv()
    supabase = get_client()

    # Determinar meses a procesar
    if args.desde or args.hasta:
        desde = parse_month_arg(args.desde) if args.desde else (2025, 1)
        hasta = parse_month_arg(args.hasta) if args.hasta else (2026, 1)
        months = []
        y, m = desde
        while (y, m) <= hasta:
            months.append((y, m))
            m += 1
            if m > 12:
                m = 1
                y += 1
    else:
        months = DEFAULT_MONTHS

    log.info(f"=== ETL Mercado Bulk — {len(months)} meses a procesar ===")
    first_run = True
    total_insertadas = 0

    for idx, (year, month) in enumerate(months):
        mes_proceso = f"{year}-{month:02d}"
        log.info(f"\n[{idx+1}/{len(months)}] Procesando {mes_proceso}...")

        # Idempotencia
        if not args.force and already_loaded(supabase, mes_proceso):
            log.info(f"  {mes_proceso} ya cargado. Saltando (usa --force para reprocesar)")
            continue

        # Descarga
        zip_bytes = download_zip(year, month)
        if zip_bytes is None:
            continue

        # Extrae CSV del ZIP
        csv_bytes = extract_csv_from_zip(zip_bytes)
        if csv_bytes is None:
            continue

        # Parsea y filtra
        df = parse_csv(csv_bytes, year, month, first_run=first_run)
        first_run = False  # solo imprime columnas la primera vez

        if df.is_empty():
            log.warning(f"  Sin filas UNSPSC 42 para {mes_proceso}")
            continue

        log.info(f"  {len(df)} filas UNSPSC 42 encontradas")

        # Deduplicar por la clave única antes del upsert
        df = df.unique(subset=["codigo_licitacion", "numero_item", "mes_proceso"], keep="last")
        log.info(f"  {len(df)} filas tras deduplicación")

        # Upsert
        rows = df.to_dicts()
        upsert_batch(supabase, rows)
        total_insertadas += len(rows)
        log.info(f"  ✓ {mes_proceso} completado")

    log.info(f"\n=== ETL Mercado Bulk finalizado. Total filas insertadas: {total_insertadas} ===")


if __name__ == "__main__":
    main()
