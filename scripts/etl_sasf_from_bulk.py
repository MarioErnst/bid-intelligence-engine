"""
etl_sasf_from_bulk.py — Extrae ofertas de SASF desde los ZIPs bulk de ChileCompra.

No requiere Excel. Descarga los mismos ZIPs mensuales, filtra por RutProveedor
de SASF (76.930.423-1), extrae TODAS sus ofertas (ganadoras y perdedoras),
enriquece con precio del ganador, calcula gaps, y carga en ofertas_sasf.

Cachea los ZIPs en data/cache/ para no re-descargar.

Uso:
    python3 scripts/etl_sasf_from_bulk.py
    python3 scripts/etl_sasf_from_bulk.py --rut 76.930.423-1
    python3 scripts/etl_sasf_from_bulk.py --desde 2025-1 --hasta 2026-1
    python3 scripts/etl_sasf_from_bulk.py --force   # reprocesa meses ya cargados
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

DEFAULT_MONTHS = [
    (2025, 1), (2025, 2), (2025, 3), (2025, 4),
    (2025, 5), (2025, 6), (2025, 7), (2025, 8),
    (2025, 9), (2025, 10), (2025, 11), (2025, 12),
    (2026, 1),
]

SASF_RUT_DEFAULT = "76.930.423-1"

UNSPSC42_MIN = 42_000_000
UNSPSC42_MAX = 42_999_999

CACHE_DIR = Path("data/cache")
BATCH_SIZE = 500


# ---------------------------------------------------------------------------
# Normalización de RUT
# ---------------------------------------------------------------------------

def normalize_rut(rut: str) -> str:
    """76.930.423-1 → 76930423-1 (sin puntos, con guión)"""
    return rut.replace(".", "").strip()


# ---------------------------------------------------------------------------
# Descarga con caché
# ---------------------------------------------------------------------------

def build_url(year: int, month: int) -> str:
    return f"{BASE_URL}/{year}-{month}.zip"


def get_zip_bytes(year: int, month: int, retries: int = 3) -> Optional[bytes]:
    """Descarga el ZIP (con caché en disco)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / f"{year}-{month}.zip"

    if cache_path.exists():
        log.info(f"  Cache hit: {cache_path} ({cache_path.stat().st_size / 1_048_576:.1f} MB)")
        return cache_path.read_bytes()

    url = build_url(year, month)
    log.info(f"  Descargando {url} ...")
    for attempt in range(1, retries + 1):
        try:
            response = httpx.get(url, timeout=300, follow_redirects=True)
            response.raise_for_status()
            data = response.content
            cache_path.write_bytes(data)
            log.info(f"  ✓ Descargado y cacheado ({len(data) / 1_048_576:.1f} MB)")
            return data
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                log.warning(f"  404 — No hay datos para {year}-{month}")
                return None
            log.warning(f"  Intento {attempt}/{retries}: {e}")
        except Exception as e:
            log.warning(f"  Intento {attempt}/{retries}: {e}")

    log.error(f"  ❌ No se pudo descargar {url}")
    return None


def extract_csv_from_zip(zip_bytes: bytes) -> Optional[bytes]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            return None
        licit_csvs = [n for n in csv_names if "licitacion" in n.lower()]
        target = licit_csvs[0] if licit_csvs else max(
            csv_names, key=lambda n: zf.getinfo(n).file_size
        )
        log.info(f"  CSV: {target}")
        return zf.read(target)


# ---------------------------------------------------------------------------
# Procesamiento del CSV
# ---------------------------------------------------------------------------

def process_csv(
    csv_bytes: bytes,
    year: int,
    month: int,
    sasf_rut_norm: str,
    first_run: bool = False,
) -> pl.DataFrame:
    """
    Lee el CSV completo (sin filtro de Seleccionada), extrae las filas de SASF
    en UNSPSC 42, enriquece con precio del ganador, calcula gaps.
    """
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
                log.error(f"  No se pudo parsear: {e}")
                return pl.DataFrame()

    if df is None or df.is_empty():
        return pl.DataFrame()

    if first_run:
        log.info(f"  COLUMNAS CSV: {df.columns}")

    # ── Normalizar RUT proveedor para comparación ──────────────────────────
    if "RutProveedor" not in df.columns:
        log.warning("  Columna RutProveedor no encontrada en CSV")
        return pl.DataFrame()

    df = df.with_columns(
        pl.col("RutProveedor").cast(pl.Utf8).str.replace_all(r"\.", "").str.strip_chars()
        .alias("_rut_norm")
    )

    # ── Filtrar UNSPSC 42 ──────────────────────────────────────────────────
    if "CodigoProductoONU" not in df.columns:
        log.warning("  Columna CodigoProductoONU no encontrada")
        return pl.DataFrame()

    df = df.with_columns(
        pl.col("CodigoProductoONU").cast(pl.Int64, strict=False).alias("_onu_int")
    ).filter(
        pl.col("_onu_int").is_between(UNSPSC42_MIN, UNSPSC42_MAX)
    )
    log.info(f"  Filas UNSPSC 42 (todas las ofertas): {len(df)}")

    if df.is_empty():
        return pl.DataFrame()

    # ── Separar: ofertas de SASF vs ofertas ganadoras ─────────────────────
    sasf_df = df.filter(pl.col("_rut_norm") == sasf_rut_norm)
    log.info(f"  Ofertas de SASF encontradas: {len(sasf_df)}")

    if sasf_df.is_empty():
        return pl.DataFrame()

    # Ganadores: una fila por (licitacion, item) con precio del adjudicado
    winner_col = "Oferta seleccionada"
    if winner_col in df.columns:
        winner_df = (
            df.filter(pl.col(winner_col).cast(pl.Utf8) == "Seleccionada")
            .select([
                "CodigoExterno",
                "Correlativo",
                pl.col("MontoUnitarioOferta").cast(pl.Float64, strict=False).alias("_precio_ganador"),
                pl.col("NombreProveedor").cast(pl.Utf8).alias("_nombre_ganador"),
                pl.col("RutProveedor").cast(pl.Utf8).alias("_rut_ganador"),
            ])
            .unique(subset=["CodigoExterno", "Correlativo"], keep="last")
        )
    else:
        log.warning("  Columna 'Oferta seleccionada' no encontrada — precio_ganador será null")
        winner_df = pl.DataFrame(schema={
            "CodigoExterno": pl.Utf8,
            "Correlativo": pl.Utf8,
            "_precio_ganador": pl.Float64,
            "_nombre_ganador": pl.Utf8,
            "_rut_ganador": pl.Utf8,
        })

    # Cast Correlativo a string en ambos para el join
    sasf_df = sasf_df.with_columns(pl.col("CodigoExterno").cast(pl.Utf8), pl.col("Correlativo").cast(pl.Utf8))
    winner_df = winner_df.with_columns(pl.col("CodigoExterno").cast(pl.Utf8), pl.col("Correlativo").cast(pl.Utf8))

    # ── Join: SASF + ganador ───────────────────────────────────────────────
    joined = sasf_df.join(winner_df, on=["CodigoExterno", "Correlativo"], how="left")

    # ── Construir dataframe final ──────────────────────────────────────────
    mes_proceso = f"{year}-{month:02d}"
    source_url = build_url(year, month)

    def safe_col(name: str, dtype=pl.Utf8):
        if name in joined.columns:
            return pl.col(name).cast(dtype, strict=False)
        return pl.lit(None).cast(dtype)

    result = joined.select([
        safe_col("CodigoExterno").alias("id_licitacion"),
        safe_col("CodigoProductoONU", pl.Int64).alias("codigo_onu"),
        safe_col("FechaAdjudicacion").alias("fecha_adjudicacion"),
        pl.lit(sasf_rut_norm).alias("rut_proveedor_sasf"),
        safe_col("Nombre linea Adquisicion").alias("nombre_item"),
        safe_col("MontoUnitarioOferta", pl.Float64).alias("monto_neto_oferta"),
        safe_col("Cantidad", pl.Float64).alias("cantidad_oferta"),
        safe_col("Oferta seleccionada").alias("resultado_oferta"),
        safe_col("NombreUnidad").alias("unidad_compra"),
        safe_col("RutUnidad").alias("unidad_compra_rut"),
        safe_col("sector").alias("sector"),
        safe_col("RegionUnidad").alias("region_unidad"),
        # Estado de la licitacion
        safe_col("Estado").alias("estado_licitacion"),
        # Ganador (del join)
        pl.col("_precio_ganador").alias("precio_ganador") if "_precio_ganador" in joined.columns else pl.lit(None).cast(pl.Float64).alias("precio_ganador"),
        pl.col("_nombre_ganador").alias("proveedor_ganador") if "_nombre_ganador" in joined.columns else pl.lit(None).cast(pl.Utf8).alias("proveedor_ganador"),
        pl.col("_rut_ganador").alias("rut_ganador") if "_rut_ganador" in joined.columns else pl.lit(None).cast(pl.Utf8).alias("rut_ganador"),
        # Metadatos
        pl.lit(mes_proceso).alias("mes_proceso"),
        pl.lit(source_url).alias("fuente_excel"),
    ])

    # ── Calcular gaps ──────────────────────────────────────────────────────
    result = result.with_columns([
        (pl.col("monto_neto_oferta") - pl.col("precio_ganador")).alias("gap_monetario"),
        pl.when(
            pl.col("precio_ganador").is_not_null() & (pl.col("precio_ganador") > 0)
        ).then(
            (pl.col("monto_neto_oferta") - pl.col("precio_ganador")) / pl.col("precio_ganador") * 100
        ).otherwise(None).alias("gap_porcentual"),
    ])

    # ── Motivo de pérdida simplificado ────────────────────────────────────
    result = result.with_columns(
        pl.when(
            (pl.col("resultado_oferta") != "Seleccionada") &
            pl.col("gap_monetario").is_not_null() &
            (pl.col("gap_monetario") > 0)
        ).then(pl.lit("PRECIO"))
        .when(
            (pl.col("resultado_oferta") != "Seleccionada") &
            pl.col("gap_monetario").is_not_null() &
            (pl.col("gap_monetario") <= 0)
        ).then(pl.lit("OTRO"))
        .otherwise(None)
        .alias("motivo_perdida")
    )

    # Deduplicar por clave única de la tabla
    result = result.unique(
        subset=["id_licitacion", "codigo_onu", "fecha_adjudicacion"],
        keep="last",
    )

    log.info(f"  {len(result)} filas SASF listas para upsert")
    return result


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def sanitize_row(row: dict) -> dict:
    import math
    return {
        k: (None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v)
        for k, v in row.items()
    }


def upsert_batch(supabase, rows: list[dict]):
    for i in range(0, len(rows), BATCH_SIZE):
        batch = [sanitize_row(r) for r in rows[i: i + BATCH_SIZE]]
        supabase.table("ofertas_sasf").upsert(
            batch,
            on_conflict="id_licitacion,codigo_onu,fecha_adjudicacion",
        ).execute()
        log.info(f"    Upserted {min(i + BATCH_SIZE, len(rows))}/{len(rows)}")


def already_loaded(supabase, mes_proceso: str) -> bool:
    result = (
        supabase.table("ofertas_sasf")
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
    parts = s.split("-")
    return int(parts[0]), int(parts[1])


def main():
    parser = argparse.ArgumentParser(
        description="Extrae ofertas de SASF desde ZIPs bulk → Supabase"
    )
    parser.add_argument("--rut", default=SASF_RUT_DEFAULT,
                        help=f"RUT de SASF (default: {SASF_RUT_DEFAULT})")
    parser.add_argument("--desde", default=None, help="Mes inicial YYYY-M")
    parser.add_argument("--hasta", default=None, help="Mes final YYYY-M")
    parser.add_argument("--force", action="store_true",
                        help="Reprocesar meses ya cargados")
    parser.add_argument("--no-cache", action="store_true",
                        help="No usar caché de ZIPs (re-descarga siempre)")
    args = parser.parse_args()

    load_dotenv()
    supabase = get_client()

    sasf_rut_norm = normalize_rut(args.rut)
    log.info(f"RUT SASF normalizado: {sasf_rut_norm}")

    if args.no_cache:
        log.info("Modo sin caché activado — re-descargando ZIPs")
        # Limpiar caché si existe
        for f in CACHE_DIR.glob("*.zip"):
            f.unlink()

    # Determinar período
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

    log.info(f"=== ETL SASF from Bulk — {len(months)} meses — RUT: {sasf_rut_norm} ===")
    first_run = True
    total = 0

    for idx, (year, month) in enumerate(months):
        mes_proceso = f"{year}-{month:02d}"
        log.info(f"\n[{idx+1}/{len(months)}] {mes_proceso}")

        if not args.force and already_loaded(supabase, mes_proceso):
            log.info(f"  Ya cargado. Saltando (usa --force para reprocesar)")
            continue

        zip_bytes = get_zip_bytes(year, month)
        if zip_bytes is None:
            continue

        csv_bytes = extract_csv_from_zip(zip_bytes)
        if csv_bytes is None:
            log.warning("  No se encontró CSV en el ZIP")
            continue

        df = process_csv(csv_bytes, year, month, sasf_rut_norm, first_run=first_run)
        first_run = False

        if df.is_empty():
            log.info(f"  Sin ofertas de SASF en UNSPSC 42 para {mes_proceso}")
            continue

        upsert_batch(supabase, df.to_dicts())
        total += len(df)
        log.info(f"  ✓ {mes_proceso} completado")

    log.info(f"\n=== ETL SASF finalizado. Total filas insertadas: {total} ===")


if __name__ == "__main__":
    main()
