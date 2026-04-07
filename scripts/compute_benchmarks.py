"""
compute_benchmarks.py — Calcula benchmarks de precios por código ONU y carga en Supabase.

Lee licitaciones_mercado (solo adjudicadas con precio > 0), agrega
estadísticas por codigo_onu, y upsertea en precios_benchmark.

Uso:
    python scripts/compute_benchmarks.py
    python scripts/compute_benchmarks.py --min-obs 3   # mínimo 3 observaciones
"""

import argparse
import logging
import math
import sys
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.db.supabase_client import get_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

PAGE_SIZE = 1000   # filas por página al leer Supabase
BATCH_SIZE = 500   # filas por upsert


# ---------------------------------------------------------------------------
# Lectura paginada
# ---------------------------------------------------------------------------

def fetch_all_adjudicadas(supabase) -> list[dict]:
    """
    Lee todas las filas de licitaciones_mercado donde estado = 'Adjudicada'
    y precio_unitario_ganador > 0, con paginación automática.
    """
    log.info("Leyendo licitaciones_mercado (adjudicadas con precio > 0)...")
    all_rows = []
    offset = 0

    while True:
        result = (
            supabase.table("licitaciones_mercado")
            .select(
                "codigo_onu,descripcion_onu,precio_unitario_ganador,"
                "fecha_adjudicacion,codigo_licitacion,rut_ganador"
            )
            .eq("estado", "Adjudicada")
            .gt("precio_unitario_ganador", 0)
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        batch = result.data or []
        all_rows.extend(batch)
        log.info(f"  Leídas {len(all_rows)} filas...")
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    log.info(f"Total filas leídas: {len(all_rows)}")
    return all_rows


# ---------------------------------------------------------------------------
# Limpieza de outliers
# ---------------------------------------------------------------------------

def remove_outliers_iqr(df: pl.DataFrame, col: str, factor: float = 3.0) -> pl.DataFrame:
    """
    Elimina outliers usando IQR × factor.
    Precios de $1 o $999.999.999 son errores de carga en el sistema.
    """
    q1 = df[col].quantile(0.25)
    q3 = df[col].quantile(0.75)
    if q1 is None or q3 is None:
        return df
    iqr = q3 - q1
    lower = q1 - factor * iqr
    upper = q3 + factor * iqr
    return df.filter(
        (pl.col(col) >= lower) & (pl.col(col) <= upper)
    )


# ---------------------------------------------------------------------------
# Agregación
# ---------------------------------------------------------------------------

def compute(rows: list[dict], min_obs: int) -> list[dict]:
    if not rows:
        log.warning("Sin datos para agregar.")
        return []

    df = pl.DataFrame(rows)
    df = df.with_columns([
        pl.col("precio_unitario_ganador").cast(pl.Float64, strict=False),
        pl.col("codigo_onu").cast(pl.Int64, strict=False),
    ]).filter(
        pl.col("precio_unitario_ganador").is_not_null()
        & pl.col("codigo_onu").is_not_null()
        & (pl.col("precio_unitario_ganador") > 0)
    )

    log.info(f"Filas válidas para agregación: {len(df)}")

    # Aplica filtro de outliers por grupo
    grupos = df["codigo_onu"].unique().to_list()
    limpio_frames = []
    for codigo in grupos:
        grupo = df.filter(pl.col("codigo_onu") == codigo)
        if len(grupo) >= 4:  # necesitamos al menos 4 para IQR
            grupo = remove_outliers_iqr(grupo, "precio_unitario_ganador")
        limpio_frames.append(grupo)

    if not limpio_frames:
        return []

    df_limpio = pl.concat(limpio_frames)
    log.info(f"Filas tras limpieza de outliers: {len(df_limpio)}")

    # Agrega por codigo_onu
    benchmarks = df_limpio.group_by("codigo_onu").agg([
        pl.col("descripcion_onu").first().alias("descripcion_onu"),
        pl.col("precio_unitario_ganador").median().alias("precio_mediana"),
        pl.col("precio_unitario_ganador").quantile(0.25).alias("precio_p25"),
        pl.col("precio_unitario_ganador").quantile(0.75).alias("precio_p75"),
        pl.col("precio_unitario_ganador").min().alias("precio_min"),
        pl.col("precio_unitario_ganador").max().alias("precio_max"),
        pl.col("precio_unitario_ganador").mean().alias("precio_promedio"),
        pl.col("precio_unitario_ganador").std().alias("desviacion_estandar"),
        pl.col("precio_unitario_ganador").count().alias("n_observaciones"),
        pl.col("codigo_licitacion").n_unique().alias("n_licitaciones"),
        pl.col("rut_ganador").n_unique().alias("n_proveedores"),
        pl.col("fecha_adjudicacion").min().alias("fecha_desde"),
        pl.col("fecha_adjudicacion").max().alias("fecha_hasta"),
    ]).filter(
        pl.col("n_observaciones") >= min_obs
    ).sort("codigo_onu")

    log.info(f"Códigos ONU con benchmark ({min_obs}+ observaciones): {len(benchmarks)}")

    # Convierte a lista de dicts con redondeo
    rows_out = []
    for row in benchmarks.to_dicts():
        clean = {}
        for k, v in row.items():
            if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                clean[k] = None
            elif isinstance(v, float):
                clean[k] = round(v, 2)
            else:
                clean[k] = v
        rows_out.append(clean)

    return rows_out


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def upsert_benchmarks(supabase, rows: list[dict]):
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        supabase.table("precios_benchmark").upsert(
            batch,
            on_conflict="codigo_onu",
        ).execute()
        total += len(batch)
        log.info(f"  Upserted {total}/{len(rows)} benchmarks")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Calcula benchmarks de precios por código ONU")
    parser.add_argument("--min-obs", type=int, default=5,
                        help="Mínimo de observaciones para incluir un código (default: 5)")
    args = parser.parse_args()

    load_dotenv()
    supabase = get_client()

    log.info("=== Compute Benchmarks ===")

    # 1. Leer datos
    rows = fetch_all_adjudicadas(supabase)
    if not rows:
        log.error("No hay datos en licitaciones_mercado. Ejecuta etl_mercado_bulk.py primero.")
        sys.exit(1)

    # 2. Calcular
    benchmarks = compute(rows, min_obs=args.min_obs)
    if not benchmarks:
        log.warning("No se generaron benchmarks.")
        sys.exit(0)

    # 3. Upsert
    upsert_benchmarks(supabase, benchmarks)

    # 4. Verificación rápida
    result = supabase.table("precios_benchmark").select("id", count="exact").execute()
    log.info(f"\n✓ Benchmarks en Supabase: {result.count} códigos ONU")

    # Muestra los 5 más frecuentes (más datos = más confiable)
    sample = (
        supabase.table("precios_benchmark")
        .select("codigo_onu,descripcion_onu,precio_mediana,n_observaciones")
        .order("n_observaciones", desc=True)
        .limit(5)
        .execute()
    )
    log.info("\nTop 5 códigos con más datos:")
    for row in sample.data or []:
        log.info(
            f"  {row['codigo_onu']} — {str(row.get('descripcion_onu',''))[:40]} "
            f"| Mediana: ${row['precio_mediana']:,.0f} | N={row['n_observaciones']}"
        )

    log.info("\n=== Compute Benchmarks finalizado ===")


if __name__ == "__main__":
    main()
