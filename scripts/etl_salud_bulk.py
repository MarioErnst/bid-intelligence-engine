"""
etl_salud_bulk.py — ETL para archivos 7z de licitaciones por sector (ChileCompra).

Descarga o lee un archivo .7z de licitaciones del sector Salud y carga los datos
en licitaciones_mercado (solo ofertas ganadoras = adjudicadas).

Ventaja vs etl_mercado_bulk.py:
  • Archivos ~50-100x más pequeños (solo sector Salud, no todo el mercado)
  • Solo necesitas descargar 2 archivos por año en vez de 12 ZIPs mensuales

Formato de los archivos:
  URL:  https://chc-lic-files.mercadopublico.cl/sector/{año}/Sem{n}/Salud.7z
  CSV:  07Licitaciones{Mes}.csv  (sep=';', encoding=latin1)
  Fila: una por oferta (ganadora o perdedora); filtrar ResultadoOferta=='Ganadora'

Uso:
    # Con archivo local (descargado a mano)
    python3 scripts/etl_salud_bulk.py --file ~/Downloads/Salud.7z   --year 2026
    python3 scripts/etl_salud_bulk.py --file ~/Downloads/Salud-2.7z --year 2025

    # Auto-descarga desde ChileCompra
    python3 scripts/etl_salud_bulk.py --download --year 2025 --sem 2
    python3 scripts/etl_salud_bulk.py --download --year 2026 --sem 1

    # Flags extra
    --dry-run       solo cuenta filas, no upserta
    --solo-unspsc42 filtra solo productos UNSPSC 42xxxxxx (equipo médico)
    --force         re-procesa meses ya cargados
"""

import argparse
import logging
import math
import sys
import tempfile
from pathlib import Path
from typing import Optional

import polars as pl
import py7zr
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.db.supabase_client import get_client, safe_upsert
from src.core.config import UNSPSC42_MIN, UNSPSC42_MAX

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://chc-lic-files.mercadopublico.cl/sector"
BATCH_SIZE = 500

# Mapping nombre de mes en filename → número de mes
MES_MAP = {
    "ene": "01", "feb": "02", "mar": "03", "abr": "04",
    "may": "05", "jun": "06", "jul": "07", "ago": "08",
    "sep": "09", "oct": "10", "nov": "11", "dic": "12",
}

# Columnas del CSV sector → columnas de licitaciones_mercado
COLUMN_MAP = {
    "NroLicitacion":     "codigo_licitacion",
    "NombreLicitacion":  "nombre_licitacion",
    "EstadoLicitacion":  "estado",
    "TipoLicitacion":    "tipo_licitacion",
    "FechaPublicacion":  "fecha_publicacion",
    "FechaCierre":       "fecha_cierre",
    "FechaAdjudicacion": "fecha_adjudicacion",
    "NombreItem":        "nombre_item",
    "CodigoProductoONU": "codigo_onu",
    "ONUProducto":       "descripcion_onu",
    "CantidadItem":      "cantidad",
    "UnidadMedida":      "unidad_medida",
    # MontoNetoOferta = precio unitario (MontoTotalOferta = cantidad × precio)
    "MontoNetoOferta":   "precio_unitario_ganador",
    "MontoTotalOferta":  "monto_total_adjudicado",
    "ProveedorRUT":      "rut_ganador",
    "Proveedor":         "nombre_ganador",
    "UnidadCompraRUT":   "rut_unidad_compradora",
    "UnidadCompra":      "nombre_unidad_compradora",
    "Sector":            "sector",
}

DATE_COLS = {"fecha_publicacion", "fecha_cierre", "fecha_adjudicacion"}


# ---------------------------------------------------------------------------
# Descarga automática
# ---------------------------------------------------------------------------

def download_7z(year: int, sem: int, sector: str = "Salud") -> bytes:
    """Descarga el archivo 7z del sector desde ChileCompra."""
    import httpx
    url = f"{BASE_URL}/{year}/Sem{sem}/{sector}.7z"
    log.info(f"Descargando: {url}")
    with httpx.Client(timeout=300, follow_redirects=True) as client:
        r = client.get(url)
        r.raise_for_status()
    log.info(f"  Descargado: {len(r.content) / 1_048_576:.1f} MB")
    return r.content


# ---------------------------------------------------------------------------
# Parseo CSV
# ---------------------------------------------------------------------------

def mes_from_filename(name: str) -> Optional[str]:
    """
    '07LicitacionesEne.csv' → '01'
    Devuelve el número de mes (2 dígitos) o None si no reconoce el nombre.
    """
    stem = Path(name).stem.lower()  # '07licitacionesene'
    for mes_nombre, mes_num in MES_MAP.items():
        if stem.endswith(mes_nombre):
            return mes_num
    return None


def normalize_rut(s: Optional[str]) -> Optional[str]:
    """Elimina puntos del RUT: '76.930.423-1' → '76930423-1'"""
    if not s:
        return s
    return s.replace(".", "")


def parse_date_col(series: pl.Series) -> pl.Series:
    """
    Intenta parsear columna fecha en varios formatos ChileCompra a ISO YYYY-MM-DD.
    Devuelve strings o null.
    """
    # Formatos posibles: "06-03-2025 8:59:40", "06/03/2025", "2025-03-06"
    for fmt in ("%d-%m-%Y %H:%M:%S", "%d-%m-%Y", "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = series.str.to_date(fmt, strict=False)
            if parsed.null_count() < len(parsed):
                return parsed.cast(pl.Utf8)
        except Exception:
            continue
    return pl.Series([None] * len(series), dtype=pl.Utf8)


def process_csv(csv_path: Path, year: int, mes_num: str,
                solo_unspsc42: bool) -> list[dict]:
    """Lee un CSV sector, filtra ganadoras, transforma y devuelve lista de dicts."""
    mes_proceso = f"{year}-{mes_num}"
    log.info(f"  Procesando {csv_path.name} → mes_proceso={mes_proceso}")

    # Leer todas las columnas como string para manejar decimales con coma española
    df = pl.read_csv(
        csv_path,
        separator=";",
        encoding="latin1",
        infer_schema_length=0,  # todo como Utf8
        truncate_ragged_lines=True,
        null_values=["", "N/A", "-"],
    )
    log.info(f"    Filas totales: {len(df)}")

    # Filtrar solo ofertas ganadoras
    if "ResultadoOferta" not in df.columns:
        log.warning(f"    Columna 'ResultadoOferta' no encontrada. Skipping.")
        return []

    df = df.filter(pl.col("ResultadoOferta") == "Ganadora")
    log.info(f"    Filas ganadoras: {len(df)}")

    if len(df) == 0:
        return []

    # Filtro UNSPSC 42 (opcional)
    if solo_unspsc42:
        df = df.with_columns(
            pl.col("CodigoProductoONU").cast(pl.Int64, strict=False).alias("_onu_int")
        ).filter(
            pl.col("_onu_int").is_between(UNSPSC42_MIN, UNSPSC42_MAX)
        ).drop("_onu_int")
        log.info(f"    Filas UNSPSC42 ganadoras: {len(df)}")
        if len(df) == 0:
            return []

    # Renombrar columnas
    rename_map = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(rename_map)

    # Generar numero_item = rank dentro de cada licitación (para unique constraint)
    df = df.with_columns(
        pl.int_range(pl.len(), dtype=pl.Int32).over("codigo_licitacion").alias("numero_item")
    )

    # Parsear fechas
    for col in DATE_COLS:
        if col in df.columns:
            df = df.with_columns(
                parse_date_col(df[col].cast(pl.Utf8)).alias(col)
            )

    # Castear tipos numéricos (normalizar coma decimal española → punto)
    for num_col in ("precio_unitario_ganador", "monto_total_adjudicado", "cantidad"):
        if num_col in df.columns:
            df = df.with_columns(
                pl.col(num_col)
                .str.replace(",", ".", literal=True)
                .cast(pl.Float64, strict=False)
            )

    for int_col in ("codigo_onu",):
        if int_col in df.columns:
            df = df.with_columns(
                pl.col(int_col)
                .str.replace(",", ".", literal=True)
                .cast(pl.Int64, strict=False)
            )

    # Añadir columnas de control
    df = df.with_columns([
        pl.lit(mes_proceso).alias("mes_proceso"),
        pl.lit(csv_path.name).alias("fuente_zip"),
    ])

    # Seleccionar solo columnas relevantes para licitaciones_mercado
    keep = [
        "codigo_licitacion", "nombre_licitacion", "estado", "tipo_licitacion",
        "fecha_publicacion", "fecha_cierre", "fecha_adjudicacion",
        "numero_item", "nombre_item", "codigo_onu", "descripcion_onu",
        "cantidad", "unidad_medida",
        "precio_unitario_ganador", "monto_total_adjudicado",
        "rut_ganador", "nombre_ganador",
        "rut_unidad_compradora", "nombre_unidad_compradora",
        "sector", "mes_proceso", "fuente_zip",
    ]
    df = df.select([c for c in keep if c in df.columns])

    # Limpiar NaN/Inf en floats
    rows = []
    for row in df.to_dicts():
        clean = {}
        for k, v in row.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                clean[k] = None
            else:
                clean[k] = v
        # Normalizar RUTs
        for rut_col in ("rut_ganador", "rut_unidad_compradora"):
            if rut_col in clean:
                clean[rut_col] = normalize_rut(clean[rut_col])
        rows.append(clean)

    log.info(f"    → {len(rows)} filas listas para upsert")
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ETL licitaciones sector Salud (7z) → licitaciones_mercado"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--file", type=Path,
                      help="Ruta al archivo .7z local (ej: ~/Downloads/Salud.7z)")
    mode.add_argument("--download", action="store_true",
                      help="Auto-descarga desde ChileCompra (requiere --year y --sem)")

    parser.add_argument("--year", type=int, required=True,
                        help="Año del archivo (ej: 2025, 2026)")
    parser.add_argument("--sem", type=int, choices=[1, 2],
                        help="Semestre (1 o 2) — requerido con --download")
    parser.add_argument("--sector", default="Salud",
                        help="Nombre del sector (default: Salud)")
    parser.add_argument("--solo-unspsc42", action="store_true",
                        help="Solo carga productos UNSPSC 42 (equipo médico)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo cuenta filas, no escribe a Supabase")
    parser.add_argument("--force", action="store_true",
                        help="Re-procesa meses que ya existen en la DB")
    args = parser.parse_args()

    if args.download and not args.sem:
        parser.error("--download requiere --sem (1 o 2)")

    load_dotenv()
    supabase = get_client() if not args.dry_run else None

    log.info(f"=== ETL Salud Bulk — año={args.year} ===")

    # Obtener bytes del 7z
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)

        if args.download:
            data = download_7z(args.year, args.sem, args.sector)
            z_path = tmp / f"{args.sector}-Sem{args.sem}-{args.year}.7z"
            z_path.write_bytes(data)
        else:
            z_path = args.file.expanduser().resolve()
            if not z_path.exists():
                log.error(f"❌ Archivo no encontrado: {z_path}")
                sys.exit(1)

        # Extraer todos los CSVs del 7z
        log.info(f"Extrayendo: {z_path.name}")
        with py7zr.SevenZipFile(z_path, "r") as z:
            csv_names = [n for n in z.getnames() if n.lower().endswith(".csv")]
            log.info(f"  CSVs encontrados: {csv_names}")
            z.extractall(path=tmp)

        # Procesar cada CSV
        total_rows = 0
        total_upserted = 0

        for csv_name in sorted(csv_names):
            mes_num = mes_from_filename(csv_name)
            if not mes_num:
                log.warning(f"  No pude extraer mes de '{csv_name}'. Skipping.")
                continue

            mes_proceso = f"{args.year}-{mes_num}"

            # Idempotencia: saltar si ya procesado (a menos que --force)
            if not args.force and not args.dry_run:
                r = (
                    supabase.table("licitaciones_mercado")
                    .select("id", count="exact")
                    .eq("mes_proceso", mes_proceso)
                    .eq("fuente_zip", csv_name)
                    .limit(1)
                    .execute()
                )
                if r.count and r.count > 0:
                    log.info(f"  {mes_proceso} ya cargado ({r.count} filas). Usa --force para re-procesar.")
                    continue

            rows = process_csv(tmp / csv_name, args.year, mes_num, args.solo_unspsc42)
            total_rows += len(rows)

            if args.dry_run:
                log.info(f"  [DRY-RUN] {mes_proceso}: {len(rows)} filas (no upsertadas)")
                continue

            if rows:
                upserted = safe_upsert(
                    supabase,
                    "licitaciones_mercado",
                    rows,
                    on_conflict="codigo_licitacion,numero_item,mes_proceso",
                    batch_size=BATCH_SIZE,
                )
                total_upserted += upserted

        # Resumen
        print(f"\n{'═'*60}")
        print(f"  ETL SALUD BULK — RESUMEN")
        print(f"{'═'*60}")
        print(f"  Año procesado         : {args.year}")
        print(f"  CSVs procesados       : {len(csv_names)}")
        print(f"  Filas ganadoras total : {total_rows}")
        if not args.dry_run:
            print(f"  Upsertadas en DB      : {total_upserted}")

            # Verificación rápida
            r = supabase.table("licitaciones_mercado").select("id", count="exact").execute()
            print(f"  Total en licitaciones_mercado: {r.count}")

            u42 = (
                supabase.table("licitaciones_mercado")
                .select("id", count="exact")
                .gte("codigo_onu", UNSPSC42_MIN)
                .lte("codigo_onu", UNSPSC42_MAX)
                .execute()
            )
            print(f"  De ellas UNSPSC 42    : {u42.count}")
        print(f"{'═'*60}\n")

        log.info("=== ETL Salud Bulk finalizado ===")


if __name__ == "__main__":
    main()
