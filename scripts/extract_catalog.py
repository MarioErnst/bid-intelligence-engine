"""
extract_catalog.py — Extrae catálogo de productos SASF desde archivos Excel.

Lee todos los .xlsx en data/sasf_excels/, extrae pares únicos
(CodigoProductoONU, ONUProducto) y los upsertea en la tabla productos_sasf.

Uso:
    python scripts/extract_catalog.py [--excel-dir data/sasf_excels]

Convención de nombres recomendada para los Excel:
    2025-01_licitaciones.xlsx, 2025-02_licitaciones.xlsx, ...
    El archivo original 07LicitacionesEne.xlsx se infiere como 2025-01.
"""

import argparse
import glob
import logging
import re
import sys
import os
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Agrega el root del proyecto al path para imports relativos
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.db.supabase_client import get_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Mapeo de abreviaciones de meses en español a número
MESES_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4,
    "may": 5, "jun": 6, "jul": 7, "ago": 8,
    "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}


def infer_date_from_filename(path: str) -> date | None:
    """
    Intenta inferir la fecha (mes) del nombre del archivo.

    Patrones soportados:
      - 2025-01_licitaciones.xlsx  → 2025-01-01
      - 07LicitacionesEne.xlsx     → 2025-01-01 (Ene = enero, año desde mtime)
    """
    name = Path(path).stem.lower()

    # Patrón preferido: YYYY-MM al inicio
    m = re.search(r"(\d{4})-(\d{1,2})", name)
    if m:
        return date(int(m.group(1)), int(m.group(2)), 1)

    # Fallback: busca abreviación de mes en español
    for abbr, num in MESES_ES.items():
        if abbr in name:
            # Infiere año desde la fecha de modificación del archivo
            mtime = os.path.getmtime(path)
            año = date.fromtimestamp(mtime).year
            return date(año, num, 1)

    log.warning(f"No se pudo inferir fecha de '{Path(path).name}'. primera/ultima_vez_visto será null.")
    return None


def extract_catalog(excel_dir: str = "data/sasf_excels") -> list[dict]:
    """Lee todos los Excel y devuelve lista de filas para productos_sasf."""
    excel_files = sorted(glob.glob(f"{excel_dir}/*.xlsx"))
    if not excel_files:
        log.error(f"No hay archivos .xlsx en '{excel_dir}/'")
        sys.exit(1)

    log.info(f"Archivos encontrados: {len(excel_files)}")

    # catalog: (codigo_onu, nombre_producto) -> {primera_vez, ultima_vez, fuente}
    catalog: dict[tuple, dict] = {}

    for excel_path in excel_files:
        log.info(f"  Procesando {Path(excel_path).name}...")
        try:
            df = pd.read_excel(
                excel_path,
                usecols=lambda c: c in ("CodigoProductoONU", "ONUProducto"),
                dtype=str,
            )
        except Exception as e:
            log.warning(f"  No se pudo leer {excel_path}: {e}")
            continue

        if "CodigoProductoONU" not in df.columns:
            log.warning(f"  Columna 'CodigoProductoONU' no encontrada en {excel_path}")
            continue

        df["CodigoProductoONU"] = pd.to_numeric(df["CodigoProductoONU"], errors="coerce")
        df = df.dropna(subset=["CodigoProductoONU"])
        df["CodigoProductoONU"] = df["CodigoProductoONU"].astype(int)
        df["ONUProducto"] = df.get("ONUProducto", "").fillna("").astype(str).str.strip()
        df = df.drop_duplicates()

        mes_date = infer_date_from_filename(excel_path)
        fuente = Path(excel_path).name

        for _, row in df.iterrows():
            key = (int(row["CodigoProductoONU"]), row["ONUProducto"])
            if key not in catalog:
                catalog[key] = {
                    "primera_vez_visto": mes_date,
                    "ultima_vez_visto": mes_date,
                    "fuente_excel": fuente,
                }
            else:
                entry = catalog[key]
                if mes_date:
                    if entry["primera_vez_visto"] is None or mes_date < entry["primera_vez_visto"]:
                        entry["primera_vez_visto"] = mes_date
                    if entry["ultima_vez_visto"] is None or mes_date > entry["ultima_vez_visto"]:
                        entry["ultima_vez_visto"] = mes_date

    rows = [
        {
            "codigo_onu": k[0],
            "nombre_producto": k[1],
            "primera_vez_visto": v["primera_vez_visto"].isoformat() if v["primera_vez_visto"] else None,
            "ultima_vez_visto": v["ultima_vez_visto"].isoformat() if v["ultima_vez_visto"] else None,
            "fuente_excel": v["fuente_excel"],
        }
        for k, v in catalog.items()
    ]
    return rows


def upsert_catalog(rows: list[dict]):
    supabase = get_client()
    batch_size = 500
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        supabase.table("productos_sasf").upsert(
            batch,
            on_conflict="codigo_onu,nombre_producto",
        ).execute()
        total += len(batch)
        log.info(f"  Upserted {total}/{len(rows)} productos")


def main():
    parser = argparse.ArgumentParser(description="Extrae catálogo de productos SASF")
    parser.add_argument("--excel-dir", default="data/sasf_excels", help="Carpeta con archivos Excel")
    args = parser.parse_args()

    load_dotenv()

    log.info("=== Extracción de catálogo SASF ===")
    rows = extract_catalog(args.excel_dir)
    log.info(f"Pares únicos (codigo_onu, nombre_producto) encontrados: {len(rows)}")

    if rows:
        upsert_catalog(rows)
        log.info("Catálogo guardado en Supabase tabla 'productos_sasf'.")

    # Verificación rápida
    supabase = get_client()
    result = supabase.table("productos_sasf").select("id", count="exact").execute()
    log.info(f"Total en tabla productos_sasf: {result.count} filas")


if __name__ == "__main__":
    main()
