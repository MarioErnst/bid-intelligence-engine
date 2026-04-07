"""
etl_sasf_batch.py — Procesa archivos Excel de SASF (12 meses) y carga en Supabase.

Para cada fila de cada Excel:
  1. Carga datos con DataLoader (reutiliza lógica existente)
  2. Consulta API Mercado Público para datos del ganador (con skip si ya existe)
  3. Calcula gaps de precio
  4. Descarga PDF y analiza con IA (si motivo != PRECIO y --skip-ai no está activo)
  5. Upsert en tabla ofertas_sasf

Convención de nombres para Excel:
    data/sasf_excels/2025-01_licitaciones.xlsx  (preferido)
    data/sasf_excels/07LicitacionesEne.xlsx     (legado, se infiere como 2025-01)

Uso:
    python scripts/etl_sasf_batch.py
    python scripts/etl_sasf_batch.py --rut 76.930.423-1 --skip-ai
    python scripts/etl_sasf_batch.py --excel-dir data/sasf_excels --resume
"""

import argparse
import glob
import logging
import os
import re
import sys
import time
from datetime import date
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.data_loader import DataLoader
from src.api.mercado_publico import MercadoPublicoAPI
from src.agents.pdf_downloader import PdfDownloader
from src.ai.vertex_client import VertexAIClient
from src.models.data_models import LicitacionPerdida
from src.db.supabase_client import get_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

MESES_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4,
    "may": 5, "jun": 6, "jul": 7, "ago": 8,
    "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def infer_mes_from_filename(path: str) -> str:
    """Devuelve string 'YYYY-MM' o 'desconocido'."""
    name = Path(path).stem.lower()

    m = re.search(r"(\d{4})-(\d{1,2})", name)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}"

    for abbr, num in MESES_ES.items():
        if abbr in name:
            mtime = os.path.getmtime(path)
            año = date.fromtimestamp(mtime).year
            return f"{año}-{num:02d}"

    return "desconocido"


def parse_date(fecha_str: str) -> Optional[str]:
    """Convierte string de fecha a ISO date string o None."""
    if not fecha_str:
        return None
    # Intenta extraer solo la parte YYYY-MM-DD
    m = re.search(r"(\d{4}-\d{2}-\d{2})", str(fecha_str))
    return m.group(1) if m else None


def compute_price_gap(licit: LicitacionPerdida) -> tuple[Optional[float], Optional[float], str]:
    """
    Replica la lógica de MOD2 sin depender de archivos JSON intermedios.
    Devuelve (gap_monetario, gap_porcentual, motivo).
    """
    p_cli = licit.precio_oferta_cliente
    p_gan = licit.precio_ganador

    if p_cli is None or p_gan is None:
        return None, None, "DESCONOCIDO"

    try:
        p_cli = float(p_cli)
        p_gan = float(p_gan)
    except (TypeError, ValueError):
        return None, None, "DESCONOCIDO"

    if p_gan <= 0:
        return None, None, "DESCONOCIDO"

    gap = p_cli - p_gan
    pct = (gap / p_gan) * 100
    motivo = "PRECIO" if p_gan < p_cli else "OTRO"
    return round(gap, 2), round(pct, 4), motivo


def already_processed(supabase, id_licitacion: str, codigo_onu: Optional[int], fecha: Optional[str]) -> bool:
    """Devuelve True si el registro ya existe con precio_ganador no nulo."""
    codigo_onu_val = codigo_onu if codigo_onu is not None else 0
    fecha_val = fecha or "1900-01-01"
    result = (
        supabase.table("ofertas_sasf")
        .select("id,precio_ganador")
        .eq("id_licitacion", id_licitacion)
        .eq("codigo_onu", codigo_onu_val)
        .eq("fecha_adjudicacion", fecha_val)
        .limit(1)
        .execute()
    )
    if result.data:
        row = result.data[0]
        return row.get("precio_ganador") is not None
    return False


def build_row(licit: LicitacionPerdida, motivo: str, mes_proceso: str, fuente: str) -> dict:
    """Mapea LicitacionPerdida al schema de ofertas_sasf."""
    fecha = parse_date(licit.fecha_licitacion)
    return {
        "id_licitacion":      licit.id_licitacion,
        "codigo_onu":         licit.codigo_producto_onu if licit.codigo_producto_onu is not None else 0,
        "fecha_adjudicacion": fecha or "1900-01-01",
        "rut_proveedor_sasf": licit.rut_cliente,
        "nombre_item":        licit.producto_cliente,
        "monto_neto_oferta":  licit.precio_oferta_cliente,
        "cantidad_oferta":    licit.cantidad,
        "precio_ganador":     licit.precio_ganador,
        "proveedor_ganador":  licit.proveedor_ganador,
        "rut_ganador":        licit.rut_ganador,
        "estado_licitacion":  licit.estado_licitacion,
        "url_acta":           licit.url_acta,
        "gap_monetario":      licit.gap_monetario,
        "gap_porcentual":     licit.gap_porcentual,
        "motivo_perdida":     motivo,
        "analisis_ai":        licit.analisis_ai,
        "path_pdf_informe":   licit.path_pdf_informe,
        "mes_proceso":        mes_proceso,
        "fuente_excel":       fuente,
    }


def upsert_row(supabase, row: dict):
    supabase.table("ofertas_sasf").upsert(
        row,
        on_conflict="id_licitacion,codigo_onu,fecha_adjudicacion",
    ).execute()


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process_excel(
    excel_path: str,
    api: MercadoPublicoAPI,
    supabase,
    vertex: Optional[VertexAIClient],
    pdf_dl: Optional[PdfDownloader],
    rut_filter: Optional[str],
    resume: bool,
):
    mes_proceso = infer_mes_from_filename(excel_path)
    fuente = Path(excel_path).name
    log.info(f"\n{'='*60}")
    log.info(f"Archivo: {fuente}  |  Mes: {mes_proceso}")
    log.info(f"{'='*60}")

    licitaciones = DataLoader.cargar_datos_excel(excel_path)
    if not licitaciones:
        log.warning(f"  Sin datos en {fuente}")
        return

    if rut_filter:
        licitaciones = [l for l in licitaciones if l.rut_cliente == rut_filter]
        log.info(f"  Filtradas por RUT {rut_filter}: {len(licitaciones)} filas")

    procesadas = ganadas = perdidas_precio = perdidas_otro = errores = 0

    for i, licit in enumerate(licitaciones, 1):
        log.info(f"  [{i}/{len(licitaciones)}] {licit.id_licitacion} — {licit.producto_cliente[:50]}")

        fecha = parse_date(licit.fecha_licitacion)
        codigo_onu = licit.codigo_producto_onu

        # Skip si ya procesado y --resume activo
        if resume and already_processed(supabase, licit.id_licitacion, codigo_onu, fecha):
            log.info("    → Ya procesado, saltando")
            continue

        # PASO 1: API Mercado Público
        try:
            api_data = api.consultar_licitacion(licit.id_licitacion)
            if api_data:
                datos_ganador = api.extraer_datos_ganador(
                    api_data,
                    licit.producto_cliente,
                    codigo_producto_onu=codigo_onu,
                )
                if datos_ganador:
                    licit.precio_ganador     = datos_ganador.get("precio_ganador")
                    licit.proveedor_ganador  = datos_ganador.get("proveedor_ganador")
                    licit.rut_ganador        = datos_ganador.get("rut_ganador")
                    licit.estado_licitacion  = datos_ganador.get("estado_licitacion")
                    licit.url_acta           = datos_ganador.get("url_acta")
            else:
                log.warning("    ⚠️ Sin datos API")
        except Exception as e:
            log.error(f"    ❌ Error API: {e}")
            errores += 1

        # PASO 2: Gaps de precio
        gap, pct, motivo = compute_price_gap(licit)
        licit.gap_monetario  = gap
        licit.gap_porcentual = pct

        if motivo == "PRECIO":
            perdidas_precio += 1
        elif motivo == "OTRO":
            perdidas_otro += 1

        # PASO 3: PDF + IA (solo para pérdidas no-precio)
        if vertex and pdf_dl and motivo == "OTRO" and licit.url_acta:
            try:
                pdf_path = pdf_dl.download_informe(licit.url_acta, licit.id_licitacion)
                if pdf_path:
                    licit.path_pdf_informe = pdf_path
                    licit.analisis_ai = vertex.analizar_derrota(licit, pdf_path=pdf_path)
                    log.info("    ✓ PDF descargado y analizado con IA")
            except Exception as e:
                log.warning(f"    ⚠️ Error PDF/IA: {e}")

        # PASO 4: Upsert Supabase
        row = build_row(licit, motivo, mes_proceso, fuente)
        try:
            upsert_row(supabase, row)
            procesadas += 1
        except Exception as e:
            log.error(f"    ❌ Error upsert: {e}")
            errores += 1

        # Pequeña pausa para no saturar la API
        time.sleep(0.3)

    log.info(
        f"\n  Resumen {fuente}: {procesadas} procesadas | "
        f"PRECIO: {perdidas_precio} | OTRO: {perdidas_otro} | Errores: {errores}"
    )


def main():
    parser = argparse.ArgumentParser(description="ETL batch de archivos Excel SASF → Supabase")
    parser.add_argument("--excel-dir", default="data/sasf_excels",
                        help="Carpeta con archivos Excel (default: data/sasf_excels)")
    parser.add_argument("--rut", default=None,
                        help="Filtrar solo este RUT de proveedor (ej: 76.930.423-1)")
    parser.add_argument("--skip-ai", action="store_true",
                        help="Omitir descarga de PDF y análisis IA (más rápido)")
    parser.add_argument("--resume", action="store_true",
                        help="Saltar registros que ya tienen precio_ganador en Supabase")
    args = parser.parse_args()

    load_dotenv()
    supabase = get_client()
    api = MercadoPublicoAPI()
    vertex = None if args.skip_ai else VertexAIClient()
    pdf_dl = None if args.skip_ai else PdfDownloader()

    excel_files = sorted(glob.glob(f"{args.excel_dir}/*.xlsx"))
    if not excel_files:
        log.error(f"No hay archivos .xlsx en '{args.excel_dir}/'")
        log.error("Coloca los Excel mensuales de SASF en esa carpeta.")
        sys.exit(1)

    log.info(f"=== ETL SASF Batch — {len(excel_files)} archivos ===")
    if args.skip_ai:
        log.info("Modo: sin análisis IA (--skip-ai activo)")
    if args.resume:
        log.info("Modo: --resume activo (saltará registros ya procesados)")

    for excel_path in excel_files:
        process_excel(
            excel_path=excel_path,
            api=api,
            supabase=supabase,
            vertex=vertex,
            pdf_dl=pdf_dl,
            rut_filter=args.rut,
            resume=args.resume,
        )

    if pdf_dl:
        try:
            pdf_dl.close()
        except Exception:
            pass

    log.info("\n=== ETL SASF completado ===")


if __name__ == "__main__":
    main()
