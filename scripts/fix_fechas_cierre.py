"""
fix_fechas_cierre.py — Corrige fecha_cierre NULL en licitaciones_abiertas
usando el endpoint de LISTADO de la API (no el de detalle).

Estrategia eficiente:
  1. Carga todos los codigo_licitacion con fecha_cierre IS NULL desde Supabase
  2. Llama al endpoint de LISTADO (1 call por página, 1000 licitaciones por página)
  3. Extrae FechaCierre de la respuesta del listado
  4. Actualiza solo los registros con fecha_cierre NULL que aparecen en el listado

Esto tarda ~10 segundos (3-5 llamadas API) vs ~3 horas con --force completo.

Uso:
    python3 scripts/fix_fechas_cierre.py
    python3 scripts/fix_fechas_cierre.py --pages 5   # buscar en más páginas
    python3 scripts/fix_fechas_cierre.py --dry-run   # sin escribir a DB
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.db.supabase_client import get_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

BASE_URL = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"
DEFAULT_PAGES = 5
DELAY = 0.5   # segundos entre páginas (el listado es menos restrictivo)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_date(s: Optional[str]) -> Optional[str]:
    """Convierte fechas ChileCompra a YYYY-MM-DD."""
    if not s:
        return None
    for fmt in (
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(s.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return None


def fetch_page(session: requests.Session, ticket: str, page: int) -> Optional[dict]:
    """Trae una página del listado de licitaciones publicadas."""
    params = {"estado": "publicada", "pagina": page, "ticket": ticket}
    for attempt in range(1, 4):
        try:
            r = session.get(BASE_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and data.get("Codigo") == 10500:
                wait = min(DELAY * (2 ** attempt), 30)
                log.warning(f"  API saturada. Esperando {wait:.1f}s...")
                time.sleep(wait)
                continue
            return data
        except Exception as e:
            if attempt == 3:
                log.error(f"  Fallo en página {page}: {e}")
                return None
            time.sleep(DELAY * attempt)
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Corrige fecha_cierre NULL en licitaciones_abiertas (rápido)"
    )
    parser.add_argument("--pages", type=int, default=DEFAULT_PAGES,
                        help=f"Páginas de listado a consultar (default: {DEFAULT_PAGES})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo muestra qué se actualizaría, sin escribir a DB")
    args = parser.parse_args()

    load_dotenv()
    ticket = os.getenv("MERCADO_PUBLICO_API_KEY")
    if not ticket:
        log.error("❌ MERCADO_PUBLICO_API_KEY no configurada en .env")
        sys.exit(1)

    supabase = get_client()

    # 1. Cargar códigos con fecha_cierre NULL
    log.info("Cargando licitaciones con fecha_cierre NULL desde Supabase...")
    null_rows = []
    offset = 0
    while True:
        r = (
            supabase.table("licitaciones_abiertas")
            .select("codigo_licitacion")
            .is_("fecha_cierre", "null")
            .range(offset, offset + 999)
            .execute()
        )
        if not r.data:
            break
        null_rows.extend(r.data)
        if len(r.data) < 1000:
            break
        offset += 1000

    if not null_rows:
        log.info("✅ No hay licitaciones con fecha_cierre NULL. Nada que corregir.")
        return

    null_codigos = {row["codigo_licitacion"] for row in null_rows}
    log.info(f"  {len(null_codigos)} licitaciones con fecha_cierre NULL")

    # 2. Consultar páginas del listado para encontrar las fechas
    session = requests.Session()
    session.headers.update({"User-Agent": "BidEngine/1.0 fix_fechas"})

    updates = {}  # codigo → fecha_cierre ISO

    for page in range(1, args.pages + 1):
        log.info(f"  [Página {page}/{args.pages}] Consultando listado...")
        data = fetch_page(session, ticket, page)
        if not data:
            break

        listado = data.get("Listado") or []
        if not listado:
            log.info(f"  Página {page} vacía.")
            break

        for item in listado:
            codigo = item.get("CodigoExterno")
            if codigo and codigo in null_codigos:
                fecha = parse_date(item.get("FechaCierre"))
                if fecha:
                    updates[codigo] = fecha

        log.info(f"  Matches encontrados hasta ahora: {len(updates)}/{len(null_codigos)}")

        # Si ya encontramos todos, parar antes
        if len(updates) >= len(null_codigos):
            log.info("  Todos los NULL resueltos. Deteniendo búsqueda anticipada.")
            break

        time.sleep(DELAY)

    if not updates:
        log.warning(
            f"No se encontró ninguna fecha para los {len(null_codigos)} códigos. "
            f"Puede que las licitaciones ya estén cerradas (no aparecen en 'publicada'). "
            f"Prueba con --pages 10 o corre fetch_open_licitaciones.py --force."
        )
        return

    log.info(f"\n  {len(updates)} fechas encontradas de {len(null_codigos)} NULL")

    if args.dry_run:
        print(f"\n[DRY-RUN] Se actualizarían {len(updates)} registros:")
        for cod, fecha in list(updates.items())[:20]:
            print(f"  {cod} → {fecha}")
        if len(updates) > 20:
            print(f"  ... y {len(updates) - 20} más")
        return

    # 3. Actualizar en Supabase (UPDATE individual por codigo)
    log.info("Actualizando fecha_cierre en Supabase...")
    ok = 0
    errors = 0
    for codigo, fecha in updates.items():
        try:
            supabase.table("licitaciones_abiertas").update(
                {"fecha_cierre": fecha}
            ).eq("codigo_licitacion", codigo).execute()
            ok += 1
        except Exception as e:
            log.error(f"  ❌ Error actualizando {codigo}: {e}")
            errors += 1

    still_null = len(null_codigos) - len(updates)
    log.info(f"\n✅ Actualizados : {ok}")
    if errors:
        log.warning(f"  Errores       : {errors}")
    if still_null:
        log.warning(
            f"  Sin resolver  : {still_null} licitaciones "
            f"(probablemente ya cerradas y no aparecen en el listado 'publicada')"
        )

    log.info("=== fix_fechas_cierre finalizado ===")


if __name__ == "__main__":
    main()
