"""
fetch_open_licitaciones.py — Descarga licitaciones abiertas desde la API de
ChileCompra y las almacena en Supabase (tabla licitaciones_abiertas).

Para cada licitación:
  1. Llama al endpoint de lista (estado=publicada) para obtener el inventario
  2. (Opcional) Pre-filtra por keywords en el nombre antes de llamar detalle
  3. Llama al endpoint de detalle por código para obtener los ítems con ONU codes
  4. Upserta en licitaciones_abiertas con todos los campos necesarios para el scoring

Uso:
    python3 scripts/fetch_open_licitaciones.py
    python3 scripts/fetch_open_licitaciones.py --pages 5      # primeras 5000 licitaciones
    python3 scripts/fetch_open_licitaciones.py --force        # re-fetcha las ya procesadas
    python3 scripts/fetch_open_licitaciones.py --pages 2 --delay 0.5
    python3 scripts/fetch_open_licitaciones.py --keywords     # pre-filtra por nombre (mucho más rápido)
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests
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

BASE_URL = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"
BATCH_SIZE = 200
DEFAULT_PAGES = 3        # 3000 licitaciones en primera corrida
DEFAULT_DELAY = 0.35     # segundos entre llamadas a la API
ESTADO_API = "activas"   # "activas" incluye publicadas + en evaluación con plazo abierto

# Palabras clave para pre-filtrar por nombre de licitación (rubro médico/salud)
# Permite un primer filtrado rápido sin llamar el endpoint de detalle.
KEYWORDS_SALUD = [
    "medic", "salud", "hospital", "clinic", "farmac", "farmác",
    "insumo", "quirur", "ortop", "prótesis", "protesi", "dental",
    "sanitari", "equipo médic", "material médic", "dispositiv",
    "laboratori", "diagnóst", "tratamiento", "rehabilit",
    "oxígeno", "oxigeno", "ambulanci", "urgenci", "enferm",
    "instrumental", "implante", "catéter", "cateter", "jeringa",
    "sonda", "guante", "mascarill", "apósito", "venda",
]


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _get(session: requests.Session, params: dict, ticket: str,
         retries: int = 15, delay: float = DEFAULT_DELAY) -> Optional[dict]:
    """
    GET a la API de ChileCompra con retry inteligente.

    La API devuelve HTTP 500 intermitente aunque esté disponible — se resuelve
    reintentando rápido (1s). Solo hace backoff en error 10500 (throttle real).
    """
    params["ticket"] = ticket
    for attempt in range(1, retries + 1):
        try:
            r = session.get(BASE_URL, params=params, timeout=45)

            # 500 intermitente: reintentar rápido sin backoff
            if r.status_code == 500:
                log.debug(f"  HTTP 500 (intento {attempt}/{retries}), reintentando en 1s...")
                time.sleep(1)
                continue

            r.raise_for_status()
            data = r.json()

            # Código 10500 = throttle real de la API
            if isinstance(data, dict) and data.get("Codigo") == 10500:
                wait = min(2 * attempt, 30)
                log.warning(f"  API throttle (10500). Esperando {wait}s...")
                time.sleep(wait)
                continue

            return data

        except requests.exceptions.Timeout:
            log.warning(f"  Timeout (intento {attempt}/{retries}), reintentando...")
            time.sleep(2)
            continue
        except Exception as e:
            if attempt == retries:
                log.error(f"  ❌ Fallo tras {retries} intentos: {e}")
                return None
            time.sleep(1)
    log.error(f"  ❌ Agotados {retries} reintentos")
    return None


def fetch_page(session, ticket: str, page: int) -> Optional[dict]:
    """Trae una página del listado de licitaciones activas."""
    return _get(session, {"estado": ESTADO_API, "pagina": page}, ticket)


def fetch_detail(session, ticket: str, codigo: str) -> Optional[dict]:
    """Trae el detalle completo de una licitación, incluyendo ítems y ONU codes."""
    return _get(session, {"codigo": codigo}, ticket)


# ---------------------------------------------------------------------------
# Parseo
# ---------------------------------------------------------------------------

def parse_date(s: Optional[str]) -> Optional[str]:
    """Convierte fechas del formato DD/MM/AAAA (con o sin hora) o ISO a YYYY-MM-DD."""
    if not s:
        return None
    for fmt in (
        "%d/%m/%Y %H:%M:%S",   # ChileCompra: "07/04/2026 17:00:00"
        "%d/%m/%Y",             # Solo fecha
        "%Y-%m-%dT%H:%M:%S",   # ISO con T
        "%Y-%m-%d",             # ISO solo fecha
    ):
        try:
            return datetime.strptime(s.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_licitacion_detail(raw: dict) -> Optional[dict]:
    """
    Extrae campos relevantes de la respuesta de detalle de la API.
    Devuelve el dict listo para upsert en licitaciones_abiertas.
    """
    listado = raw.get("Listado") or []
    if not listado:
        return None
    L = listado[0]

    organismo = L.get("Organismo") or {}
    items_raw = (L.get("Items") or {}).get("Listado") or []

    # Parsear ítems y detectar ONU codes
    items = []
    n_unspsc42 = 0
    for item in items_raw:
        try:
            onu = int(item.get("CodigoProducto") or 0)
        except (ValueError, TypeError):
            onu = None

        is42 = bool(onu and UNSPSC42_MIN <= onu <= UNSPSC42_MAX)
        if is42:
            n_unspsc42 += 1

        try:
            cantidad = float(item.get("Cantidad") or 0)
        except (ValueError, TypeError):
            cantidad = None

        items.append({
            "correlativo":  item.get("Correlativo"),
            "codigo_onu":   onu,
            "nombre":       (item.get("Nombre") or "")[:300],
            "cantidad":     cantidad,
            "unidad":       item.get("UnidadMedida"),
            "es_unspsc42":  is42,
        })

    try:
        monto = float(L.get("MontoEstimado") or 0) or None
    except (ValueError, TypeError):
        monto = None

    return {
        "codigo_licitacion":  L.get("CodigoExterno"),
        "nombre_licitacion":  (L.get("Nombre") or "")[:500],
        "tipo":               L.get("Tipo"),
        "estado":             L.get("Estado"),
        "fecha_publicacion":  parse_date(L.get("FechaPublicacion")),
        "fecha_cierre":       parse_date(L.get("FechaCierre")),
        "monto_estimado":     monto,
        "nombre_organismo":   (organismo.get("Nombre") or "")[:300],
        "rut_unidad":         organismo.get("RutUnidad"),
        "region":             L.get("RegionUnidad") or organismo.get("Region"),
        "sector":             L.get("Sector") or organismo.get("Sector"),
        "n_items_total":      len(items),
        "n_items_unspsc42":   n_unspsc42,
        "items":              items,
        "updated_at":         datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def upsert_batch(supabase, rows: list[dict]):
    """Upserta en licitaciones_abiertas."""
    safe_upsert(supabase, "licitaciones_abiertas", rows,
                on_conflict="codigo_licitacion",
                batch_size=BATCH_SIZE)


def already_fetched_today(supabase, codigo: str) -> bool:
    """True si ya fue fetcheada hoy (evita llamadas innecesarias)."""
    today = date.today().isoformat()
    r = (
        supabase.table("licitaciones_abiertas")
        .select("fetched_at")
        .eq("codigo_licitacion", codigo)
        .gte("updated_at", today)
        .limit(1)
        .execute()
    )
    return bool(r.data)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch licitaciones abiertas de ChileCompra → Supabase"
    )
    parser.add_argument("--pages", type=int, default=DEFAULT_PAGES,
                        help=f"Páginas a procesar (1000 por página). Default: {DEFAULT_PAGES}")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                        help=f"Segundos entre llamadas API. Default: {DEFAULT_DELAY}")
    parser.add_argument("--force", action="store_true",
                        help="Re-fetcha licitaciones ya procesadas hoy")
    parser.add_argument("--solo-unspsc42", action="store_true",
                        help="Solo guarda licitaciones con al menos 1 ítem UNSPSC 42")
    parser.add_argument("--keywords", action="store_true",
                        help="Pre-filtra por nombre antes de llamar detalle (mucho más rápido, "
                             "puede perder licitaciones con nombres poco descriptivos)")
    args = parser.parse_args()

    load_dotenv()
    ticket = os.getenv("MERCADO_PUBLICO_API_KEY")
    if not ticket:
        log.error("❌ MERCADO_PUBLICO_API_KEY no configurada en .env")
        sys.exit(1)

    supabase = get_client()
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 BidEngine/1.0",
        "Connection": "close",
    })

    log.info(f"=== Fetch Licitaciones Abiertas — {args.pages} páginas ===")

    total_fetched = 0
    total_saved = 0
    batch = []

    for page in range(1, args.pages + 1):
        log.info(f"\n[Página {page}/{args.pages}] Obteniendo listado...")
        page_data = fetch_page(session, ticket, page)

        if not page_data:
            log.warning(f"  Sin datos en página {page}. Terminando.")
            break

        listado = page_data.get("Listado") or []
        if not listado:
            log.info(f"  Página {page} vacía. No hay más licitaciones publicadas.")
            break

        log.info(f"  {len(listado)} licitaciones en página {page}")
        skipped = 0

        for lic in listado:
            codigo = lic.get("CodigoExterno")
            if not codigo:
                continue

            total_fetched += 1

            # Pre-filtro por keywords en nombre (evita calls de detalle innecesarios)
            if args.keywords:
                nombre_lower = (lic.get("Nombre") or "").lower()
                if not any(kw in nombre_lower for kw in KEYWORDS_SALUD):
                    continue

            # Idempotencia: saltar si ya fue procesada hoy
            if not args.force and already_fetched_today(supabase, codigo):
                skipped += 1
                continue

            time.sleep(args.delay)

            detail_raw = fetch_detail(session, ticket, codigo)
            if not detail_raw:
                continue

            parsed = parse_licitacion_detail(detail_raw)
            if not parsed:
                continue

            # Filtro opcional: solo UNSPSC 42
            if args.solo_unspsc42 and parsed["n_items_unspsc42"] == 0:
                continue

            batch.append(parsed)

            # Flush batch
            if len(batch) >= BATCH_SIZE:
                upsert_batch(supabase, batch)
                total_saved += len(batch)
                batch = []

        if skipped:
            log.info(f"  {skipped} ya procesadas hoy (usa --force para re-fetchear)")

    # Flush final
    if batch:
        upsert_batch(supabase, batch)
        total_saved += len(batch)

    log.info(f"\n=== Fetch finalizado ===")
    log.info(f"  Licitaciones revisadas : {total_fetched}")
    log.info(f"  Guardadas en Supabase  : {total_saved}")
    log.info(f"\n→ Siguiente paso: python3 scripts/compute_match_scores.py")


if __name__ == "__main__":
    main()
