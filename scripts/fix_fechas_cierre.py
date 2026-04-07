"""
fix_fechas_cierre.py — Corrige fecha_cierre NULL en licitaciones_abiertas.

Estrategia: endpoint de DETALLE por código (no el de listado).
El detalle funciona para cualquier licitación sin importar su estado (abierta/cerrada).

Flujo:
  1. Lee todos los codigo_licitacion con fecha_cierre IS NULL desde Supabase
  2. Llama al endpoint de detalle una vez por código
  3. Extrae FechaCierre y actualiza el registro

Tiempo estimado: ~2 min para 359 códigos (0.35s de delay entre calls).

Uso:
    python3 scripts/fix_fechas_cierre.py
    python3 scripts/fix_fechas_cierre.py --delay 0.5    # más lento si la API da errores
    python3 scripts/fix_fechas_cierre.py --dry-run      # sin escribir a DB
    python3 scripts/fix_fechas_cierre.py --batch 50     # detener tras N actualizaciones
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

BASE_URL  = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"
DEFAULT_DELAY = 0.35


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


def fetch_detail(
    session: requests.Session,
    ticket: str,
    codigo: str,
    delay: float,
    retries: int = 4,
) -> Optional[dict]:
    """Llama al endpoint de detalle para un código específico."""
    params = {"codigo": codigo, "ticket": ticket}
    for attempt in range(1, retries + 1):
        try:
            r = session.get(BASE_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and data.get("Codigo") == 10500:
                wait = min(delay * (2 ** attempt), 60)
                log.warning(f"  API saturada. Esperando {wait:.1f}s...")
                time.sleep(wait)
                continue
            return data
        except Exception as e:
            if attempt == retries:
                log.warning(f"  Fallo en {codigo}: {e}")
                return None
            time.sleep(delay * attempt)
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Corrige fecha_cierre NULL en licitaciones_abiertas vía endpoint de detalle"
    )
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                        help=f"Segundos entre calls API (default: {DEFAULT_DELAY})")
    parser.add_argument("--batch", type=int, default=None,
                        help="Detener tras N actualizaciones (útil para tests)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo muestra qué haría, sin escribir a DB")
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

    codigos = [row["codigo_licitacion"] for row in null_rows]
    total   = len(codigos)
    log.info(f"  {total} licitaciones con fecha_cierre NULL")
    log.info(f"  Tiempo estimado: ~{total * args.delay / 60:.1f} min a {args.delay}s por call\n")

    # 2. Llamar endpoint de detalle por cada código
    session = requests.Session()
    session.headers.update({"User-Agent": "BidEngine/1.0 fix_fechas", "Connection": "close"})

    ok = 0
    sin_fecha = 0
    errores = 0
    limit = args.batch or total

    for i, codigo in enumerate(codigos, 1):
        if ok >= limit:
            log.info(f"  Límite --batch {limit} alcanzado. Deteniendo.")
            break

        if i % 25 == 0 or i == 1:
            log.info(f"  [{i}/{total}] Procesando... ({ok} actualizados, {sin_fecha} sin fecha)")

        time.sleep(args.delay)
        data = fetch_detail(session, ticket, codigo, args.delay)

        if not data:
            errores += 1
            continue

        listado = data.get("Listado") or []
        if not listado:
            sin_fecha += 1
            continue

        fecha = parse_date(listado[0].get("FechaCierre"))
        if not fecha:
            sin_fecha += 1
            continue

        if args.dry_run:
            print(f"  [DRY-RUN] {codigo} → {fecha}")
            ok += 1
            continue

        try:
            supabase.table("licitaciones_abiertas").update(
                {"fecha_cierre": fecha}
            ).eq("codigo_licitacion", codigo).execute()
            ok += 1
        except Exception as e:
            log.error(f"  ❌ Error actualizando {codigo}: {e}")
            errores += 1

    # 3. Resumen
    print(f"\n{'═'*60}")
    print(f"  fix_fechas_cierre — RESUMEN")
    print(f"{'═'*60}")
    print(f"  Códigos procesados : {min(i, total)}/{total}")
    print(f"  ✅ Actualizados    : {ok}")
    print(f"  ⚠  Sin FechaCierre : {sin_fecha}  (licitación sin fecha en API)")
    if errores:
        print(f"  ❌ Errores DB      : {errores}")
    if sin_fecha > 0:
        print(f"\n  Nota: {sin_fecha} licitaciones no tienen FechaCierre en la API.")
        print(f"  Probablemente son licitaciones sin plazo definido (trato directo,")
        print(f"  convenio marco, etc.). Es esperable.")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    main()
