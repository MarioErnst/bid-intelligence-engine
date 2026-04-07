"""
cleanup_licitaciones.py — Limpieza del ciclo de vida de licitaciones_abiertas.

Borra de licitaciones_abiertas todas las licitaciones cuya fecha_cierre ya pasó.
Gracias a las FK con ON DELETE CASCADE, el borrado se propaga automáticamente a:
  • match_scores               (scores calculados para esa licitación)
  • pricing_recommendations    (precios calculados para esa licitación)

Ejecutar diariamente después de fetch_open_licitaciones.py.

Uso:
    python3 scripts/cleanup_licitaciones.py
    python3 scripts/cleanup_licitaciones.py --dry-run       # solo muestra, no borra
    python3 scripts/cleanup_licitaciones.py --dias-gracia 2 # tolerancia de 2 días
"""

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.db.supabase_client import get_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Borra licitaciones cerradas y sus scores/precios en cascada"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Muestra qué se borraría sin ejecutar")
    parser.add_argument("--dias-gracia", type=int, default=1,
                        help="Días de gracia tras fecha_cierre antes de borrar (default: 1)")
    args = parser.parse_args()

    load_dotenv()
    supabase = get_client()
    hoy = date.today()
    corte = (hoy - timedelta(days=args.dias_gracia)).isoformat()

    # --- Consultar qué se va a borrar ---
    log.info(f"Fecha de corte: {corte} (hoy={hoy}, gracia={args.dias_gracia}d)")

    r = (
        supabase.table("licitaciones_abiertas")
        .select("codigo_licitacion, nombre_licitacion, fecha_cierre")
        .lt("fecha_cierre", corte)
        .order("fecha_cierre")
        .execute()
    )
    a_borrar = r.data or []

    if not a_borrar:
        log.info("✅ No hay licitaciones cerradas para limpiar.")
        return

    print(f"\n{'═'*72}")
    print(f"  CLEANUP — Licitaciones a eliminar: {len(a_borrar)}")
    print(f"  (fecha_cierre < {corte})")
    print(f"{'─'*72}")
    for lic in a_borrar[:15]:
        nombre = (lic.get("nombre_licitacion") or "Sin nombre")[:55]
        print(f"  {lic['codigo_licitacion']:<20}  {lic.get('fecha_cierre','?')}  {nombre}")
    if len(a_borrar) > 15:
        print(f"  ... y {len(a_borrar) - 15} más")
    print(f"{'═'*72}")

    if args.dry_run:
        print(f"\n  [DRY-RUN] Se borrarían {len(a_borrar)} licitaciones")
        print(f"  y en cascada sus match_scores + pricing_recommendations.\n")
        return

    # --- Borrar (CASCADE se encarga de match_scores y pricing_recommendations) ---
    codigos = [r["codigo_licitacion"] for r in a_borrar]

    # Supabase no soporta DELETE con IN en todos los SDKs — usar lote manual
    CHUNK = 50
    total_borradas = 0
    for i in range(0, len(codigos), CHUNK):
        chunk = codigos[i: i + CHUNK]
        try:
            supabase.table("licitaciones_abiertas").delete().in_(
                "codigo_licitacion", chunk
            ).execute()
            total_borradas += len(chunk)
            log.info(f"  Borradas {total_borradas}/{len(codigos)} licitaciones")
        except Exception as e:
            log.error(f"  ❌ Error borrando chunk {i}-{i+CHUNK}: {e}")
            raise

    # --- Verificar estado post-limpieza ---
    res = supabase.table("licitaciones_abiertas").select(
        "id", count="exact"
    ).execute()
    ms_res = supabase.table("match_scores").select(
        "id", count="exact"
    ).execute()
    pr_res = supabase.table("pricing_recommendations").select(
        "id", count="exact"
    ).execute()

    print(f"\n{'═'*72}")
    print(f"  CLEANUP COMPLETADO")
    print(f"{'─'*72}")
    print(f"  Licitaciones borradas            : {total_borradas}")
    print(f"  licitaciones_abiertas restantes  : {res.count}")
    print(f"  match_scores restantes           : {ms_res.count}")
    print(f"  pricing_recommendations restantes: {pr_res.count}")
    print(f"{'═'*72}\n")
    log.info("=== cleanup_licitaciones finalizado ===")


if __name__ == "__main__":
    main()
