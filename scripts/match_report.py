"""
match_report.py — Genera el reporte completo de Match Scoring desde Supabase.

Lee la tabla match_scores y presenta las oportunidades ordenadas por score,
con detalle de ítems, tasas de éxito y benchmarks de precio.

Uso:
    python3 scripts/match_report.py
    python3 scripts/match_report.py --top 20          # solo top 20
    python3 scripts/match_report.py --rec ALTA        # solo ALTA prioridad
    python3 scripts/match_report.py --dias 7          # licitaciones que cierran en 7 días
    python3 scripts/match_report.py --exportar        # guarda CSV en data/match_report_YYYYMMDD.csv
"""

import argparse
import csv
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.db.supabase_client import get_client

logging.basicConfig(level=logging.WARNING)  # silenciar logs internos en el reporte
log = logging.getLogger(__name__)

COLORES = {
    "ALTA":      "🔥",
    "MEDIA":     "⭐",
    "BAJA":      "📌",
    "SIN_MATCH": "❌",
}


def load_scores(supabase, rec_filter: str = None, dias: int = None, top: int = None) -> list[dict]:
    """Lee match_scores con filtros opcionales."""
    q = (
        supabase.table("match_scores")
        .select("*")
        .neq("recomendacion", "SIN_MATCH")
        .order("score_total", desc=True)
    )

    if rec_filter:
        q = q.eq("recomendacion", rec_filter.upper())

    if dias is not None:
        hoy = date.today()
        limite = (hoy + timedelta(days=dias)).isoformat()
        hoy_str = hoy.isoformat()
        q = q.gte("fecha_cierre", hoy_str).lte("fecha_cierre", limite)

    if top:
        q = q.limit(top)

    r = q.execute()
    return r.data or []


def format_monto(monto) -> str:
    if not monto:
        return "N/D"
    try:
        m = float(monto)
        if m >= 1_000_000:
            return f"${m/1_000_000:.1f}M"
        elif m >= 1_000:
            return f"${m/1_000:.0f}K"
        return f"${m:.0f}"
    except Exception:
        return "N/D"


def format_price(p) -> str:
    if p is None:
        return "N/D"
    try:
        return f"${float(p):,.0f}"
    except Exception:
        return "N/D"


def print_report(scores: list[dict], dias: int = None):
    from collections import Counter

    if not scores:
        print("\n  Sin oportunidades encontradas con los filtros aplicados.")
        return

    hoy = date.today()
    recs = Counter(s["recomendacion"] for s in scores)

    print("\n" + "═" * 76)
    print("  BID INTELLIGENCE ENGINE — REPORTE DE OPORTUNIDADES")
    print(f"  Generado: {hoy}  |  RUT SASF: 76930423-1")
    print("═" * 76)

    if dias:
        print(f"  ⚠  Filtrado: licitaciones que cierran en los próximos {dias} días")
    print(f"\n  Total oportunidades : {len(scores)}")
    for rec, n in [("ALTA", recs.get("ALTA", 0)), ("MEDIA", recs.get("MEDIA", 0)),
                   ("BAJA", recs.get("BAJA", 0))]:
        if n:
            print(f"  {COLORES[rec]} {rec:10} : {n}")

    # Agrupar por recomendación
    for nivel in ["ALTA", "MEDIA", "BAJA"]:
        grupo = [s for s in scores if s["recomendacion"] == nivel]
        if not grupo:
            continue

        print(f"\n{'─' * 76}")
        print(f"  {COLORES[nivel]}  {nivel} PRIORIDAD  ({len(grupo)} licitaciones)")
        print(f"{'─' * 76}")

        for s in grupo:
            cierre = s.get("fecha_cierre") or "?"
            dias_restantes = None
            if cierre != "?":
                try:
                    dias_restantes = (date.fromisoformat(cierre) - hoy).days
                except Exception:
                    pass

            dias_str = ""
            if dias_restantes is not None:
                if dias_restantes < 0:
                    dias_str = "  [CERRADA]"
                elif dias_restantes == 0:
                    dias_str = "  [HOY]"
                elif dias_restantes <= 3:
                    dias_str = f"  [⚠ {dias_restantes}d]"
                else:
                    dias_str = f"  [{dias_restantes}d]"

            print(f"\n  ▶ {s['codigo_licitacion']}   Score: {s['score_total']:.1f}/100{dias_str}")
            print(f"    {(s['nombre_licitacion'] or 'Sin nombre')[:70]}")
            print(f"    Organismo : {(s['nombre_organismo'] or 'N/D')[:60]}")
            print(f"    Cierre    : {cierre}   |   Monto estimado: {format_monto(s['monto_estimado'])}")
            print(f"    Match     : {s['n_items_match']}/{s['n_items_total']} ítems  "
                  f"({s['pct_match']:.0f}% cobertura)")
            print(f"    Componentes: Match={s['score_match']:.0f} | WinRate={s['score_win_rate']:.0f} | "
                  f"Exp={s['score_experiencia']:.0f} | Mercado={s['score_mercado']:.0f}")
            print(f"    → {s['razon']}")

            # Detalle de ítems que hacen match
            detail = s.get("items_match_detail") or []
            if detail:
                print(f"    Ítems relevantes:")
                for item in detail[:5]:
                    onu   = item.get("codigo_onu", "?")
                    nom   = (item.get("nombre_producto") or item.get("nombre_item") or "?")[:55]
                    wr    = item.get("win_rate_pct", 0)
                    med   = format_price(item.get("benchmark_mediana"))
                    p25   = format_price(item.get("benchmark_p25"))
                    bids  = item.get("n_bids", 0)
                    wins  = item.get("n_wins", 0)
                    print(f"      • [{onu}] {nom}")
                    print(f"        Bids: {bids} ({wins} ganados, {wr:.1f}%)  "
                          f"Benchmark: mediana={med}  p25={p25}")
                if len(detail) > 5:
                    print(f"      ... y {len(detail) - 5} ítems más")

    print("\n" + "═" * 76 + "\n")


def export_csv(scores: list[dict], path: str):
    """Exporta scores a CSV."""
    if not scores:
        return

    fieldnames = [
        "recomendacion", "score_total", "score_match", "score_win_rate",
        "score_experiencia", "score_mercado", "codigo_licitacion",
        "nombre_licitacion", "nombre_organismo", "fecha_cierre",
        "monto_estimado", "n_items_total", "n_items_match", "pct_match", "razon"
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(scores)

    print(f"  ✓ CSV exportado: {path}")


def main():
    parser = argparse.ArgumentParser(description="Reporte de Match Scoring SASF")
    parser.add_argument("--top",       type=int,   default=None, help="Límite de resultados")
    parser.add_argument("--rec",       type=str,   default=None, help="Filtrar por recomendación (ALTA/MEDIA/BAJA)")
    parser.add_argument("--dias",      type=int,   default=None, help="Licitaciones que cierran en N días")
    parser.add_argument("--exportar",  action="store_true",      help="Exportar a CSV")
    args = parser.parse_args()

    load_dotenv()
    supabase = get_client()

    scores = load_scores(supabase, rec_filter=args.rec, dias=args.dias, top=args.top)

    if not scores:
        print("\n  ⚠  Sin datos en match_scores.")
        print("  Corre primero:")
        print("    python3 scripts/fetch_open_licitaciones.py")
        print("    python3 scripts/compute_match_scores.py\n")
        return

    print_report(scores, dias=args.dias)

    if args.exportar:
        from datetime import datetime
        fname = f"data/match_report_{datetime.today().strftime('%Y%m%d')}.csv"
        Path("data").mkdir(exist_ok=True)
        export_csv(scores, fname)


if __name__ == "__main__":
    main()
