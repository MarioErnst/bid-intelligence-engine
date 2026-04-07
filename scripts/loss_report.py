"""
loss_report.py — Feature 3: Reporte de Diagnóstico de Pérdidas

Muestra un análisis completo y accionable de por qué SASF pierde licitaciones,
con secciones priorizadas para toma de decisiones comerciales.

Uso:
    python3 scripts/loss_report.py                    # reporte completo
    python3 scripts/loss_report.py --seccion alertas  # solo alertas
    python3 scripts/loss_report.py --seccion near_misses
    python3 scripts/loss_report.py --seccion competidores
    python3 scripts/loss_report.py --seccion sweet_spots
    python3 scripts/loss_report.py --seccion chronic
    python3 scripts/loss_report.py --seccion organismos
    python3 scripts/loss_report.py --seccion meses
    python3 scripts/loss_report.py --exportar         # guarda CSV
"""

import argparse
import csv
import sys
from datetime import date
from pathlib import Path
from typing import Dict, Optional

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.db.supabase_client import get_client

SASF_RUT = "76930423-1"
SECCIONES_VALIDAS = [
    "alertas", "near_misses", "no_precio", "competidores",
    "chronic", "sweet_spots", "organismos", "meses", "todas"
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pct(v) -> str:
    if v is None:
        return "  N/D"
    try:
        f = float(v)
        return f"{f:>6.1f}%"
    except (ValueError, TypeError):
        return "  N/D"


def _precio(v) -> str:
    if v is None:
        return "       N/D"
    try:
        return f"${float(v):>10,.0f}"
    except (ValueError, TypeError):
        return "       N/D"


def _prio_icon(p: str) -> str:
    return {
        "CRÍTICA":     "🚨",
        "ALTA":        "🔴",
        "MEDIA":       "🟡",
        "OPORTUNIDAD": "💚",
    }.get(p, "⚪")


# ---------------------------------------------------------------------------
# Carga
# ---------------------------------------------------------------------------

def load_diagnostics(supabase) -> Optional[Dict]:
    r = (
        supabase.table("loss_diagnostics")
        .select("*")
        .eq("rut_proveedor", SASF_RUT)
        .limit(1)
        .execute()
    )
    return r.data[0] if r.data else None


# ---------------------------------------------------------------------------
# Secciones del reporte
# ---------------------------------------------------------------------------

def print_alertas(data: dict):
    alertas = data.get("alertas") or []
    print(f"\n{'═'*78}")
    print("  ⚡ ALERTAS Y RECOMENDACIONES PRIORIZADAS")
    print(f"{'═'*78}")
    if not alertas:
        print("  Sin alertas registradas.")
        return
    for a in alertas:
        icon = _prio_icon(a.get("prioridad", ""))
        print(f"\n  {icon} [{a.get('prioridad','?'):10}] {a.get('titulo','')}")
        if a.get("detalle"):
            # Wrap a 70 chars
            detalle = a["detalle"]
            for i in range(0, len(detalle), 72):
                print(f"     {detalle[i:i+72]}")
        if a.get("accion"):
            print(f"     → {a['accion'][:75]}")


def print_resumen_global(data: dict):
    r = data.get("resumen_global") or {}
    computed = data.get("computed_at", "?")[:10]
    print(f"\n{'═'*78}")
    print(f"  📊 RESUMEN GLOBAL  —  Período analizado al {computed}")
    print(f"{'═'*78}")
    print(f"  Total ofertas          : {r.get('total_bids', '?'):>6}")
    print(f"  ✅ Ganadas             : {r.get('total_wins', '?'):>6}  ({r.get('win_rate_pct', '?')}%)")
    print(f"  ❌ Perdidas            : {r.get('total_losses', '?'):>6}")
    print(f"  Con datos de precio    : {r.get('n_con_gap', '?'):>6}  ({r.get('pct_con_gap', '?')}%)")
    print(f"  ─────────────────────────────────────────")
    print(f"  Gap promedio vs ganador: {_pct(r.get('gap_promedio_pct'))}")
    print(f"  Gap mediana vs ganador : {_pct(r.get('gap_mediana_pct'))}")
    print(f"  ─────────────────────────────────────────")
    print(f"  Near misses (<10% gap) : {r.get('near_misses', '?'):>6}  ← casi ganó")
    print(f"  SASF más barato pero perdió: {r.get('sasf_mas_barato', '?'):>3}  ← problema no-precio")
    print(f"  Pérdidas por PRECIO    : {r.get('perdidas_precio', '?'):>6}")
    print(f"  Pérdidas por OTRO      : {r.get('perdidas_otro', '?'):>6}")


def print_near_misses(data: dict, max_n: int = 20):
    near = data.get("near_misses") or []
    print(f"\n{'═'*78}")
    print(f"  🎯 NEAR MISSES — Licitaciones perdidas por < 10% de diferencia ({len(near)} total)")
    print(f"{'═'*78}")
    if not near:
        print("  Sin near misses registrados.")
        return
    print(f"  {'ID LICITACIÓN':<22} {'PRODUCTO':<28} {'GAP':>6} {'PRECIO SASF':>12} {'GANADOR':>12} {'COMPETIDOR':<25}")
    print(f"  {'─'*75}")
    for n in near[:max_n]:
        print(
            f"  {str(n.get('id_licitacion','')):<22} "
            f"{str(n.get('nombre_producto',''))[:27]:<28} "
            f"{_pct(n.get('gap_pct'))} "
            f"{_precio(n.get('precio_sasf'))} "
            f"{_precio(n.get('precio_ganador'))} "
            f"{str(n.get('proveedor_ganador',''))[:24]}"
        )
    if len(near) > max_n:
        print(f"  ... y {len(near) - max_n} más")
    print(f"\n  💡 Con bajar {near[0]['gap_pct']:.1f}% el precio, SASF hubiera ganado la primera.")


def print_no_precio(data: dict):
    no_p = data.get("perdidas_no_precio") or []
    print(f"\n{'═'*78}")
    print(f"  ⚠️  PÉRDIDAS NO-PRECIO — SASF era más barato pero perdió ({len(no_p)} casos)")
    print(f"{'═'*78}")
    if not no_p:
        print("  Sin pérdidas no-precio.")
        return
    print("  Señal de problemas técnicos, experiencia acreditada, garantías u otro criterio.")
    print(f"\n  {'ID LICITACIÓN':<22} {'PRODUCTO':<28} {'VENTAJA':>8} {'SASF':>12} {'GANADOR':>12}")
    print(f"  {'─'*75}")
    for r in no_p[:15]:
        print(
            f"  {str(r.get('id_licitacion','')):<22} "
            f"{str(r.get('nombre_producto',''))[:27]:<28} "
            f"{_pct(r.get('ventaja_precio_pct'))} "
            f"{_precio(r.get('precio_sasf'))} "
            f"{_precio(r.get('precio_ganador'))}"
        )
    if len(no_p) > 15:
        print(f"  ... y {len(no_p) - 15} más")
    print(f"\n  Acción: Revisar requisitos técnicos de estas licitaciones. "
          f"¿Qué tenía el ganador que SASF no acreditó?")


def print_competidores(data: dict):
    comps = data.get("top_competidores") or []
    print(f"\n{'═'*78}")
    print(f"  🥊 TOP COMPETIDORES — Quién le gana más a SASF")
    print(f"{'═'*78}")
    if not comps:
        print("  Sin datos de competidores.")
        return
    print(f"  {'#':<3} {'PROVEEDOR':<40} {'WINS':>5} {'PRODUCTOS':>9} {'GAP PROMEDIO':>13}")
    print(f"  {'─'*73}")
    for i, c in enumerate(comps[:12], 1):
        gap_str = _pct(c.get("gap_avg_pct")) if c.get("gap_avg_pct") is not None else "  N/D  "
        print(
            f"  {i:<3} {str(c.get('nombre',''))[:39]:<40} "
            f"{c.get('n_wins_vs_sasf', 0):>5} "
            f"{c.get('n_productos_dist', 0):>9} "
            f"{gap_str:>13}"
        )
    print(f"\n  💡 Investiga los precios públicos del competidor #1 en licitaciones adjudicadas.")


def print_chronic(data: dict):
    chronic = data.get("chronic_losers") or []
    print(f"\n{'═'*78}")
    print(f"  💀 CHRONIC LOSERS — Productos con ≥5 ofertas y 0 victorias ({len(chronic)} total)")
    print(f"{'═'*78}")
    if not chronic:
        print("  Sin chronic losers.")
        return

    # Separar por tipo
    precio_alto = [c for c in chronic if c.get("tipo") == "PRECIO_ALTO"]
    no_precio_c = [c for c in chronic if c.get("tipo") == "NO_PRECIO"]
    marginal    = [c for c in chronic if c.get("tipo") == "MARGINAL"]

    if precio_alto:
        print(f"\n  🔴 Problema PRECIO (gap > 30%): {len(precio_alto)} productos")
        print(f"  {'PRODUCTO':<40} {'BIDS':>5} {'GAP MED':>8} {'ACCIÓN':>6}")
        print(f"  {'─'*70}")
        for c in precio_alto[:10]:
            print(
                f"  {str(c.get('nombre',''))[:39]:<40} "
                f"{c.get('n_bids', 0):>5} "
                f"{_pct(c.get('gap_mediana')):>8} "
                f"  Bajar precio ≥{c.get('gap_mediana', 0):.0f}%"
            )

    if no_precio_c:
        print(f"\n  🟡 Problema NO-PRECIO (gap < 10%): {len(no_precio_c)} productos")
        print("  El precio no era el problema — revisar aspectos técnicos")
        for c in no_precio_c[:5]:
            print(f"    • {c['nombre'][:60]}  ({c['n_bids']} bids)")

    if marginal:
        print(f"\n  ⚪ Marginal: {len(marginal)} productos con gap entre 10-30%")


def print_sweet_spots(data: dict):
    sweet = data.get("sweet_spots") or []
    print(f"\n{'═'*78}")
    print(f"  💚 SWEET SPOTS — Productos donde SASF tiene ventaja ({len(sweet)} total)")
    print(f"{'═'*78}")
    if not sweet:
        print("  Sin sweet spots identificados.")
        return
    print(f"  {'PRODUCTO':<42} {'BIDS':>5} {'WINS':>5} {'WIN RATE':>9} {'PRECIO WIN':>11}")
    print(f"  {'─'*75}")
    for s in sweet[:15]:
        print(
            f"  {str(s.get('nombre',''))[:41]:<42} "
            f"{s.get('n_bids', 0):>5} "
            f"{s.get('n_wins', 0):>5} "
            f"{_pct(s.get('win_rate_pct')):>9} "
            f"{_precio(s.get('precio_win_avg')):>11}"
        )
    print(f"\n  💡 Busca activamente licitaciones con estos productos — son tu zona de confort.")


def print_organismos(data: dict):
    orgs = data.get("por_organismo") or []
    print(f"\n{'═'*78}")
    print(f"  🏥 POR ORGANISMO — Win rate por entidad compradora")
    print(f"{'═'*78}")
    if not orgs:
        print("  Sin datos de organismos.")
        return

    mejores = [o for o in orgs if o.get("n_wins", 0) > 0][:10]
    peores  = [o for o in sorted(orgs, key=lambda x: (x.get("win_rate_pct", 0), -x.get("n_bids", 0)))
               if o.get("n_wins", 0) == 0 and o.get("n_bids", 0) >= 5][:10]

    if mejores:
        print(f"\n  ✅ Mejores organismos (donde SASF ha ganado):")
        print(f"  {'ORGANISMO':<45} {'BIDS':>5} {'WINS':>5} {'WIN RATE':>9}")
        print(f"  {'─'*65}")
        for o in mejores:
            print(
                f"  {str(o.get('nombre',''))[:44]:<45} "
                f"{o.get('n_bids', 0):>5} "
                f"{o.get('n_wins', 0):>5} "
                f"{_pct(o.get('win_rate_pct')):>9}"
            )

    if peores:
        print(f"\n  ❌ Organismos con 0 victorias (≥5 intentos):")
        print(f"  {'ORGANISMO':<45} {'BIDS':>5} {'GAP AVG':>8}")
        print(f"  {'─'*60}")
        for o in peores[:8]:
            print(
                f"  {str(o.get('nombre',''))[:44]:<45} "
                f"{o.get('n_bids', 0):>5} "
                f"{_pct(o.get('gap_avg_pct')):>8}"
            )


def print_meses(data: dict):
    meses = data.get("por_mes") or []
    print(f"\n{'═'*78}")
    print(f"  📅 TENDENCIA MENSUAL — Evolución del win rate")
    print(f"{'═'*78}")
    if not meses:
        print("  Sin datos mensuales.")
        return
    print(f"  {'MES':<10} {'BIDS':>6} {'WINS':>6} {'WIN RATE':>9}  BARRA")
    print(f"  {'─'*60}")
    max_wr = max((m.get("win_rate_pct", 0) for m in meses), default=1) or 1
    for m in meses:
        wr = m.get("win_rate_pct", 0)
        barra = "█" * int(wr / max_wr * 20)
        print(
            f"  {m['mes']:<10} {m.get('n_bids', 0):>6} {m.get('n_wins', 0):>6} "
            f"{_pct(wr):>9}  {barra}"
        )


# ---------------------------------------------------------------------------
# Export CSV
# ---------------------------------------------------------------------------

def exportar_csv(data: dict, filename: str):
    """Exporta near misses + chronic losers a CSV."""
    hoy = date.today().strftime("%Y%m%d")
    path = Path(filename)

    # Near misses
    near = data.get("near_misses") or []
    near_path = path.parent / f"near_misses_{hoy}.csv"
    if near:
        with open(near_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=near[0].keys())
            writer.writeheader()
            writer.writerows(near)
        print(f"  ✅ Near misses: {near_path}")

    # Chronic losers
    chronic = data.get("chronic_losers") or []
    ch_path = path.parent / f"chronic_losers_{hoy}.csv"
    if chronic:
        with open(ch_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=chronic[0].keys())
            writer.writeheader()
            writer.writerows(chronic)
        print(f"  ✅ Chronic losers: {ch_path}")

    # Sweet spots
    sweet = data.get("sweet_spots") or []
    ss_path = path.parent / f"sweet_spots_{hoy}.csv"
    if sweet:
        with open(ss_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=sweet[0].keys())
            writer.writeheader()
            writer.writerows(sweet)
        print(f"  ✅ Sweet spots: {ss_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Reporte de Loss Diagnostics — Feature 3"
    )
    parser.add_argument("--seccion", default="todas",
                        choices=SECCIONES_VALIDAS,
                        help="Sección específica a mostrar (default: todas)")
    parser.add_argument("--exportar", action="store_true",
                        help="Exportar near misses, chronic y sweet spots a CSV")
    args = parser.parse_args()

    load_dotenv()
    supabase = get_client()

    data = load_diagnostics(supabase)
    if not data:
        print("\n  Sin datos. Corre primero: python3 scripts/compute_loss_diagnostics.py")
        return

    hoy = date.today().strftime("%Y-%m-%d")
    computed = (data.get("computed_at") or "?")[:10]

    print("\n" + "═" * 78)
    print(f"  BID INTELLIGENCE ENGINE — LOSS DIAGNOSTICS")
    print(f"  Generado: {hoy}  |  Datos al: {computed}  |  RUT: {SASF_RUT}")
    print("═" * 78)

    sec = args.seccion

    if sec in ("todas", "alertas"):
        print_alertas(data)

    if sec in ("todas",):
        print_resumen_global(data)

    if sec in ("todas", "near_misses"):
        print_near_misses(data)

    if sec in ("todas", "no_precio"):
        print_no_precio(data)

    if sec in ("todas", "competidores"):
        print_competidores(data)

    if sec in ("todas", "chronic"):
        print_chronic(data)

    if sec in ("todas", "sweet_spots"):
        print_sweet_spots(data)

    if sec in ("todas", "organismos"):
        print_organismos(data)

    if sec in ("todas", "meses"):
        print_meses(data)

    print("\n" + "═" * 78)
    print("  Secciones disponibles: --seccion [alertas|near_misses|no_precio|")
    print("  competidores|chronic|sweet_spots|organismos|meses]")
    print("═" * 78 + "\n")

    if args.exportar:
        print("Exportando CSVs...")
        exportar_csv(data, f"data/loss_report_{hoy}.csv")


if __name__ == "__main__":
    main()
