"""
pricing_report.py — Feature 2: Reporte de Precios Recomendados

Muestra tabla completa de precios por ítem para cada licitación con
pricing calculado, con énfasis en la estrategia recomendada.

Uso:
    python3 scripts/pricing_report.py                       # todas las ALTA
    python3 scripts/pricing_report.py --rec ALTA MEDIA      # ALTA y MEDIA
    python3 scripts/pricing_report.py --cod 2098-40-LE26    # licitación específica
    python3 scripts/pricing_report.py --top 5               # top 5 por score
    python3 scripts/pricing_report.py --exportar            # guarda CSV
    python3 scripts/pricing_report.py --estrategia AGRESIVA # solo agresivas
"""

import argparse
import csv
import logging
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.db.supabase_client import get_client
from src.core.config import SASF_RUT

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers de formato
# ---------------------------------------------------------------------------

def _fmt_precio(v) -> str:
    if v is None:
        return "     N/D"
    try:
        return f"${float(v):>9,.0f}"
    except (ValueError, TypeError):
        return "     N/D"


def _fmt_pct(v, signo=True) -> str:
    if v is None:
        return "   N/D"
    try:
        f = float(v)
        s = "+" if (signo and f > 0) else ""
        return f"{s}{f:>6.1f}%"
    except (ValueError, TypeError):
        return "   N/D"


def _fmt_monto(v) -> str:
    if v is None:
        return "         N/D"
    try:
        return f"${float(v):>13,.0f}"
    except (ValueError, TypeError):
        return "         N/D"


def _estrategia_icon(est: str) -> str:
    return {"AGRESIVA": "🔴", "EQUILIBRADA": "🟡", "CONSERVADORA": "🟢"}.get(est, "⚪")


def _rec_icon(rec: str) -> str:
    return {"ALTA": "🔥", "MEDIA": "⭐", "BAJA": "📌"}.get(rec, "")


# ---------------------------------------------------------------------------
# Carga
# ---------------------------------------------------------------------------

def load_pricing(supabase, recs: list[str], codigos: list[str] = None,
                 estrategia: str = None, top: int = None) -> list[dict]:
    """Carga pricing_recommendations con filtros opcionales."""
    all_rows = []
    for rec in recs:
        rows_for_rec = []
        offset = 0
        while True:
            q = (
                supabase.table("pricing_recommendations")
                .select("*")
                .eq("rut_proveedor", SASF_RUT)
                .eq("recomendacion_score", rec)
                .order("score_total", desc=True)
                .range(offset, offset + 999)
            )
            if codigos:
                q = q.in_("codigo_licitacion", codigos)
            if estrategia:
                q = q.eq("estrategia_global", estrategia)
            r = q.execute()
            if not r.data:
                break
            rows_for_rec.extend(r.data)
            if len(r.data) < 1000:
                break
            offset += 1000
        all_rows.extend(rows_for_rec)

    # Ordenar globalmente por score
    all_rows.sort(key=lambda x: float(x.get("score_total") or 0), reverse=True)

    if top:
        all_rows = all_rows[:top]

    return all_rows


# ---------------------------------------------------------------------------
# Impresión de una licitación
# ---------------------------------------------------------------------------

def print_licitacion(row: dict, show_all_items: bool = False, max_items: int = 10):
    """Imprime el detalle de pricing de una licitación."""
    codigo       = row["codigo_licitacion"]
    rec          = row.get("recomendacion_score", "?")
    score        = row.get("score_total", 0)
    est          = row.get("estrategia_global", "?")
    nombre_lic   = row.get("nombre_licitacion") or "Sin nombre"
    organismo    = row.get("nombre_organismo") or "N/D"
    fecha_cierre = row.get("fecha_cierre") or "?"
    monto_lic    = row.get("monto_estimado")
    n_con_precio = row.get("n_items_con_precio", 0)
    n_sin_precio = row.get("n_items_sin_precio", 0)
    items        = row.get("items_pricing") or []

    # Montos totales
    m_agr = row.get("monto_total_agresivo")
    m_equ = row.get("monto_total_equilibrado")
    m_con = row.get("monto_total_conservador")

    # Calcular cobertura precio vs monto licitación
    cobertura_str = ""
    if monto_lic and m_equ:
        pct = m_equ / float(monto_lic) * 100
        cobertura_str = f"  ({pct:.0f}% del presupuesto estimado)"

    print(f"\n{'━'*78}")
    print(f"  {_rec_icon(rec)} [{score:.0f}pts] {codigo}  {_estrategia_icon(est)} {est}")
    print(f"  {nombre_lic[:75]}")
    print(f"  Organismo : {organismo[:65]}")
    print(f"  Cierre    : {fecha_cierre}   Monto est. lic.: {_fmt_monto(monto_lic)}")
    print(f"  Ítems con benchmark: {n_con_precio}  |  Sin benchmark: {n_sin_precio}")

    # Tabla de totales
    print(f"\n  {'─'*72}")
    print(f"  {'ESTRATEGIA':<16} {'OFERTA TOTAL':>16}  {'vs. PRESUPUESTO':>16}")
    print(f"  {'─'*72}")
    for label, monto, marker in [
        ("🔴 AGRESIVA",    m_agr, " "),
        ("🟡 EQUILIBRADA", m_equ, " ← RECOMENDADA" if est == "EQUILIBRADA" else " ← RECOMENDADA" if not m_agr else " "),
        ("🟢 CONSERVADORA",m_con, " "),
    ]:
        if est == "AGRESIVA"    and "AGRESIVA"    in label: marker = " ← RECOMENDADA"
        if est == "CONSERVADORA" and "CONSERVADORA" in label: marker = " ← RECOMENDADA"
        cob = ""
        if monto_lic and monto:
            pct = float(monto) / float(monto_lic) * 100
            cob = f"  {pct:>5.0f}%"
        print(f"  {label:<17} {_fmt_monto(monto)}{cob}{marker}")
    print(f"  {'─'*72}")

    # Tabla de ítems
    if not items:
        print("\n  Sin ítems con pricing calculado.")
        return

    # Mostrar máximo 10 ítems por defecto (los de mayor monto equilibrado)
    items_sorted = sorted(
        items,
        key=lambda x: float(x.get("monto_equilibrado") or 0),
        reverse=True
    )
    mostrar = items_sorted if show_all_items else items_sorted[:max_items]

    print(f"\n  {'─'*78}")
    print(f"  {'#':<3} {'PRODUCTO':<32} {'CANT':>6} {'P25':>10} {'AGRESIVO':>10} {'EQUILIB.':>10} {'GAP HIST':>9} {'AJUSTE':>8}")
    print(f"  {'─'*78}")

    for i, item in enumerate(mostrar, 1):
        nombre_p = (item.get("nombre_producto") or item.get("nombre_item") or "?")[:31]
        cant     = item.get("cantidad") or 0
        p25      = item.get("precio_p25")
        agr      = item.get("precio_agresivo")
        equ      = item.get("precio_equilibrado")
        gap_med  = item.get("gap_mediana_pct")
        ajuste   = item.get("ajuste_necesario_pct")
        est_item = item.get("estrategia_item", "")
        icon     = _estrategia_icon(est_item)

        cant_str = f"{cant:>6.0f}" if cant else "     ?"
        print(
            f"  {i:<3} {nombre_p:<32} {cant_str} "
            f"{_fmt_precio(p25)} "
            f"{_fmt_precio(agr)} "
            f"{_fmt_precio(equ)} "
            f"{_fmt_pct(gap_med):>9} "
            f"{_fmt_pct(ajuste, signo=True):>8}"
            f" {icon}"
        )

    if len(items_sorted) > max_items and not show_all_items:
        print(f"  ... y {len(items_sorted) - max_items} ítems más (usa --todos para ver todos)")

    # Ítem con mayor impacto
    if items_sorted and items_sorted[0].get("ajuste_necesario_pct") is not None:
        top_item = items_sorted[0]
        nombre_top = (top_item.get("nombre_producto") or "?")[:50]
        ajuste_top = top_item.get("ajuste_necesario_pct")
        if ajuste_top and ajuste_top < -5:
            print(f"\n  ⚠️  Mayor ajuste necesario: {nombre_top}")
            print(f"     Reducir precio histórico en {abs(ajuste_top):.1f}% para ser competitivo")

    # Razón global
    razon = row.get("resumen_razon") or ""
    if razon:
        print(f"\n  💡 {razon[:120]}")


# ---------------------------------------------------------------------------
# Export CSV
# ---------------------------------------------------------------------------

def exportar_csv(rows: list[dict], filename: str):
    """Exporta pricing a CSV con una fila por ítem."""
    out_path = Path(filename)
    filas_csv = []

    for row in rows:
        items = row.get("items_pricing") or []
        base = {
            "codigo_licitacion":    row["codigo_licitacion"],
            "nombre_licitacion":    row.get("nombre_licitacion", ""),
            "nombre_organismo":     row.get("nombre_organismo", ""),
            "fecha_cierre":         row.get("fecha_cierre", ""),
            "monto_estimado_lic":   row.get("monto_estimado", ""),
            "recomendacion":        row.get("recomendacion_score", ""),
            "score_total":          row.get("score_total", ""),
            "estrategia_global":    row.get("estrategia_global", ""),
            "monto_total_agresivo":    row.get("monto_total_agresivo", ""),
            "monto_total_equilibrado": row.get("monto_total_equilibrado", ""),
            "monto_total_conservador": row.get("monto_total_conservador", ""),
        }
        if not items:
            filas_csv.append(base)
            continue
        for item in items:
            fila = {**base,
                "correlativo":            item.get("correlativo", ""),
                "codigo_onu":             item.get("codigo_onu", ""),
                "nombre_producto":        item.get("nombre_producto", ""),
                "nombre_item":            item.get("nombre_item", ""),
                "cantidad":               item.get("cantidad", ""),
                "unidad":                 item.get("unidad", ""),
                "precio_p25":             item.get("precio_p25", ""),
                "precio_mediana":         item.get("precio_mediana", ""),
                "precio_agresivo":        item.get("precio_agresivo", ""),
                "precio_equilibrado":     item.get("precio_equilibrado", ""),
                "precio_conservador":     item.get("precio_conservador", ""),
                "monto_agresivo":         item.get("monto_agresivo", ""),
                "monto_equilibrado":      item.get("monto_equilibrado", ""),
                "monto_conservador":      item.get("monto_conservador", ""),
                "n_bids_sasf":            item.get("n_bids_sasf", ""),
                "n_wins_sasf":            item.get("n_wins_sasf", ""),
                "gap_mediana_pct":        item.get("gap_mediana_pct", ""),
                "precio_sasf_historico":  item.get("precio_sasf_historico", ""),
                "ajuste_necesario_pct":   item.get("ajuste_necesario_pct", ""),
                "estrategia_item":        item.get("estrategia_item", ""),
                "razon_item":             item.get("razon_item", ""),
            }
            filas_csv.append(fila)

    if not filas_csv:
        print("Sin datos para exportar.")
        return

    fieldnames = list(filas_csv[0].keys())
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filas_csv)

    print(f"\n✅ CSV exportado: {out_path}  ({len(filas_csv)} filas)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Reporte de Pricing Recommendations — Feature 2"
    )
    parser.add_argument("--rec", nargs="+", default=["ALTA"],
                        choices=["ALTA", "MEDIA", "BAJA"],
                        help="Niveles de recomendación (default: ALTA)")
    parser.add_argument("--cod", nargs="+", default=None,
                        help="Código(s) de licitación específico(s)")
    parser.add_argument("--top", type=int, default=None,
                        help="Mostrar solo top N licitaciones")
    parser.add_argument("--estrategia", default=None,
                        choices=["AGRESIVA", "EQUILIBRADA", "CONSERVADORA"],
                        help="Filtrar por estrategia global")
    parser.add_argument("--todos", action="store_true",
                        help="Mostrar todos los ítems (ignora --max-items)")
    parser.add_argument("--max-items", type=int, default=10, dest="max_items",
                        help="Máximo de ítems por licitación (default: 10)")
    parser.add_argument("--exportar", action="store_true",
                        help="Exportar a CSV en data/")
    args = parser.parse_args()

    load_dotenv()
    supabase = get_client()

    rows = load_pricing(
        supabase,
        recs=args.rec,
        codigos=args.cod,
        estrategia=args.estrategia,
        top=args.top,
    )

    hoy = date.today().strftime("%Y-%m-%d")
    recs_str = ", ".join(args.rec)

    print("\n" + "═" * 78)
    print(f"  BID INTELLIGENCE ENGINE — PRICING RECOMMENDATIONS")
    print(f"  Generado: {hoy}  |  RUT SASF: {SASF_RUT}  |  Filtro: {recs_str}")
    print("═" * 78)

    if not rows:
        print("\n  Sin registros. Corre primero: python3 scripts/compute_pricing.py")
        print("═" * 78)
        return

    # Resumen ejecutivo
    n_agr = sum(1 for r in rows if r.get("estrategia_global") == "AGRESIVA")
    n_equ = sum(1 for r in rows if r.get("estrategia_global") == "EQUILIBRADA")
    n_con = sum(1 for r in rows if r.get("estrategia_global") == "CONSERVADORA")
    total_oportunidad = sum(
        float(r.get("monto_total_equilibrado") or 0) for r in rows
    )

    print(f"\n  Total licitaciones    : {len(rows)}")
    print(f"  🔴 Estrategia AGRESIVA    : {n_agr}")
    print(f"  🟡 Estrategia EQUILIBRADA : {n_equ}")
    print(f"  🟢 Estrategia CONSERVADORA: {n_con}")
    if total_oportunidad > 0:
        print(f"\n  💰 Pipeline total (equilibrada) : {_fmt_monto(total_oportunidad)}")

    # Detalle por licitación
    for row in rows:
        print_licitacion(row, show_all_items=args.todos, max_items=args.max_items)

    print("\n" + "═" * 78)
    print("  Leyenda columnas: P25=cuartil 25% mercado | AGRESIVO=p25×0.90 |")
    print("  EQUILIB.=p25×1.00 | GAP HIST=% que SASF ha ofertado sobre ganador |")
    print("  AJUSTE=% que SASF debe reducir su precio histórico para ser competitivo")
    print("═" * 78 + "\n")

    if args.exportar:
        filename = f"data/pricing_report_{hoy}.csv"
        exportar_csv(rows, filename)


if __name__ == "__main__":
    main()
