"""
backtest_pricing.py — Valida el motor de precios contra licitaciones ya cerradas.

Toma N licitaciones adjudicadas de licitaciones_mercado, aplica la lógica de
pricing (benchmarks + estrategia) y compara el precio recomendado vs el precio
real ganador. Permite saber si el engine hubiera sido competitivo.

Uso:
    python3 scripts/backtest_pricing.py
    python3 scripts/backtest_pricing.py --n 20 --mes 2025-12
    python3 scripts/backtest_pricing.py --cod 1234-5-LE25
"""

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.db.supabase_client import get_client
from src.core.config import FACTOR_AGRESIVO, FACTOR_EQUILIBRADO, UNSPSC42_MIN, UNSPSC42_MAX

logging.basicConfig(level=logging.WARNING)

FACTOR_CONSERVADOR = 1.10


def load_benchmarks(supabase) -> dict:
    """Carga todos los benchmarks en un dict {codigo_onu: row}."""
    all_rows = {}
    offset = 0
    while True:
        r = (
            supabase.table("precios_benchmark")
            .select("codigo_onu,precio_mediana,precio_p25,precio_p75,n_observaciones")
            .range(offset, offset + 999)
            .execute()
        )
        if not r.data:
            break
        for row in r.data:
            all_rows[row["codigo_onu"]] = row
        if len(r.data) < 1000:
            break
        offset += 1000
    return all_rows


def load_licitaciones(supabase, n: int, mes: str = None, cod: str = None) -> list:
    """Carga licitaciones adjudicadas con ítems UNSPSC42."""
    # Buscar licitaciones con items UNSPSC42
    q = (
        supabase.table("licitaciones_mercado")
        .select("codigo_licitacion,nombre_licitacion,fecha_adjudicacion,mes_proceso")
        .eq("estado", "Adjudicada")
        .gte("codigo_onu", UNSPSC42_MIN)
        .lte("codigo_onu", UNSPSC42_MAX)
        .gt("precio_unitario_ganador", 0)
    )
    if mes:
        q = q.eq("mes_proceso", mes)
    if cod:
        q = q.eq("codigo_licitacion", cod)

    r = q.order("fecha_adjudicacion", desc=True).limit(n * 5).execute()
    if not r.data:
        return []

    # Deduplicar por codigo_licitacion y tomar los primeros N
    seen = {}
    for row in r.data:
        c = row["codigo_licitacion"]
        if c not in seen:
            seen[c] = row
        if len(seen) >= n:
            break
    return list(seen.values())


def load_items_licitacion(supabase, codigo: str) -> list:
    """Carga todos los ítems UNSPSC42 de una licitación adjudicada."""
    r = (
        supabase.table("licitaciones_mercado")
        .select(
            "codigo_onu,descripcion_onu,nombre_item,cantidad,"
            "precio_unitario_ganador,monto_total_adjudicado,nombre_ganador"
        )
        .eq("codigo_licitacion", codigo)
        .eq("estado", "Adjudicada")
        .gte("codigo_onu", UNSPSC42_MIN)
        .lte("codigo_onu", UNSPSC42_MAX)
        .gt("precio_unitario_ganador", 0)
        .execute()
    )
    return r.data or []


def fmt_precio(v):
    if v is None:
        return "      N/D"
    return f"${float(v):>10,.0f}"


def pct(recom, real):
    if not recom or not real or float(real) == 0:
        return None
    return (float(recom) - float(real)) / float(real) * 100


def clasificar(pct_val):
    if pct_val is None:
        return "N/D ", "?"
    if pct_val <= -10:
        return "✅ HUBIERA GANADO", "muy competitivo"
    if pct_val <= 5:
        return "✅ COMPETITIVO   ", "dentro del rango"
    if pct_val <= 20:
        return "⚠️  CERCA        ", "ligeramente alto"
    return "❌ MUY CARO      ", "necesita ajuste"


def main():
    parser = argparse.ArgumentParser(
        description="Backtest del motor de precios contra licitaciones históricas"
    )
    parser.add_argument("--n", type=int, default=10,
                        help="Número de licitaciones a analizar (default: 10)")
    parser.add_argument("--mes", default=None,
                        help="Filtrar por mes (ej: 2025-12)")
    parser.add_argument("--cod", default=None,
                        help="Código de licitación específica")
    args = parser.parse_args()

    load_dotenv()
    supabase = get_client()

    print(f"\n{'═'*78}")
    print(f"  BACKTEST — Motor de Precios vs Realidad Histórica")
    if args.mes:
        print(f"  Período: {args.mes}")
    print(f"{'═'*78}")

    benchmarks = load_benchmarks(supabase)
    print(f"  Benchmarks cargados: {len(benchmarks):,} códigos ONU\n")

    licitaciones = load_licitaciones(supabase, args.n, args.mes, args.cod)
    if not licitaciones:
        print("  Sin licitaciones adjudicadas con ítems UNSPSC42. Verifica los datos.")
        return

    # Estadísticas globales
    total_items = 0
    items_ganaria = 0
    items_competitivo = 0
    items_lejos = 0
    items_sin_benchmark = 0

    for lic in licitaciones:
        codigo = lic["codigo_licitacion"]
        nombre = (lic.get("nombre_licitacion") or "Sin nombre")[:60]
        fecha_adj = lic.get("fecha_adjudicacion", "?")

        items = load_items_licitacion(supabase, codigo)
        if not items:
            continue

        print(f"\n{'─'*78}")
        print(f"  📋 {codigo}  ({fecha_adj[:10] if fecha_adj else '?'})")
        print(f"  {nombre}")
        print(f"  Ítems UNSPSC42 con precio: {len(items)}")
        print(f"  {'─'*78}")
        print(f"  {'PRODUCTO':<28} {'REAL':>11} {'AGRESIVO':>11} {'EQUILIB.':>11} {'RESULTADO':>20}")
        print(f"  {'─'*78}")

        monto_real_total = 0
        monto_agresivo_total = 0
        monto_equilibrado_total = 0

        for item in items:
            onu = item.get("codigo_onu")
            precio_real = item.get("precio_unitario_ganador")
            cantidad = float(item.get("cantidad") or 1)
            nombre_p = (item.get("descripcion_onu") or item.get("nombre_item") or "?")[:27]

            bench = benchmarks.get(onu)
            if not bench or not bench.get("precio_p25"):
                print(f"  {nombre_p:<28} {fmt_precio(precio_real)}  {'N/D':>11}  {'N/D':>11}  {'sin benchmark':>20}")
                items_sin_benchmark += 1
                total_items += 1
                if precio_real:
                    monto_real_total += float(precio_real) * cantidad
                continue

            p25 = float(bench["precio_p25"])
            precio_agr = p25 * FACTOR_AGRESIVO
            precio_equ = p25 * FACTOR_EQUILIBRADO

            pct_agr = pct(precio_agr, precio_real)
            resultado, _ = clasificar(pct_agr)

            total_items += 1
            if pct_agr is not None:
                if pct_agr <= -10:
                    items_ganaria += 1
                elif pct_agr <= 5:
                    items_competitivo += 1
                else:
                    items_lejos += 1

            if precio_real:
                monto_real_total += float(precio_real) * cantidad
            monto_agresivo_total += precio_agr * cantidad
            monto_equilibrado_total += precio_equ * cantidad

            pct_str = f"{pct_agr:+.1f}%" if pct_agr is not None else "N/D"
            print(
                f"  {nombre_p:<28} "
                f"{fmt_precio(precio_real)} "
                f"{fmt_precio(precio_agr)} "
                f"{fmt_precio(precio_equ)} "
                f"  {pct_str:>7}  {resultado}"
            )

        # Totales de la licitación
        if monto_real_total > 0:
            pct_agr_total = (monto_agresivo_total - monto_real_total) / monto_real_total * 100
            pct_equ_total = (monto_equilibrado_total - monto_real_total) / monto_real_total * 100
            ganador_tipico = items[0].get("nombre_ganador", "?")[:30] if items else "?"
            print(f"  {'─'*78}")
            print(f"  Ganador real       : {ganador_tipico}")
            print(f"  Monto real total   : {fmt_precio(monto_real_total)}")
            print(f"  Nuestra oferta AGR : {fmt_precio(monto_agresivo_total)}  ({pct_agr_total:+.1f}% vs real)")
            print(f"  Nuestra oferta EQU : {fmt_precio(monto_equilibrado_total)}  ({pct_equ_total:+.1f}% vs real)")

    # Resumen global
    print(f"\n{'═'*78}")
    print(f"  RESUMEN BACKTEST")
    print(f"{'─'*78}")
    print(f"  Licitaciones analizadas  : {len(licitaciones)}")
    print(f"  Total ítems UNSPSC42     : {total_items}")
    print(f"  ✅ Hubiera ganado (agr<-10%): {items_ganaria}  ({items_ganaria/max(total_items,1)*100:.0f}%)")
    print(f"  ✅ Competitivo (agr<+5%)  : {items_competitivo}  ({items_competitivo/max(total_items,1)*100:.0f}%)")
    print(f"  ❌ Precio alto (agr>+5%)  : {items_lejos}  ({items_lejos/max(total_items,1)*100:.0f}%)")
    print(f"  ⚪ Sin benchmark          : {items_sin_benchmark}  ({items_sin_benchmark/max(total_items,1)*100:.0f}%)")

    cobertura_ok = items_ganaria + items_competitivo
    if total_items > 0:
        pct_ok = cobertura_ok / total_items * 100
        print(f"\n  🎯 Competitividad global : {pct_ok:.0f}% de ítems dentro de rango ganador")
        if pct_ok >= 70:
            print(f"  ✅ El motor de precios es COMPETITIVO — listo para producción")
        elif pct_ok >= 50:
            print(f"  ⚠️  El motor es PARCIALMENTE competitivo — revisar calibración")
        else:
            print(f"  ❌ El motor necesita ajuste — p25 puede estar desalineado")
    print(f"{'═'*78}\n")


if __name__ == "__main__":
    main()
