"""
compute_pricing.py — Feature 2: Pricing Recommendations

Para cada licitación con score ALTA o MEDIA, calcula los precios óptimos
de oferta para cada ítem basándose en:
  • Benchmarks de mercado (precio_p25, precio_mediana de precios_benchmark)
  • Historial de SASF (gap vs. ganador de ofertas_sasf)
  • Win rate histórico por código ONU

Tres estrategias de precio:
  AGRESIVA    → p25 × 0.90  (10% bajo cuartil inferior — máxima competitividad)
  EQUILIBRADA → p25 × 1.00  (en el cuartil inferior — precio competitivo)
  CONSERVADORA→ mediana      (precio de mercado — margen más cómodo)

Estrategia recomendada por ítem:
  • gap_mediana > 40%  →  AGRESIVA   (SASF ha ofertado muy por encima del ganador)
  • gap_mediana 10-40% →  EQUILIBRADA
  • gap_mediana < 10%  →  EQUILIBRADA (sin datos suficientes o ya competitivo)
  • win_rate > 15%     →  CONSERVADORA (ya gana, no sacrificar margen)

Uso:
    python3 scripts/compute_pricing.py
    python3 scripts/compute_pricing.py --rec ALTA        # solo ALTA prioridad
    python3 scripts/compute_pricing.py --rec ALTA MEDIA  # ALTA y MEDIA (default)
    python3 scripts/compute_pricing.py --force           # recomputa existentes
"""

import argparse
import logging
import math
import sys
from datetime import date
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.db.supabase_client import get_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

SASF_RUT = "76930423-1"
BATCH_SIZE = 100

# Multiplicadores de precio por estrategia
FACTOR_AGRESIVO     = 0.90   # p25 × 0.90
FACTOR_EQUILIBRADO  = 1.00   # p25 × 1.00
# Conservador usa la mediana directamente

GAP_UMBRAL_AGRESIVO    = 40.0   # % gap para recomendar estrategia agresiva
GAP_UMBRAL_EQUILIBRADO = 10.0   # % gap para recomendar equilibrada
WIN_RATE_CONSERVADOR   = 0.15   # si win_rate > 15%, conservadora
MIN_BIDS_PARA_ESTRATEGIA = 3    # mínimo bids para usar gap histórico


# ---------------------------------------------------------------------------
# Carga de datos
# ---------------------------------------------------------------------------

def _paginate(supabase, table: str, select: str, filters: dict = None) -> list[dict]:
    """Lee todos los registros de una tabla con paginación."""
    rows = []
    offset = 0
    while True:
        q = supabase.table(table).select(select).range(offset, offset + 999)
        if filters:
            for col, val in filters.items():
                q = q.eq(col, val)
        r = q.execute()
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        offset += 1000
    return rows


def load_gap_data(supabase) -> dict:
    """
    Carga estadísticas de gap histórico SASF por codigo_onu.
    Usa solo filas con gap_porcentual IS NOT NULL.

    Retorna:
        {42311505: {
            "n_con_gap": 39,
            "gap_avg_pct": 66.3,
            "gap_mediana_pct": 62.3,     # más robusta que el promedio
            "precio_sasf_avg": 956,
            "precio_ganador_avg": 682,
        }, ...}
    """
    log.info("Cargando historial de gaps SASF...")
    rows = _paginate(
        supabase, "ofertas_sasf",
        "codigo_onu, monto_neto_oferta, precio_ganador, gap_porcentual, resultado_oferta"
    )

    # Agregar por codigo_onu
    by_onu: dict[int, dict] = {}
    for row in rows:
        onu = row.get("codigo_onu")
        gap = row.get("gap_porcentual")
        if not onu or gap is None:
            continue
        try:
            gap_f = float(gap)
            precio_sasf = float(row.get("monto_neto_oferta") or 0)
            precio_gan  = float(row.get("precio_ganador") or 0)
        except (ValueError, TypeError):
            continue

        if onu not in by_onu:
            by_onu[onu] = {"gaps": [], "precios_sasf": [], "precios_gan": []}

        by_onu[onu]["gaps"].append(gap_f)
        if precio_sasf > 0:
            by_onu[onu]["precios_sasf"].append(precio_sasf)
        if precio_gan > 0:
            by_onu[onu]["precios_gan"].append(precio_gan)

    # Calcular estadísticas finales
    result = {}
    for onu, data in by_onu.items():
        gaps = sorted(data["gaps"])
        n = len(gaps)
        gap_mediana = gaps[n // 2] if n else None
        gap_avg = sum(gaps) / n if n else None
        precio_sasf_avg = (
            sum(data["precios_sasf"]) / len(data["precios_sasf"])
            if data["precios_sasf"] else None
        )
        precio_gan_avg = (
            sum(data["precios_gan"]) / len(data["precios_gan"])
            if data["precios_gan"] else None
        )
        result[onu] = {
            "n_con_gap":         n,
            "gap_avg_pct":       round(gap_avg, 1) if gap_avg is not None else None,
            "gap_mediana_pct":   round(gap_mediana, 1) if gap_mediana is not None else None,
            "precio_sasf_avg":   round(precio_sasf_avg, 0) if precio_sasf_avg else None,
            "precio_ganador_avg": round(precio_gan_avg, 0) if precio_gan_avg else None,
        }

    log.info(f"  {len(result)} códigos ONU con datos de gap histórico")
    return result


def load_benchmark(supabase) -> dict:
    """
    Retorna {codigo_onu: {precio_p25, precio_mediana, precio_p75, n_observaciones, descripcion_onu}}
    """
    log.info("Cargando benchmarks de precios...")
    rows = _paginate(
        supabase, "precios_benchmark",
        "codigo_onu, descripcion_onu, precio_p25, precio_mediana, precio_p75, n_observaciones"
    )
    result = {}
    for row in rows:
        onu = row.get("codigo_onu")
        if not onu:
            continue
        result[onu] = {
            "descripcion_onu":  row.get("descripcion_onu", f"ONU {onu}"),
            "precio_p25":       float(row["precio_p25"]) if row.get("precio_p25") else None,
            "precio_mediana":   float(row["precio_mediana"]) if row.get("precio_mediana") else None,
            "precio_p75":       float(row["precio_p75"]) if row.get("precio_p75") else None,
            "n_observaciones":  int(row.get("n_observaciones") or 0),
        }
    log.info(f"  {len(result)} códigos ONU con benchmark")
    return result


def load_catalog_stats(supabase) -> dict:
    """
    Retorna {codigo_onu: {n_bids, n_wins, win_rate_bayes}}
    """
    rows = _paginate(supabase, "ofertas_sasf", "codigo_onu, resultado_oferta")
    catalog: dict[int, dict] = {}
    for row in rows:
        onu = row.get("codigo_onu")
        if not onu:
            continue
        if onu not in catalog:
            catalog[onu] = {"n_bids": 0, "n_wins": 0}
        catalog[onu]["n_bids"] += 1
        if row.get("resultado_oferta") == "Seleccionada":
            catalog[onu]["n_wins"] += 1
    for onu, s in catalog.items():
        s["win_rate_bayes"] = (s["n_wins"] + 1) / (s["n_bids"] + 10)
    return catalog


def load_match_scores(supabase, recs: list[str]) -> list[dict]:
    """Carga match_scores filtrando por recomendacion."""
    log.info(f"Cargando match_scores ({', '.join(recs)})...")
    all_rows = []
    for rec in recs:
        rows = _paginate(
            supabase, "match_scores",
            "codigo_licitacion, score_total, recomendacion, "
            "nombre_licitacion, nombre_organismo, fecha_cierre, monto_estimado",
            {"recomendacion": rec, "rut_proveedor": SASF_RUT}
        )
        all_rows.extend(rows)
    log.info(f"  {len(all_rows)} licitaciones con score {', '.join(recs)}")
    return all_rows


def load_licitacion_items(supabase, codigo: str) -> list[dict]:
    """Carga los ítems de una licitación específica desde licitaciones_abiertas."""
    r = (
        supabase.table("licitaciones_abiertas")
        .select("items")
        .eq("codigo_licitacion", codigo)
        .limit(1)
        .execute()
    )
    if not r.data:
        return []
    return r.data[0].get("items") or []


# ---------------------------------------------------------------------------
# Lógica de pricing por ítem
# ---------------------------------------------------------------------------

def _estrategia_item(
    gap_mediana_pct: Optional[float],
    n_bids: int,
    win_rate_bayes: float,
) -> tuple[str, str]:
    """Determina estrategia y razón para un ítem."""
    if win_rate_bayes > WIN_RATE_CONSERVADOR:
        pct = round(win_rate_bayes * 100, 1)
        return "CONSERVADORA", (
            f"Win rate {pct}% — SASF ya es competitivo. "
            f"Mantener precio con margen (mediana de mercado)."
        )
    if n_bids < MIN_BIDS_PARA_ESTRATEGIA or gap_mediana_pct is None:
        return "EQUILIBRADA", (
            "Sin historial suficiente. Usar p25 del mercado como referencia base."
        )
    if gap_mediana_pct > GAP_UMBRAL_AGRESIVO:
        return "AGRESIVA", (
            f"SASF ha ofertado {gap_mediana_pct:.0f}% sobre el ganador histórico. "
            f"Se requiere reducción agresiva de precio para ser competitivo."
        )
    if gap_mediana_pct > GAP_UMBRAL_EQUILIBRADO:
        return "EQUILIBRADA", (
            f"Gap de {gap_mediana_pct:.0f}% vs ganador — ajustar al p25 del mercado."
        )
    return "EQUILIBRADA", (
        f"Gap bajo ({gap_mediana_pct:.0f}%) — precio en zona competitiva. "
        f"Usar p25 como referencia."
    )


def _calcular_precios(
    p25: Optional[float],
    mediana: Optional[float],
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """Retorna (precio_agresivo, precio_equilibrado, precio_conservador)."""
    if not p25:
        return None, None, mediana
    agresivo    = round(p25 * FACTOR_AGRESIVO, 2)
    equilibrado = round(p25 * FACTOR_EQUILIBRADO, 2)
    conservador = round(mediana, 2) if mediana else round(p25 * 1.30, 2)
    return agresivo, equilibrado, conservador


def _ajuste_necesario(
    precio_recomendado: Optional[float],
    precio_sasf_historico: Optional[float],
) -> Optional[float]:
    """% que SASF debe reducir su precio histórico para llegar al recomendado."""
    if not precio_recomendado or not precio_sasf_historico or precio_sasf_historico <= 0:
        return None
    return round((precio_recomendado - precio_sasf_historico) / precio_sasf_historico * 100, 1)


def _estrategia_global(item_estrategias: list[str]) -> str:
    """Determina la estrategia global basada en los ítems — prevalece la más agresiva."""
    if not item_estrategias:
        return "EQUILIBRADA"
    if "AGRESIVA" in item_estrategias:
        return "AGRESIVA"
    if "CONSERVADORA" in item_estrategias and "EQUILIBRADA" not in item_estrategias:
        return "CONSERVADORA"
    return "EQUILIBRADA"


# ---------------------------------------------------------------------------
# Procesar una licitación
# ---------------------------------------------------------------------------

def compute_pricing_for_lic(
    lic: dict,
    benchmark: dict,
    gap_data: dict,
    catalog: dict,
) -> dict:
    """
    Calcula pricing para todos los ítems de una licitación.
    Retorna dict listo para upsert en pricing_recommendations.
    """
    codigo = lic["codigo_licitacion"]
    items_lic = lic.get("_items", [])  # inyectado por el llamador

    items_pricing = []
    estrategias_usadas = []
    n_con_precio = 0
    n_sin_precio = 0

    total_agresivo    = 0.0
    total_equilibrado = 0.0
    total_conservador = 0.0

    for item in items_lic:
        onu = item.get("codigo_onu")
        if not onu or not catalog.get(onu):
            continue  # ítem no en catálogo SASF

        bench    = benchmark.get(onu, {})
        stats    = catalog.get(onu, {})
        gap_info = gap_data.get(onu, {})

        p25     = bench.get("precio_p25")
        mediana = bench.get("precio_mediana")
        nombre_prod = bench.get("descripcion_onu", f"ONU {onu}")

        n_bids      = stats.get("n_bids", 0)
        n_wins      = stats.get("n_wins", 0)
        win_rate    = stats.get("win_rate_bayes", 0.09)
        n_con_gap   = gap_info.get("n_con_gap", 0)
        gap_med_pct = gap_info.get("gap_mediana_pct")
        precio_sasf_hist = gap_info.get("precio_sasf_avg")

        try:
            cantidad = float(item.get("cantidad") or 0)
        except (ValueError, TypeError):
            cantidad = 0.0

        # Precios por estrategia
        agr, equ, con = _calcular_precios(p25, mediana)

        if p25 is None and mediana is None:
            n_sin_precio += 1
            est_item = "EQUILIBRADA"
            razon_item = "Sin benchmark disponible para este código ONU."
        else:
            n_con_precio += 1
            est_item, razon_item = _estrategia_item(gap_med_pct, n_bids, win_rate)
            estrategias_usadas.append(est_item)

            if cantidad > 0:
                total_agresivo    += (agr or 0) * cantidad
                total_equilibrado += (equ or 0) * cantidad
                total_conservador += (con or 0) * cantidad

        # Ajuste necesario desde precio histórico SASF al recomendado
        precio_rec = {"AGRESIVA": agr, "EQUILIBRADA": equ, "CONSERVADORA": con}.get(est_item, equ)
        ajuste_pct = _ajuste_necesario(precio_rec, precio_sasf_hist)

        items_pricing.append({
            "correlativo":           item.get("correlativo"),
            "codigo_onu":            onu,
            "nombre_item":           (item.get("nombre") or "")[:200],
            "nombre_producto":       nombre_prod[:200],
            "cantidad":              cantidad,
            "unidad":                item.get("unidad"),
            # Benchmark
            "precio_p25":            p25,
            "precio_mediana":        mediana,
            # Precios recomendados
            "precio_agresivo":       agr,
            "precio_equilibrado":    equ,
            "precio_conservador":    con,
            # Montos (precio × cantidad)
            "monto_agresivo":        round(agr * cantidad, 0) if agr and cantidad else None,
            "monto_equilibrado":     round(equ * cantidad, 0) if equ and cantidad else None,
            "monto_conservador":     round(con * cantidad, 0) if con and cantidad else None,
            # Historial SASF
            "n_bids_sasf":           n_bids,
            "n_wins_sasf":           n_wins,
            "n_con_gap":             n_con_gap,
            "gap_mediana_pct":       gap_med_pct,
            "precio_sasf_historico": precio_sasf_hist,
            "ajuste_necesario_pct":  ajuste_pct,
            # Estrategia item
            "estrategia_item":       est_item,
            "razon_item":            razon_item,
        })

    estrategia_global = _estrategia_global(estrategias_usadas)

    # Razón global
    n_agr = estrategias_usadas.count("AGRESIVA")
    n_equ = estrategias_usadas.count("EQUILIBRADA")
    n_con = estrategias_usadas.count("CONSERVADORA")
    resumen_razon = (
        f"{n_con_precio} ítems con benchmark. "
        f"Estrategias: {n_agr} AGRESIVA, {n_equ} EQUILIBRADA, {n_con} CONSERVADORA. "
        f"Oferta total recomendada (equilibrada): ${total_equilibrado:,.0f}."
    )

    return {
        "codigo_licitacion":      codigo,
        "rut_proveedor":          SASF_RUT,
        "recomendacion_score":    lic.get("recomendacion"),
        "score_total":            lic.get("score_total"),
        "n_items_con_precio":     n_con_precio,
        "n_items_sin_precio":     n_sin_precio,
        "monto_total_agresivo":   round(total_agresivo, 0) if total_agresivo else None,
        "monto_total_equilibrado": round(total_equilibrado, 0) if total_equilibrado else None,
        "monto_total_conservador": round(total_conservador, 0) if total_conservador else None,
        "estrategia_global":      estrategia_global,
        "resumen_razon":          resumen_razon,
        "items_pricing":          items_pricing,
        "nombre_licitacion":      (lic.get("nombre_licitacion") or "")[:500],
        "nombre_organismo":       (lic.get("nombre_organismo") or "")[:300],
        "fecha_cierre":           lic.get("fecha_cierre"),
        "monto_estimado":         lic.get("monto_estimado"),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Calcula Pricing Recommendations para licitaciones ALTA/MEDIA"
    )
    parser.add_argument("--rec", nargs="+", default=["ALTA", "MEDIA"],
                        choices=["ALTA", "MEDIA", "BAJA"],
                        help="Niveles de recomendación a procesar (default: ALTA MEDIA)")
    parser.add_argument("--force", action="store_true",
                        help="Recomputa precios ya calculados")
    args = parser.parse_args()

    load_dotenv()
    supabase = get_client()

    # 1. Cargar datos de referencia
    gap_data = load_gap_data(supabase)
    benchmark = load_benchmark(supabase)
    catalog   = load_catalog_stats(supabase)

    # 2. Cargar licitaciones con score en rango
    match_rows = load_match_scores(supabase, args.rec)
    if not match_rows:
        log.warning("Sin licitaciones con los scores indicados.")
        sys.exit(0)

    # 3. Pre-cargar ítems ya calculados (para idempotencia)
    existing = set()
    if not args.force:
        ex_rows = _paginate(
            supabase, "pricing_recommendations",
            "codigo_licitacion",
            {"rut_proveedor": SASF_RUT}
        )
        existing = {r["codigo_licitacion"] for r in ex_rows}
        if existing:
            log.info(f"  {len(existing)} licitaciones ya tienen pricing (--force para recomputar)")

    # 4. Computar
    log.info(f"\nCalculando precios para {len(match_rows)} licitaciones...")
    results = []
    skipped = 0

    for lic in match_rows:
        codigo = lic["codigo_licitacion"]
        if not args.force and codigo in existing:
            skipped += 1
            continue

        # Inyectar ítems de licitaciones_abiertas
        items = load_licitacion_items(supabase, codigo)
        lic["_items"] = items

        row = compute_pricing_for_lic(lic, benchmark, gap_data, catalog)

        if row["n_items_con_precio"] == 0:
            log.debug(f"  {codigo}: sin ítems con benchmark — igual se guarda")

        results.append(row)

    if skipped:
        log.info(f"  {skipped} saltados (ya calculados)")

    if not results:
        log.info("Sin pricing nuevo que calcular.")
        sys.exit(0)

    # 5. Upsert
    log.info(f"  {len(results)} registros — guardando en Supabase...")
    for i in range(0, len(results), BATCH_SIZE):
        batch = results[i: i + BATCH_SIZE]
        supabase.table("pricing_recommendations").upsert(
            batch,
            on_conflict="codigo_licitacion,rut_proveedor",
        ).execute()
        log.info(f"  ✓ Upserted {min(i + BATCH_SIZE, len(results))}/{len(results)}")

    # 6. Resumen
    alta = [r for r in results if r["recomendacion_score"] == "ALTA"]
    media = [r for r in results if r["recomendacion_score"] == "MEDIA"]

    print("\n" + "═" * 72)
    print("  PRICING RECOMMENDATIONS — RESUMEN")
    print("═" * 72)
    print(f"  Licitaciones procesadas : {len(results)}")
    print(f"  🔥 ALTA prioridad       : {len(alta)}")
    print(f"  ⭐ MEDIA prioridad      : {len(media)}")

    if alta:
        print("\n─── 🔥 ALTA PRIORIDAD — Montos de oferta recomendados ─────────────")
        for r in sorted(alta, key=lambda x: x["score_total"] or 0, reverse=True):
            monto_rec = r.get("monto_total_equilibrado")
            monto_agg = r.get("monto_total_agresivo")
            monto_lic = r.get("monto_estimado")
            est       = r.get("estrategia_global", "?")
            n_items   = r.get("n_items_con_precio", 0)
            print(f"\n  [{r['score_total']:.0f}pts] {r['codigo_licitacion']}  [{est}]")
            print(f"    {(r['nombre_licitacion'] or '')[:65]}")
            print(f"    Ítems con precio: {n_items}")
            if monto_agg:
                print(f"    Oferta AGRESIVA    : ${monto_agg:>14,.0f}")
            if monto_rec:
                print(f"    Oferta EQUILIBRADA : ${monto_rec:>14,.0f}  ← recomendada")
            if monto_lic:
                print(f"    Monto est. lic.    : ${monto_lic:>14,.0f}")

    print("\n" + "═" * 72)
    print("  Pricing guardado en Supabase → tabla pricing_recommendations")
    print("  → Siguiente: python3 scripts/pricing_report.py")
    print("═" * 72 + "\n")


if __name__ == "__main__":
    main()
