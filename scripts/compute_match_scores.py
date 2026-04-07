"""
compute_match_scores.py — Calcula el Match Score de SASF para cada licitación
abierta almacenada en licitaciones_abiertas.

Algoritmo de scoring (0-100 por componente):
  • score_match      (45%): cobertura = % ítems de la licitación en catálogo SASF
  • score_win_rate   (25%): tasa de éxito bayesiana en esos códigos ONU
  • score_experiencia(20%): log-scale de cuántas veces SASF ha ofertado en esos productos
  • score_mercado    (10%): profundidad del mercado (benchmark disponible y activo)

Recomendación final:
  ALTA     → score >= 60   (buscar activamente)
  MEDIA    → score >= 35   (vale la pena evaluar)
  BAJA     → score >= 15   (posible si hay capacidad)
  SIN_MATCH→ score <  15   (no aplica)

Uso:
    python3 scripts/compute_match_scores.py
    python3 scripts/compute_match_scores.py --rut 76930423-1
    python3 scripts/compute_match_scores.py --todos    # incluye licitaciones ya cerradas
    python3 scripts/compute_match_scores.py --force    # recomputa scores existentes
"""

import argparse
import logging
import math
import os
import sys
from datetime import date
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.db.supabase_client import get_client, safe_upsert
from src.core.config import (
    SASF_RUT,
    W_MATCH, W_WIN_RATE, W_EXPERIENCIA, W_MERCADO,
    THRESH_ALTA, THRESH_MEDIA, THRESH_BAJA,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

SASF_RUT_DEFAULT = SASF_RUT   # alias para compatibilidad con --rut default
BATCH_SIZE = 300

# ---------------------------------------------------------------------------
# Carga del catálogo SASF con estadísticas históricas
# ---------------------------------------------------------------------------

def load_catalog(supabase) -> dict:
    """
    Lee ofertas_sasf + precios_benchmark y construye un dict indexado por
    codigo_onu con todas las estadísticas necesarias para el scoring.

    Retorna:
        {
          42311505: {
            "nombre": "VENDAS O VENDAJES PARA USO GENERAL",
            "n_bids": 66,
            "n_wins": 8,
            "win_rate_bayes": 0.136,   # (wins+1)/(bids+10)
            "mercado_depth": 1356,     # n_observaciones en benchmark
            "precio_mediana": 2730,
            "precio_p25": 650,
          }, ...
        }
    """
    log.info("Cargando catálogo SASF...")

    # Paginación manual (PostgREST devuelve máx 1000 por llamada)
    rows = []
    offset = 0
    while True:
        r = (
            supabase.table("ofertas_sasf")
            .select("codigo_onu, resultado_oferta")
            .range(offset, offset + 999)
            .execute()
        )
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        offset += 1000

    log.info(f"  {len(rows)} registros de ofertas_sasf leídos")

    # Agregar por codigo_onu
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

    # Enriquecer con benchmark (nombre + precio + profundidad)
    bench_rows = []
    offset = 0
    while True:
        r = (
            supabase.table("precios_benchmark")
            .select("codigo_onu, descripcion_onu, precio_mediana, precio_p25, n_observaciones")
            .range(offset, offset + 999)
            .execute()
        )
        if not r.data:
            break
        bench_rows.extend(r.data)
        if len(r.data) < 1000:
            break
        offset += 1000

    bench_map = {row["codigo_onu"]: row for row in bench_rows}

    for onu, stats in catalog.items():
        b = bench_map.get(onu, {})
        stats["nombre"]         = b.get("descripcion_onu", f"ONU {onu}")
        stats["mercado_depth"]  = int(b.get("n_observaciones") or 0)
        stats["precio_mediana"] = float(b.get("precio_mediana") or 0) or None
        stats["precio_p25"]     = float(b.get("precio_p25") or 0) or None
        # Tasa bayesiana: suaviza con prior de 10 observaciones, 10% win rate
        stats["win_rate_bayes"] = (stats["n_wins"] + 1) / (stats["n_bids"] + 10)

    log.info(f"  Catálogo SASF: {len(catalog)} códigos ONU únicos")

    # Calcular máximos para normalización
    max_bids  = max((s["n_bids"]       for s in catalog.values()), default=1)
    max_depth = max((s["mercado_depth"] for s in catalog.values()), default=1)

    return catalog, max_bids, max_depth


# ---------------------------------------------------------------------------
# Motor de scoring
# ---------------------------------------------------------------------------

def score_licitacion(
    lic: dict,
    catalog: dict,
    max_bids: int,
    max_depth: int,
) -> dict:
    """
    Calcula el score de match para una licitación dada.

    Devuelve el dict listo para upsert en match_scores.
    """
    items = lic.get("items") or []
    if not items:
        return _no_match(lic, "Sin ítems en la licitación")

    n_total = len(items)
    matched_items = []

    for item in items:
        onu = item.get("codigo_onu")
        if onu and onu in catalog:
            matched_items.append({
                "item":  item,
                "stats": catalog[onu],
            })

    n_match = len(matched_items)

    # ── Score match (cobertura) ─────────────────────────────────────────────
    score_match = (n_match / n_total) * 100 if n_total > 0 else 0

    if n_match == 0:
        return _no_match(lic, "Ningún ítem coincide con catálogo SASF")

    # ── Score win rate (bayesiano) ──────────────────────────────────────────
    win_rates = [m["stats"]["win_rate_bayes"] for m in matched_items]
    avg_win_rate = sum(win_rates) / len(win_rates)
    score_win_rate = min(avg_win_rate * 100, 100)

    # ── Score experiencia (log-scale sobre máximo) ─────────────────────────
    n_bids_list = [m["stats"]["n_bids"] for m in matched_items]
    avg_bids = sum(n_bids_list) / len(n_bids_list)
    score_experiencia = (math.log10(avg_bids + 1) / math.log10(max_bids + 1)) * 100 if max_bids > 0 else 0

    # ── Score mercado (log-scale sobre máximo) ─────────────────────────────
    depths = [m["stats"]["mercado_depth"] for m in matched_items]
    avg_depth = sum(depths) / len(depths)
    score_mercado = (math.log10(avg_depth + 1) / math.log10(max_depth + 1)) * 100 if max_depth > 0 else 0

    # ── Score total ponderado ───────────────────────────────────────────────
    score_total = (
        W_MATCH       * score_match       +
        W_WIN_RATE    * score_win_rate     +
        W_EXPERIENCIA * score_experiencia  +
        W_MERCADO     * score_mercado
    )

    # ── Recomendación + razón ───────────────────────────────────────────────
    recomendacion, razon = _recomendar(
        score_total, score_match, score_win_rate, n_match, n_total, avg_win_rate
    )

    # ── Detalle de ítems que hacen match ───────────────────────────────────
    items_detail = []
    for m in matched_items:
        s = m["stats"]
        items_detail.append({
            "codigo_onu":         m["item"].get("codigo_onu"),
            "nombre_item":        m["item"].get("nombre", "")[:200],
            "nombre_producto":    s.get("nombre", "")[:200],
            "n_bids":             s["n_bids"],
            "n_wins":             s["n_wins"],
            "win_rate_pct":       round(s["win_rate_bayes"] * 100, 1),
            "benchmark_mediana":  s.get("precio_mediana"),
            "benchmark_p25":      s.get("precio_p25"),
        })

    return {
        "codigo_licitacion":  lic["codigo_licitacion"],
        "rut_proveedor":      SASF_RUT_DEFAULT,
        "score_total":        round(score_total, 2),
        "score_match":        round(score_match, 2),
        "score_win_rate":     round(score_win_rate, 2),
        "score_experiencia":  round(score_experiencia, 2),
        "score_mercado":      round(score_mercado, 2),
        "n_items_total":      n_total,
        "n_items_match":      n_match,
        "pct_match":          round(score_match, 1),
        "items_match_detail": items_detail,
        "recomendacion":      recomendacion,
        "razon":              razon,
        # Campos denormalizados
        "fecha_cierre":       lic.get("fecha_cierre"),
        "nombre_licitacion":  (lic.get("nombre_licitacion") or "")[:500],
        "nombre_organismo":   (lic.get("nombre_organismo") or "")[:300],
        "monto_estimado":     lic.get("monto_estimado"),
    }


def _no_match(lic: dict, razon: str) -> dict:
    """Score cero cuando no hay match alguno."""
    return {
        "codigo_licitacion":  lic["codigo_licitacion"],
        "rut_proveedor":      SASF_RUT_DEFAULT,
        "score_total":        0.0,
        "score_match":        0.0,
        "score_win_rate":     0.0,
        "score_experiencia":  0.0,
        "score_mercado":      0.0,
        "n_items_total":      len(lic.get("items") or []),
        "n_items_match":      0,
        "pct_match":          0.0,
        "items_match_detail": [],
        "recomendacion":      "SIN_MATCH",
        "razon":              razon,
        "fecha_cierre":       lic.get("fecha_cierre"),
        "nombre_licitacion":  (lic.get("nombre_licitacion") or "")[:500],
        "nombre_organismo":   (lic.get("nombre_organismo") or "")[:300],
        "monto_estimado":     lic.get("monto_estimado"),
    }


def _recomendar(
    score_total: float,
    score_match: float,
    score_win_rate: float,
    n_match: int,
    n_total: int,
    avg_win_rate: float,
) -> tuple[str, str]:
    """Devuelve (recomendacion, razon) según los scores."""
    win_pct = round(avg_win_rate * 100, 1)
    pct_cob = round(score_match, 0)

    if score_total >= THRESH_ALTA:
        rec = "ALTA"
        if avg_win_rate >= 0.12:
            razon = (f"SASF cubre {n_match}/{n_total} ítems ({pct_cob:.0f}%) "
                     f"con tasa de éxito del {win_pct}%. Oportunidad fuerte.")
        else:
            razon = (f"SASF cubre {n_match}/{n_total} ítems ({pct_cob:.0f}%). "
                     f"Alta cobertura, aunque la tasa de éxito ({win_pct}%) tiene margen de mejora.")
    elif score_total >= THRESH_MEDIA:
        rec = "MEDIA"
        if n_match < n_total:
            razon = (f"SASF cubre {n_match} de {n_total} ítems ({pct_cob:.0f}%). "
                     f"Cobertura parcial, tasa de éxito: {win_pct}%.")
        else:
            razon = (f"Cobertura total ({n_match} ítems), tasa de éxito moderada: {win_pct}%.")
    elif score_total >= THRESH_BAJA:
        rec = "BAJA"
        razon = (f"Solo {n_match}/{n_total} ítems en catálogo SASF ({pct_cob:.0f}%). "
                 f"Oportunidad limitada. Evaluar con criterio comercial.")
    else:
        rec = "SIN_MATCH"
        razon = (f"Solo {n_match}/{n_total} ítems coinciden ({pct_cob:.0f}%) "
                 f"y la tasa de éxito histórica es muy baja ({win_pct}%).")

    return rec, razon


# ---------------------------------------------------------------------------
# Carga de licitaciones desde Supabase
# ---------------------------------------------------------------------------

def load_licitaciones(supabase, solo_abiertas: bool = True) -> list[dict]:
    """
    Lee licitaciones_abiertas.
    Si solo_abiertas=True, mantiene solo licitaciones con fecha_cierre >= hoy
    O con fecha_cierre NULL (aún no parseada — se consideran abiertas).
    """
    log.info("Cargando licitaciones abiertas...")
    rows = []
    offset = 0
    today = date.today().isoformat()

    while True:
        q = (
            supabase.table("licitaciones_abiertas")
            .select("codigo_licitacion, nombre_licitacion, nombre_organismo, "
                    "fecha_cierre, monto_estimado, n_items_total, n_items_unspsc42, items")
            .range(offset, offset + 999)
        )
        # Sin filtro de fecha en DB — filtramos en Python para incluir NULL
        r = q.execute()
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        offset += 1000

    if solo_abiertas:
        # Incluir: fecha_cierre >= hoy  O  fecha_cierre IS NULL (aún no parseada)
        rows = [
            row for row in rows
            if not row.get("fecha_cierre") or row["fecha_cierre"] >= today
        ]
        log.info(f"  {len(rows)} licitaciones abiertas (incl. sin fecha)")
    else:
        log.info(f"  {len(rows)} licitaciones cargadas (todas)")
    return rows


def load_existing_scores(supabase, rut: str) -> set:
    """Carga todos los codigo_licitacion ya calculados en un set (O(1) lookup)."""
    existing = set()
    offset = 0
    while True:
        r = (
            supabase.table("match_scores")
            .select("codigo_licitacion")
            .eq("rut_proveedor", rut)
            .range(offset, offset + 999)
            .execute()
        )
        if not r.data:
            break
        existing.update(row["codigo_licitacion"] for row in r.data)
        if len(r.data) < 1000:
            break
        offset += 1000
    return existing


# ---------------------------------------------------------------------------
# Upsert scores
# ---------------------------------------------------------------------------

def upsert_scores(supabase, scores: list[dict]):
    safe_upsert(supabase, "match_scores", scores,
                on_conflict="codigo_licitacion,rut_proveedor",
                batch_size=BATCH_SIZE)


# ---------------------------------------------------------------------------
# Reporte de resumen en consola
# ---------------------------------------------------------------------------

def print_summary(scores: list[dict]):
    from collections import Counter
    recs = Counter(s["recomendacion"] for s in scores)
    high = [s for s in scores if s["recomendacion"] == "ALTA"]
    med  = [s for s in scores if s["recomendacion"] == "MEDIA"]

    print("\n" + "═" * 72)
    print("  MATCH SCORING — RESUMEN")
    print("═" * 72)
    print(f"  Total licitaciones evaluadas : {len(scores)}")
    print(f"  🔥 ALTA PRIORIDAD            : {recs.get('ALTA', 0)}")
    print(f"  ⭐ MEDIA PRIORIDAD           : {recs.get('MEDIA', 0)}")
    print(f"  📌 BAJA PRIORIDAD            : {recs.get('BAJA', 0)}")
    print(f"  ❌ SIN MATCH                 : {recs.get('SIN_MATCH', 0)}")

    if high:
        print("\n─── 🔥 TOP OPORTUNIDADES ALTA PRIORIDAD ───────────────────────────")
        high_sorted = sorted(high, key=lambda s: s["score_total"], reverse=True)
        for s in high_sorted[:10]:
            cierre = s.get("fecha_cierre", "?")
            monto  = s.get("monto_estimado")
            monto_str = f"  ${monto:,.0f}" if monto else ""
            print(f"\n  [{s['score_total']:.0f}pts] {s['codigo_licitacion']}")
            print(f"    {(s['nombre_licitacion'] or '')[:65]}")
            print(f"    Organismo: {(s['nombre_organismo'] or '')[:50]}  |  Cierre: {cierre}{monto_str}")
            print(f"    Match: {s['n_items_match']}/{s['n_items_total']} ítems  "
                  f"| WinRate: {s['score_win_rate']:.0f}pts  "
                  f"| {s['razon'][:80]}")

    if med:
        print("\n─── ⭐ TOP OPORTUNIDADES MEDIA PRIORIDAD ───────────────────────────")
        med_sorted = sorted(med, key=lambda s: s["score_total"], reverse=True)
        for s in med_sorted[:5]:
            cierre = s.get("fecha_cierre", "?")
            print(f"  [{s['score_total']:.0f}pts] {s['codigo_licitacion']}  "
                  f"Match:{s['n_items_match']}/{s['n_items_total']}  "
                  f"Cierre:{cierre}  {(s['nombre_licitacion'] or '')[:50]}")

    print("\n" + "═" * 72)
    print("  Scores guardados en Supabase → tabla match_scores")
    print("  → Siguiente: python3 scripts/match_report.py  (reporte completo)")
    print("═" * 72 + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Calcula Match Scores SASF para licitaciones abiertas"
    )
    rut_env = os.getenv("PROVEEDOR_RUT", SASF_RUT_DEFAULT)
    parser.add_argument("--rut", default=rut_env,
                        help=f"RUT proveedor (default: env PROVEEDOR_RUT o {SASF_RUT_DEFAULT})")
    parser.add_argument("--todos", action="store_true",
                        help="Incluir licitaciones ya cerradas")
    parser.add_argument("--force", action="store_true",
                        help="Recomputar scores que ya existen")
    args = parser.parse_args()

    load_dotenv()
    supabase = get_client()

    # 1. Cargar catálogo SASF con stats
    catalog, max_bids, max_depth = load_catalog(supabase)
    if not catalog:
        log.error("❌ Catálogo SASF vacío. Verifica tabla ofertas_sasf.")
        sys.exit(1)

    # 2. Cargar licitaciones
    licitaciones = load_licitaciones(supabase, solo_abiertas=not args.todos)
    if not licitaciones:
        log.warning("Sin licitaciones para evaluar. Corre primero fetch_open_licitaciones.py")
        sys.exit(0)

    # 3. Computar scores
    log.info(f"\nComputando scores para {len(licitaciones)} licitaciones...")
    scores = []
    skipped = 0

    # Cargar existentes en un set (1 query en lugar de N+1)
    existing = set() if args.force else load_existing_scores(supabase, args.rut)
    if existing:
        log.info(f"  {len(existing)} scores ya existentes en DB")

    for lic in licitaciones:
        codigo = lic.get("codigo_licitacion")
        if not codigo:
            continue
        if not args.force and codigo in existing:
            skipped += 1
            continue

        result = score_licitacion(lic, catalog, max_bids, max_depth)
        scores.append(result)

    if skipped:
        log.info(f"  {skipped} scores ya existentes saltados (--force para recomputar)")

    if not scores:
        log.info("Sin scores nuevos que calcular.")
        # Aún así mostrar resumen de los que ya hay en DB
    else:
        log.info(f"  {len(scores)} scores calculados — guardando...")
        upsert_scores(supabase, scores)

    # 4. Mostrar resumen (incluyendo los ya guardados si no hay nuevos)
    if scores:
        print_summary(scores)
    else:
        log.info("Usa --force para ver el resumen de scores existentes.")

    log.info("=== compute_match_scores finalizado ===")


if __name__ == "__main__":
    main()
