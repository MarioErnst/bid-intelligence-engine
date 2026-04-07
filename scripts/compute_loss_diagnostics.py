"""
compute_loss_diagnostics.py — Feature 3: Loss Diagnostics

Analiza el historial completo de ofertas SASF para diagnosticar
por qué se pierden licitaciones y qué patrones son accionables.

Dimensiones analizadas:
  • Resumen global        — win rate, near misses, pérdidas por precio vs otro
  • Competidores          — quién gana más y por cuánto
  • Near misses           — licitaciones perdidas por < 10% de diferencia
  • Pérdidas no-precio    — SASF era más barato pero igual perdió (alerta técnica)
  • Chronic losers        — productos con muchos bids y 0 wins
  • Sweet spots           — productos donde SASF sí gana (reforzar)
  • Por organismo         — entidades con mejor/peor tasa para SASF
  • Por mes               — tendencia temporal del win rate
  • Alertas               — recomendaciones priorizadas y concretas

Uso:
    python3 scripts/compute_loss_diagnostics.py
    python3 scripts/compute_loss_diagnostics.py --force
"""

import argparse
import logging
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.db.supabase_client import get_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

SASF_RUT = "76930423-1"

# Umbrales de diagnóstico
NEAR_MISS_THRESHOLD    = 10.0   # gap < 10% → casi ganó
NO_PRECIO_THRESHOLD    = 0.0    # gap < 0   → SASF era más barato
CHRONIC_MIN_BIDS       = 5      # mínimo bids para "chronic loser"
SWEET_SPOT_MIN_BIDS    = 3      # mínimo bids para "sweet spot"
SWEET_SPOT_MIN_WINRATE = 0.10   # mínimo win rate para sweet spot


# ---------------------------------------------------------------------------
# Carga de datos
# ---------------------------------------------------------------------------

def load_ofertas(supabase) -> list[dict]:
    """Carga todas las ofertas de SASF con campos relevantes."""
    log.info("Cargando ofertas SASF...")
    rows = []
    offset = 0
    while True:
        r = (
            supabase.table("ofertas_sasf")
            .select(
                "id_licitacion, codigo_onu, nombre_item, resultado_oferta, "
                "monto_neto_oferta, precio_ganador, gap_monetario, gap_porcentual, "
                "motivo_perdida, fecha_adjudicacion, unidad_compra, unidad_compra_rut, "
                "sector, region_unidad, proveedor_ganador, rut_ganador, mes_proceso"
            )
            .range(offset, offset + 999)
            .execute()
        )
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        offset += 1000
    log.info(f"  {len(rows)} ofertas cargadas")
    return rows


def load_benchmark_names(supabase) -> dict:
    """Retorna {codigo_onu: descripcion_onu}."""
    rows = []
    offset = 0
    while True:
        r = (
            supabase.table("precios_benchmark")
            .select("codigo_onu, descripcion_onu")
            .range(offset, offset + 999)
            .execute()
        )
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        offset += 1000
    return {row["codigo_onu"]: row.get("descripcion_onu", f"ONU {row['codigo_onu']}") for row in rows}


# ---------------------------------------------------------------------------
# Computaciones
# ---------------------------------------------------------------------------

def _safe_float(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def compute_resumen_global(rows: list[dict]) -> dict:
    total  = len(rows)
    wins   = sum(1 for r in rows if r.get("resultado_oferta") == "Seleccionada")
    losses = total - wins
    con_gap = [r for r in rows if _safe_float(r.get("gap_porcentual")) is not None]
    near_misses = sum(
        1 for r in con_gap
        if _safe_float(r["gap_porcentual"]) is not None
        and 0 <= _safe_float(r["gap_porcentual"]) < NEAR_MISS_THRESHOLD
        and r.get("resultado_oferta") != "Seleccionada"
    )
    sasf_mas_barato = sum(
        1 for r in con_gap
        if _safe_float(r["gap_porcentual"]) is not None
        and _safe_float(r["gap_porcentual"]) < NO_PRECIO_THRESHOLD
        and r.get("resultado_oferta") != "Seleccionada"
    )
    perdidas_precio = sum(
        1 for r in rows
        if r.get("motivo_perdida") == "PRECIO"
    )
    perdidas_otro = sum(
        1 for r in rows
        if r.get("motivo_perdida") == "OTRO"
    )
    gaps = [_safe_float(r["gap_porcentual"]) for r in con_gap
            if _safe_float(r.get("gap_porcentual")) is not None
            and _safe_float(r.get("gap_porcentual")) >= 0
            and r.get("resultado_oferta") != "Seleccionada"]
    gap_promedio = round(sum(gaps) / len(gaps), 1) if gaps else None
    gaps_sorted = sorted(gaps)
    gap_mediana = round(gaps_sorted[len(gaps_sorted)//2], 1) if gaps_sorted else None

    return {
        "total_bids":          total,
        "total_wins":          wins,
        "total_losses":        losses,
        "win_rate_pct":        round(wins / total * 100, 1) if total else 0,
        "n_con_gap":           len(con_gap),
        "near_misses":         near_misses,
        "sasf_mas_barato":     sasf_mas_barato,
        "perdidas_precio":     perdidas_precio,
        "perdidas_otro":       perdidas_otro,
        "gap_promedio_pct":    gap_promedio,
        "gap_mediana_pct":     gap_mediana,
        "pct_con_gap":         round(len(con_gap) / total * 100, 1) if total else 0,
    }


def compute_top_competidores(rows: list[dict], top_n: int = 15) -> list[dict]:
    """Competidores que ganaron más veces contra SASF."""
    by_comp: dict[str, dict] = {}
    for r in rows:
        if r.get("resultado_oferta") == "Seleccionada":
            continue  # SASF ganó, no hay competidor
        rut_gan = r.get("rut_ganador")
        nombre  = r.get("proveedor_ganador") or rut_gan or "Desconocido"
        gap     = _safe_float(r.get("gap_porcentual"))

        if not rut_gan:
            rut_gan = "__sin_rut__"

        if rut_gan not in by_comp:
            by_comp[rut_gan] = {
                "rut_ganador":    rut_gan,
                "nombre":         nombre,
                "n_wins_vs_sasf": 0,
                "gaps":           [],
                "productos":      set(),
            }
        by_comp[rut_gan]["n_wins_vs_sasf"] += 1
        if gap is not None and gap >= 0:
            by_comp[rut_gan]["gaps"].append(gap)
        if r.get("codigo_onu"):
            by_comp[rut_gan]["productos"].add(r["codigo_onu"])

    result = []
    for comp in sorted(by_comp.values(), key=lambda x: x["n_wins_vs_sasf"], reverse=True)[:top_n]:
        gaps = comp["gaps"]
        result.append({
            "rut_ganador":       comp["rut_ganador"],
            "nombre":            comp["nombre"][:100],
            "n_wins_vs_sasf":    comp["n_wins_vs_sasf"],
            "gap_avg_pct":       round(sum(gaps)/len(gaps), 1) if gaps else None,
            "n_productos_dist":  len(comp["productos"]),
        })
    return result


def compute_near_misses(rows: list[dict], names: dict, max_n: int = 50) -> list[dict]:
    """Licitaciones donde SASF perdió por menos del 10% de diferencia."""
    near = []
    for r in rows:
        if r.get("resultado_oferta") == "Seleccionada":
            continue
        gap = _safe_float(r.get("gap_porcentual"))
        if gap is None or gap < 0 or gap >= NEAR_MISS_THRESHOLD:
            continue
        onu = r.get("codigo_onu")
        near.append({
            "id_licitacion":     r.get("id_licitacion"),
            "codigo_onu":        onu,
            "nombre_producto":   names.get(onu, f"ONU {onu}")[:80] if onu else "?",
            "gap_pct":           round(gap, 2),
            "precio_sasf":       _safe_float(r.get("monto_neto_oferta")),
            "precio_ganador":    _safe_float(r.get("precio_ganador")),
            "proveedor_ganador": (r.get("proveedor_ganador") or "?")[:60],
            "fecha":             str(r.get("fecha_adjudicacion") or ""),
            "organismo":         (r.get("unidad_compra") or "?")[:60],
            "region":            r.get("region_unidad"),
        })
    # Ordenar por gap ascendente (los más cercanos primero)
    near.sort(key=lambda x: x["gap_pct"])
    return near[:max_n]


def compute_perdidas_no_precio(rows: list[dict], names: dict) -> list[dict]:
    """
    Casos donde SASF ofertó más barato que el ganador pero igual perdió.
    Señal de problemas técnicos, habilitación, o calidad de oferta.
    """
    result = []
    for r in rows:
        if r.get("resultado_oferta") == "Seleccionada":
            continue
        gap = _safe_float(r.get("gap_porcentual"))
        if gap is None or gap >= 0:
            continue  # No era más barato
        onu = r.get("codigo_onu")
        precio_sasf = _safe_float(r.get("monto_neto_oferta"))
        precio_gan  = _safe_float(r.get("precio_ganador"))
        ventaja_pct = abs(gap)
        result.append({
            "id_licitacion":     r.get("id_licitacion"),
            "codigo_onu":        onu,
            "nombre_producto":   names.get(onu, f"ONU {onu}")[:80] if onu else "?",
            "precio_sasf":       precio_sasf,
            "precio_ganador":    precio_gan,
            "ventaja_precio_pct": round(ventaja_pct, 1),
            "proveedor_ganador": (r.get("proveedor_ganador") or "?")[:60],
            "fecha":             str(r.get("fecha_adjudicacion") or ""),
            "organismo":         (r.get("unidad_compra") or "?")[:60],
            "posible_causa":     "Evaluación técnica, experiencia, garantías u otro criterio no-precio",
        })
    result.sort(key=lambda x: x["ventaja_precio_pct"], reverse=True)
    return result


def compute_chronic_losers(rows: list[dict], names: dict) -> list[dict]:
    """
    Códigos ONU donde SASF ha ofertado >= CHRONIC_MIN_BIDS veces sin ganar nunca.
    Separamos entre los que tienen gap alto (problema precio) vs gap bajo (otro problema).
    """
    by_onu: dict[int, dict] = {}
    for r in rows:
        onu = r.get("codigo_onu")
        if not onu:
            continue
        if onu not in by_onu:
            by_onu[onu] = {"n_bids": 0, "n_wins": 0, "gaps": [], "fechas": []}
        by_onu[onu]["n_bids"] += 1
        if r.get("resultado_oferta") == "Seleccionada":
            by_onu[onu]["n_wins"] += 1
        gap = _safe_float(r.get("gap_porcentual"))
        if gap is not None and gap >= 0:
            by_onu[onu]["gaps"].append(gap)
        if r.get("fecha_adjudicacion"):
            by_onu[onu]["fechas"].append(str(r["fecha_adjudicacion"]))

    result = []
    for onu, stats in by_onu.items():
        if stats["n_bids"] < CHRONIC_MIN_BIDS or stats["n_wins"] > 0:
            continue
        gaps = sorted(stats["gaps"])
        gap_med = round(gaps[len(gaps)//2], 1) if gaps else None
        gap_avg = round(sum(gaps)/len(gaps), 1) if gaps else None
        ultima_fecha = max(stats["fechas"]) if stats["fechas"] else None

        if gap_med is not None and gap_med > 30:
            accion = f"Reducir precio ≥{gap_med:.0f}% para ser competitivo"
            tipo = "PRECIO_ALTO"
        elif gap_med is not None and gap_med < 10:
            accion = "Revisar criterios técnicos/habilitación — el precio no es el problema"
            tipo = "NO_PRECIO"
        else:
            accion = "Evaluar si el margen del producto permite bajar a p25 del mercado"
            tipo = "MARGINAL"

        result.append({
            "codigo_onu":    onu,
            "nombre":        names.get(onu, f"ONU {onu}")[:80],
            "n_bids":        stats["n_bids"],
            "n_wins":        0,
            "gap_mediana":   gap_med,
            "gap_promedio":  gap_avg,
            "ultima_oferta": ultima_fecha,
            "tipo":          tipo,
            "accion":        accion,
        })
    result.sort(key=lambda x: x["n_bids"], reverse=True)
    return result


def compute_sweet_spots(rows: list[dict], names: dict) -> list[dict]:
    """
    Códigos ONU donde SASF tiene win rate > SWEET_SPOT_MIN_WINRATE y al menos N bids.
    Son las fortalezas — reforzar presencia.
    """
    by_onu: dict[int, dict] = {}
    for r in rows:
        onu = r.get("codigo_onu")
        if not onu:
            continue
        if onu not in by_onu:
            by_onu[onu] = {"n_bids": 0, "n_wins": 0, "gaps": [], "precios_sasf": []}
        by_onu[onu]["n_bids"] += 1
        if r.get("resultado_oferta") == "Seleccionada":
            by_onu[onu]["n_wins"] += 1
            p = _safe_float(r.get("monto_neto_oferta"))
            if p:
                by_onu[onu]["precios_sasf"].append(p)
        gap = _safe_float(r.get("gap_porcentual"))
        if gap is not None and gap >= 0:
            by_onu[onu]["gaps"].append(gap)

    result = []
    for onu, stats in by_onu.items():
        if stats["n_bids"] < SWEET_SPOT_MIN_BIDS:
            continue
        win_rate = stats["n_wins"] / stats["n_bids"]
        if win_rate < SWEET_SPOT_MIN_WINRATE:
            continue
        precio_win_avg = (
            round(sum(stats["precios_sasf"]) / len(stats["precios_sasf"]), 0)
            if stats["precios_sasf"] else None
        )
        result.append({
            "codigo_onu":     onu,
            "nombre":         names.get(onu, f"ONU {onu}")[:80],
            "n_bids":         stats["n_bids"],
            "n_wins":         stats["n_wins"],
            "win_rate_pct":   round(win_rate * 100, 1),
            "precio_win_avg": precio_win_avg,
            "accion":         "Fortaleza — buscar activamente licitaciones con este producto",
        })
    result.sort(key=lambda x: (x["win_rate_pct"], x["n_bids"]), reverse=True)
    return result


def compute_por_organismo(rows: list[dict]) -> list[dict]:
    """Win rate por organismo comprador."""
    by_org: dict[str, dict] = {}
    for r in rows:
        rut_org  = r.get("unidad_compra_rut") or "__sin_rut__"
        nombre   = r.get("unidad_compra") or rut_org
        if rut_org not in by_org:
            by_org[rut_org] = {
                "rut":    rut_org,
                "nombre": nombre,
                "n_bids": 0,
                "n_wins": 0,
                "gaps":   [],
            }
        by_org[rut_org]["n_bids"] += 1
        if r.get("resultado_oferta") == "Seleccionada":
            by_org[rut_org]["n_wins"] += 1
        gap = _safe_float(r.get("gap_porcentual"))
        if gap is not None and gap >= 0:
            by_org[rut_org]["gaps"].append(gap)

    result = []
    for stats in by_org.values():
        if stats["n_bids"] < 3:
            continue
        gaps = stats["gaps"]
        win_rate = stats["n_wins"] / stats["n_bids"]
        result.append({
            "rut":         stats["rut"],
            "nombre":      stats["nombre"][:80],
            "n_bids":      stats["n_bids"],
            "n_wins":      stats["n_wins"],
            "win_rate_pct": round(win_rate * 100, 1),
            "gap_avg_pct": round(sum(gaps)/len(gaps), 1) if gaps else None,
        })
    result.sort(key=lambda x: (x["win_rate_pct"], x["n_bids"]), reverse=True)
    return result


def compute_por_mes(rows: list[dict]) -> list[dict]:
    """Tendencia mensual de win rate."""
    by_mes: dict[str, dict] = {}
    for r in rows:
        mes = r.get("mes_proceso") or (
            str(r["fecha_adjudicacion"])[:7]
            if r.get("fecha_adjudicacion") else None
        )
        if not mes:
            continue
        if mes not in by_mes:
            by_mes[mes] = {"n_bids": 0, "n_wins": 0}
        by_mes[mes]["n_bids"] += 1
        if r.get("resultado_oferta") == "Seleccionada":
            by_mes[mes]["n_wins"] += 1

    result = []
    for mes in sorted(by_mes.keys()):
        s = by_mes[mes]
        result.append({
            "mes":         mes,
            "n_bids":      s["n_bids"],
            "n_wins":      s["n_wins"],
            "win_rate_pct": round(s["n_wins"] / s["n_bids"] * 100, 1) if s["n_bids"] else 0,
        })
    return result


def compute_alertas(
    resumen: dict,
    chronic: list[dict],
    sweet:   list[dict],
    near:    list[dict],
    no_precio: list[dict],
    competidores: list[dict],
) -> list[dict]:
    """
    Genera lista priorizada de alertas accionables.
    """
    alertas = []

    # 1. Win rate crítico
    if resumen["win_rate_pct"] < 5:
        alertas.append({
            "prioridad": "CRÍTICA",
            "tipo":      "WIN_RATE",
            "titulo":    f"Win rate global {resumen['win_rate_pct']}% — muy por debajo del mercado",
            "detalle":   (
                f"De {resumen['total_bids']} ofertas, solo {resumen['total_wins']} ganadas. "
                f"El gap promedio es {resumen.get('gap_promedio_pct', '?')}% sobre el ganador."
            ),
            "accion":    "Revisar política de precios en todos los productos. Usar p25 como precio base.",
        })

    # 2. Pérdidas no-precio (SASF era más barato pero perdió)
    if no_precio:
        alertas.append({
            "prioridad": "ALTA",
            "tipo":      "NO_PRECIO",
            "titulo":    f"{len(no_precio)} licitaciones perdidas aunque SASF ofertó más barato",
            "detalle":   (
                f"En {len(no_precio)} casos, el ganador tuvo un precio mayor al de SASF. "
                f"Posibles causas: evaluación técnica, experiencia acreditada, garantías, o error en oferta."
            ),
            "accion":    "Revisar requisitos técnicos y de habilitación en las licitaciones perdidas. "
                         "Verificar certificaciones y experiencia requeridas.",
        })

    # 3. Near misses: oportunidades perdidas por poco
    if len(near) >= 10:
        alertas.append({
            "prioridad": "ALTA",
            "tipo":      "NEAR_MISS",
            "titulo":    f"{len(near)} licitaciones perdidas por menos del 10% de diferencia",
            "detalle":   (
                f"Con una reducción mínima de precio, SASF habría ganado estas licitaciones. "
                f"La más cercana tuvo gap de {near[0]['gap_pct'] if near else '?'}%."
            ),
            "accion":    "Priorizar estos códigos ONU y organismos. Bajar precio en 10-15% para capturar.",
        })

    # 4. Competidor dominante
    if competidores:
        top = competidores[0]
        alertas.append({
            "prioridad": "MEDIA",
            "tipo":      "COMPETIDOR",
            "titulo":    f"{top['nombre']} es el rival #1 — gana {top['n_wins_vs_sasf']} veces a SASF",
            "detalle":   (
                f"Activo en {top['n_productos_dist']} categorías de productos SASF. "
                + (f"Gap promedio cuando gana: {top['gap_avg_pct']}%." if top.get('gap_avg_pct') else "")
            ),
            "accion":    "Investigar precios públicos de este proveedor en licitaciones adjudicadas.",
        })

    # 5. Chronic losers con muchos bids
    top_chronic = [c for c in chronic if c["tipo"] == "PRECIO_ALTO"][:3]
    if top_chronic:
        nombres = ", ".join(c["nombre"][:30] for c in top_chronic)
        alertas.append({
            "prioridad": "MEDIA",
            "tipo":      "CHRONIC_LOSER",
            "titulo":    f"{len(chronic)} productos sin ninguna victoria histórica",
            "detalle":   f"Más críticos: {nombres}.",
            "accion":    "Evaluar si el margen permite llegar al p25 del mercado. "
                         "Si no, considerar dejar de ofertar en estos productos.",
        })

    # 6. Sweet spots: reforzar fortalezas
    if sweet:
        nombres_ss = ", ".join(s["nombre"][:25] for s in sweet[:3])
        alertas.append({
            "prioridad": "OPORTUNIDAD",
            "tipo":      "SWEET_SPOT",
            "titulo":    f"{len(sweet)} productos donde SASF tiene ventaja competitiva",
            "detalle":   f"Win rate superior al 10% en: {nombres_ss}.",
            "accion":    "Buscar activamente más licitaciones de estos productos. "
                         "Considerar convenio marco en estas categorías.",
        })

    return alertas


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Calcula diagnóstico completo de pérdidas SASF"
    )
    parser.add_argument("--force", action="store_true",
                        help="Recomputa aunque ya exista diagnóstico")
    args = parser.parse_args()

    load_dotenv()
    supabase = get_client()

    # Verificar si ya existe (idempotencia)
    if not args.force:
        ex = (
            supabase.table("loss_diagnostics")
            .select("computed_at")
            .eq("rut_proveedor", SASF_RUT)
            .limit(1)
            .execute()
        )
        if ex.data:
            log.info(f"Diagnóstico ya existe (computed_at={ex.data[0]['computed_at']}). "
                     f"Usa --force para recomputar.")
            sys.exit(0)

    # Cargar datos
    rows  = load_ofertas(supabase)
    names = load_benchmark_names(supabase)

    if not rows:
        log.error("Sin datos en ofertas_sasf.")
        sys.exit(1)

    log.info("Computando diagnóstico...")

    resumen      = compute_resumen_global(rows)
    competidores = compute_top_competidores(rows, top_n=15)
    near_misses  = compute_near_misses(rows, names, max_n=50)
    no_precio    = compute_perdidas_no_precio(rows, names)
    chronic      = compute_chronic_losers(rows, names)
    sweet        = compute_sweet_spots(rows, names)
    por_org      = compute_por_organismo(rows)
    por_mes      = compute_por_mes(rows)
    alertas      = compute_alertas(resumen, chronic, sweet, near_misses, no_precio, competidores)

    log.info(f"  Resumen: {resumen['total_wins']} wins / {resumen['total_losses']} losses "
             f"({resumen['win_rate_pct']}% win rate)")
    log.info(f"  Near misses: {len(near_misses)}")
    log.info(f"  Perdidas no-precio: {len(no_precio)}")
    log.info(f"  Chronic losers: {len(chronic)}")
    log.info(f"  Sweet spots: {len(sweet)}")
    log.info(f"  Alertas generadas: {len(alertas)}")

    # Upsert
    payload = {
        "rut_proveedor":    SASF_RUT,
        "resumen_global":   resumen,
        "top_competidores": competidores,
        "near_misses":      near_misses,
        "perdidas_no_precio": no_precio,
        "chronic_losers":   chronic,
        "sweet_spots":      sweet,
        "por_organismo":    por_org,
        "por_mes":          por_mes,
        "alertas":          alertas,
    }

    supabase.table("loss_diagnostics").upsert(
        payload,
        on_conflict="rut_proveedor",
    ).execute()

    log.info("✅ Diagnóstico guardado en Supabase → loss_diagnostics")

    # Resumen ejecutivo
    print("\n" + "═" * 72)
    print("  LOSS DIAGNOSTICS — RESUMEN EJECUTIVO")
    print("═" * 72)
    print(f"  Total ofertas analizadas : {resumen['total_bids']}")
    print(f"  Wins                     : {resumen['total_wins']}  ({resumen['win_rate_pct']}%)")
    print(f"  Losses                   : {resumen['total_losses']}")
    print(f"  Gap promedio vs ganador  : {resumen.get('gap_promedio_pct', 'N/D')}%")
    print(f"  Gap mediana vs ganador   : {resumen.get('gap_mediana_pct', 'N/D')}%")
    print(f"  Near misses (<10% gap)   : {resumen['near_misses']}")
    print(f"  SASF más barato pero perdió: {resumen['sasf_mas_barato']}")
    print(f"\n  Alertas generadas        : {len(alertas)}")
    for a in alertas:
        print(f"  [{a['prioridad']:10}] {a['titulo'][:65]}")
    print("\n" + "═" * 72)
    print("  → Siguiente: python3 scripts/loss_report.py")
    print("═" * 72 + "\n")


if __name__ == "__main__":
    main()
