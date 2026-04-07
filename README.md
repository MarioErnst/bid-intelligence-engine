# Bid Intelligence Engine 🇨🇱

Motor de inteligencia competitiva para licitaciones del Estado chileno (ChileCompra / Mercado Público).

Desarrollado para **SASF COMERCIAL LIMITADA** (RUT 76.930.423-1) — proveedor de insumos médicos.

---

## ¿Qué hace?

A partir de datos públicos de ChileCompra y el historial propio de SASF, el engine entrega **tres features accionables**:

### Feature 1 — Match Scoring
Escanea las licitaciones abiertas y calcula qué tan bien encaja el catálogo de SASF con cada una.

```
Score = 45% cobertura de ítems + 25% win rate histórico + 20% experiencia + 10% profundidad de mercado
```

Salida: ranking de oportunidades con recomendación ALTA / MEDIA / BAJA / SIN_MATCH.

### Feature 2 — Pricing Recommendations
Para cada licitación con score ALTA o MEDIA, calcula los precios óptimos de oferta por ítem.

Tres estrategias:
- 🔴 **AGRESIVA** — p25 del mercado × 0.90
- 🟡 **EQUILIBRADA** — p25 del mercado × 1.00 *(recomendada por defecto)*
- 🟢 **CONSERVADORA** — mediana del mercado

Usa el historial de gap de precios de SASF vs. ganadores reales para ajustar la estrategia.

### Feature 3 — Loss Diagnostics
Analiza el historial completo de pérdidas de SASF para identificar patrones accionables:
- Near misses (perdidas por < 10% de diferencia)
- Pérdidas no-precio (SASF era más barato pero igual perdió)
- Chronic losers (productos sin ninguna victoria)
- Sweet spots (fortalezas a reforzar)
- Ranking de competidores
- Win rate por organismo y tendencia mensual

---

## Stack

| Capa | Tecnología |
|---|---|
| DB | [Supabase](https://supabase.com) (PostgreSQL) |
| Datos públicos | ChileCompra Bulk CSVs + REST API |
| ETL | Python + Polars + Requests |
| Lenguaje | Python 3.9+ |

---

## Estructura

```
scripts/
├── etl_mercado_bulk.py          # Descarga y procesa CSVs masivos de ChileCompra
├── etl_sasf_from_bulk.py        # Extrae historial de ofertas SASF de los CSVs públicos
├── compute_benchmarks.py        # Calcula precios de referencia por código ONU
├── fetch_open_licitaciones.py   # Descarga licitaciones abiertas de la API
│
├── compute_match_scores.py      # Feature 1: scoring de match
├── match_report.py              # Feature 1: reporte de oportunidades
│
├── compute_pricing.py           # Feature 2: calcula precios recomendados
├── pricing_report.py            # Feature 2: reporte de precios por ítem
│
├── compute_loss_diagnostics.py  # Feature 3: diagnóstico de pérdidas
└── loss_report.py               # Feature 3: reporte de diagnóstico

src/db/
└── supabase_client.py           # Cliente Supabase singleton
```

---

## Setup

### 1. Clonar y configurar entorno

```bash
git clone https://github.com/tu-usuario/bid-intelligence-engine.git
cd bid-intelligence-engine
pip install -r requirements.txt
cp .env.example .env
# Edita .env con tus credenciales
```

### 2. Variables de entorno (`.env`)

```
SUPABASE_URL=https://xxxx.supabase.co
SUPABASE_KEY=your_key
MERCADO_PUBLICO_API_KEY=your_ticket
```

### 3. Base de datos

Aplica las migraciones desde el dashboard de Supabase o con el MCP. Las tablas necesarias son:

- `licitaciones_mercado` — licitaciones adjudicadas históricas
- `precios_benchmark` — precios de referencia por código ONU (UNSPSC)
- `ofertas_sasf` — historial de ofertas de SASF
- `productos_sasf` — catálogo de productos
- `licitaciones_abiertas` — licitaciones actualmente abiertas
- `match_scores` — scores de Feature 1
- `pricing_recommendations` — precios de Feature 2
- `loss_diagnostics` — diagnóstico de Feature 3

---

## Flujo de uso diario

```bash
# 1. Actualizar licitaciones abiertas (correr diariamente)
python3 scripts/fetch_open_licitaciones.py

# 2. Calcular scores de match
python3 scripts/compute_match_scores.py

# 3. Ver oportunidades ALTA prioridad
python3 scripts/match_report.py --rec ALTA

# 4. Calcular precios recomendados
python3 scripts/compute_pricing.py

# 5. Ver reporte de precios
python3 scripts/pricing_report.py --rec ALTA

# 6. Ver diagnóstico de pérdidas (mensual)
python3 scripts/compute_loss_diagnostics.py --force
python3 scripts/loss_report.py
```

---

## Datos

Los CSVs de descarga masiva de ChileCompra se descargan automáticamente desde:
`https://transparenciachilecompra.blob.core.windows.net/descargamasiva/`

El historial de SASF se extrae filtrando por `RutProveedor = 76930423-1` de los CSVs públicos — sin necesidad de exportar desde el portal.

---

## Contexto del negocio

SASF compite en insumos médicos UNSPSC categoría 42 (Medical Equipment). 
Con 1,284 ofertas analizadas (Ene 2025 – May 2026), win rate global: **4.2%**.

Gap mediano vs. ganadores: **78%** — SASF oferta ~78% por encima del precio ganador.

Las features 2 y 3 están diseñadas específicamente para atacar este problema.

---

*Built with Claude Code*
