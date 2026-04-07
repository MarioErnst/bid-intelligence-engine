"""
setup_db.py — Crea las 4 tablas en Supabase (idempotente).

Uso:
    python scripts/setup_db.py

Requiere en .env:
    DATABASE_URL=postgresql://postgres:[password]@db.[ref].supabase.co:5432/postgres
"""

import os
import sys
import logging
import psycopg2
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

SQL_PRODUCTOS_SASF = """
CREATE TABLE IF NOT EXISTS productos_sasf (
    id              SERIAL PRIMARY KEY,
    codigo_onu      BIGINT NOT NULL,
    nombre_producto TEXT   NOT NULL,
    primera_vez_visto DATE,
    ultima_vez_visto  DATE,
    fuente_excel    TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_productos_sasf UNIQUE (codigo_onu, nombre_producto)
);
CREATE INDEX IF NOT EXISTS idx_productos_sasf_codigo ON productos_sasf (codigo_onu);
"""

SQL_OFERTAS_SASF = """
CREATE TABLE IF NOT EXISTS ofertas_sasf (
    id                  SERIAL PRIMARY KEY,
    id_licitacion       TEXT        NOT NULL,
    codigo_onu          BIGINT,
    fecha_adjudicacion  DATE,
    rut_proveedor_sasf  TEXT        NOT NULL,
    nombre_item         TEXT,
    monto_neto_oferta   NUMERIC(18,2),
    cantidad_oferta     NUMERIC(18,4),
    resultado_oferta    TEXT,
    unidad_compra       TEXT,
    unidad_compra_rut   TEXT,
    sector              TEXT,
    region_unidad       TEXT,
    precio_ganador      NUMERIC(18,2),
    proveedor_ganador   TEXT,
    rut_ganador         TEXT,
    estado_licitacion   TEXT,
    url_acta            TEXT,
    gap_monetario       NUMERIC(18,2),
    gap_porcentual      NUMERIC(10,4),
    motivo_perdida      TEXT,
    analisis_ai         TEXT,
    path_pdf_informe    TEXT,
    mes_proceso         TEXT,
    fuente_excel        TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_ofertas_sasf UNIQUE (id_licitacion, codigo_onu, fecha_adjudicacion)
);
CREATE INDEX IF NOT EXISTS idx_ofertas_sasf_rut    ON ofertas_sasf (rut_proveedor_sasf);
CREATE INDEX IF NOT EXISTS idx_ofertas_sasf_codigo ON ofertas_sasf (codigo_onu);
CREATE INDEX IF NOT EXISTS idx_ofertas_sasf_motivo ON ofertas_sasf (motivo_perdida);
CREATE INDEX IF NOT EXISTS idx_ofertas_sasf_fecha  ON ofertas_sasf (fecha_adjudicacion);
"""

SQL_LICITACIONES_MERCADO = """
CREATE TABLE IF NOT EXISTS licitaciones_mercado (
    id                       SERIAL PRIMARY KEY,
    codigo_licitacion        TEXT          NOT NULL,
    nombre_licitacion        TEXT,
    estado                   TEXT,
    tipo_licitacion          TEXT,
    fecha_publicacion        DATE,
    fecha_cierre             DATE,
    fecha_adjudicacion       DATE,
    numero_item              INTEGER,
    nombre_item              TEXT,
    codigo_onu               BIGINT,
    descripcion_onu          TEXT,
    cantidad                 NUMERIC(18,4),
    unidad_medida            TEXT,
    precio_unitario_ganador  NUMERIC(18,2),
    monto_total_adjudicado   NUMERIC(18,2),
    rut_ganador              TEXT,
    nombre_ganador           TEXT,
    rut_unidad_compradora    TEXT,
    nombre_unidad_compradora TEXT,
    region                   TEXT,
    sector                   TEXT,
    mes_proceso              TEXT          NOT NULL,
    fuente_zip               TEXT,
    created_at               TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_licitaciones_mercado UNIQUE (codigo_licitacion, numero_item, mes_proceso)
);
CREATE INDEX IF NOT EXISTS idx_licit_mercado_codigo_onu ON licitaciones_mercado (codigo_onu);
CREATE INDEX IF NOT EXISTS idx_licit_mercado_mes        ON licitaciones_mercado (mes_proceso);
CREATE INDEX IF NOT EXISTS idx_licit_mercado_fecha      ON licitaciones_mercado (fecha_adjudicacion);
CREATE INDEX IF NOT EXISTS idx_licit_mercado_ganador    ON licitaciones_mercado (rut_ganador);
CREATE INDEX IF NOT EXISTS idx_licit_mercado_unspsc42
    ON licitaciones_mercado (codigo_onu, fecha_adjudicacion)
    WHERE codigo_onu >= 42000000 AND codigo_onu < 43000000;
"""

SQL_PRECIOS_BENCHMARK = """
CREATE TABLE IF NOT EXISTS precios_benchmark (
    id                  SERIAL PRIMARY KEY,
    codigo_onu          BIGINT NOT NULL,
    descripcion_onu     TEXT,
    precio_mediana      NUMERIC(18,2),
    precio_p25          NUMERIC(18,2),
    precio_p75          NUMERIC(18,2),
    precio_min          NUMERIC(18,2),
    precio_max          NUMERIC(18,2),
    precio_promedio     NUMERIC(18,2),
    desviacion_estandar NUMERIC(18,2),
    n_observaciones     INTEGER,
    n_licitaciones      INTEGER,
    n_proveedores       INTEGER,
    fecha_desde         DATE,
    fecha_hasta         DATE,
    computed_at         TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_benchmark_codigo UNIQUE (codigo_onu)
);
CREATE INDEX IF NOT EXISTS idx_benchmark_codigo ON precios_benchmark (codigo_onu);
"""

TABLES = [
    ("productos_sasf",       SQL_PRODUCTOS_SASF),
    ("ofertas_sasf",         SQL_OFERTAS_SASF),
    ("licitaciones_mercado", SQL_LICITACIONES_MERCADO),
    ("precios_benchmark",    SQL_PRECIOS_BENCHMARK),
]

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def setup_database():
    load_dotenv()
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log.error("Falta DATABASE_URL en .env")
        sys.exit(1)

    log.info("Conectando a Supabase PostgreSQL...")
    conn = psycopg2.connect(db_url)
    conn.autocommit = True

    with conn.cursor() as cur:
        for name, ddl in TABLES:
            log.info(f"Creando tabla '{name}'...")
            cur.execute(ddl)
            log.info(f"  ✓ '{name}' lista")

    conn.close()
    log.info("Setup completado. Las 4 tablas están listas en Supabase.")


if __name__ == "__main__":
    setup_database()
