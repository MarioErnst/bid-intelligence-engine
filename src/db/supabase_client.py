"""
Cliente Supabase compartido para todos los scripts ETL.
Singleton — se inicializa una sola vez por proceso.
"""

import logging
import os
import time
from typing import Optional

from supabase import create_client, Client
from dotenv import load_dotenv

log = logging.getLogger(__name__)

_client: Optional[Client] = None


def get_client() -> Client:
    """Devuelve el cliente Supabase, inicializándolo si es necesario."""
    global _client
    if _client is None:
        load_dotenv()
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        if not url or not key:
            raise EnvironmentError(
                "Faltan variables de entorno SUPABASE_URL y/o SUPABASE_KEY. "
                "Revisa tu archivo .env"
            )
        _client = create_client(url, key)
    return _client


def safe_upsert(
    supabase,
    table: str,
    rows: list,
    on_conflict: str,
    batch_size: int = 300,
) -> int:
    """
    Upserta filas en lotes con reintentos exponenciales.

    Retorna el total de filas enviadas.
    Lanza excepción si un lote falla tras 3 intentos.
    """
    if not rows:
        return 0

    total = 0
    n_batches = (len(rows) + batch_size - 1) // batch_size

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        batch_num = i // batch_size + 1

        for attempt in range(1, 4):
            try:
                supabase.table(table).upsert(
                    batch, on_conflict=on_conflict
                ).execute()
                total += len(batch)
                if n_batches > 1:
                    log.info(f"  [{table}] Upserted {total}/{len(rows)}")
                break
            except Exception as exc:
                if attempt == 3:
                    log.error(
                        f"  ❌ [{table}] Upsert falló tras 3 intentos "
                        f"(batch {batch_num}/{n_batches}): {exc}"
                    )
                    raise
                wait = 2 ** attempt
                log.warning(
                    f"  [{table}] Reintento {attempt}/3 en {wait}s: {exc}"
                )
                time.sleep(wait)

    return total
