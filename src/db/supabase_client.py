"""
Cliente Supabase compartido para todos los scripts ETL.
Singleton — se inicializa una sola vez por proceso.
"""

import os
from typing import Optional
from supabase import create_client, Client
from dotenv import load_dotenv

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
