import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes globales del dominio
# ---------------------------------------------------------------------------

# RUT oficial de SASF Comercial Limitada (sin puntos, con guión)
SASF_RUT: str = "76930423-1"

# Rango UNSPSC categoría 42 — Equipos médicos y de laboratorio
UNSPSC42_MIN: int = 42_000_000
UNSPSC42_MAX: int = 42_999_999

# ---------------------------------------------------------------------------
# Feature 1 — Match Scoring (compute_match_scores.py)
# ---------------------------------------------------------------------------

# Pesos del scoring total (deben sumar 1.0)
W_MATCH: float       = 0.45   # cobertura del catálogo SASF
W_WIN_RATE: float    = 0.25   # tasa de éxito histórica bayesiana
W_EXPERIENCIA: float = 0.20   # experiencia acumulada (log-scale)
W_MERCADO: float     = 0.10   # profundidad del mercado de benchmark

# Umbrales de recomendación (sobre score 0–100)
THRESH_ALTA: int  = 60
THRESH_MEDIA: int = 35
THRESH_BAJA: int  = 15

# ---------------------------------------------------------------------------
# Feature 2 — Pricing Recommendations (compute_pricing.py)
# ---------------------------------------------------------------------------

FACTOR_AGRESIVO: float    = 0.90   # p25 × 0.90 → estrategia agresiva
FACTOR_EQUILIBRADO: float = 1.00   # p25 × 1.00 → estrategia equilibrada
# Estrategia conservadora usa la mediana directamente

GAP_UMBRAL_AGRESIVO: float    = 40.0   # % gap para recomendar agresiva
GAP_UMBRAL_EQUILIBRADO: float = 10.0   # % gap para recomendar equilibrada
WIN_RATE_CONSERVADOR: float   = 0.15   # win rate > 15% → conservadora
MIN_BIDS_PARA_ESTRATEGIA: int = 3      # mínimo bids para usar gap histórico

# ---------------------------------------------------------------------------
# Feature 3 — Loss Diagnostics (compute_loss_diagnostics.py)
# ---------------------------------------------------------------------------

NEAR_MISS_THRESHOLD: float    = 10.0   # gap < 10% → casi ganó
NO_PRECIO_THRESHOLD: float    = 0.0    # gap < 0   → SASF era más barato
CHRONIC_MIN_BIDS: int         = 5      # mínimo bids para "chronic loser"
SWEET_SPOT_MIN_BIDS: int      = 3      # mínimo bids para "sweet spot"
SWEET_SPOT_MIN_WINRATE: float = 0.10   # win rate mínimo para sweet spot

# ---------------------------------------------------------------------------
# Configuración dinámica (desde .env)
# ---------------------------------------------------------------------------

class Config:
    """Configuración centralizada del sistema."""

    def __init__(self):
        self.load_env()

    def load_env(self):
        """Carga variables de entorno desde .env."""
        try:
            load_dotenv()
            self.API_KEY            = os.getenv("MERCADO_PUBLICO_API_KEY", "")
            self.MAX_RETRIES        = int(os.getenv("API_MAX_RETRIES", 20))
            self.RETRY_DELAY        = int(os.getenv("API_RETRY_DELAY", 5))
            self.PRICE_GAP_THRESHOLD = float(os.getenv("PRICE_GAP_THRESHOLD", 5.0))

            # Vertex AI
            self.PROJECT_ID  = os.getenv("PROJECT_ID", "licitaciones-486301")
            self.LOCATION    = os.getenv("LOCATION", "us-central1")
            self.MODEL_NAME  = os.getenv("MODEL_NAME", "gemini-2.5-pro")

        except ImportError:
            logger.warning("python-dotenv no disponible. Usando configuración por defecto.")
            self.API_KEY             = ""
            self.MAX_RETRIES         = 20
            self.RETRY_DELAY         = 5
            self.PRICE_GAP_THRESHOLD = 5.0
            self.PROJECT_ID          = "licitaciones-486301"
            self.LOCATION            = "us-central1"
            self.MODEL_NAME          = "gemini-2.5-pro"

    API_BASE_URL = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"
