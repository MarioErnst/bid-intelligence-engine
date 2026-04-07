import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class Config:
    """Configuración centralizada del sistema"""
    
    def __init__(self):
        self.load_env()
        
    def load_env(self):
        """Carga variables de entorno desde .env"""
        try:
            load_dotenv()
            self.API_KEY = os.getenv('MERCADO_PUBLICO_API_KEY', '')
            self.MAX_RETRIES = int(os.getenv('API_MAX_RETRIES', 20))
            self.RETRY_DELAY = int(os.getenv('API_RETRY_DELAY', 5))
            self.PRICE_GAP_THRESHOLD = float(os.getenv('PRICE_GAP_THRESHOLD', 5.0))
            
            # Vertex AI Config
            self.PROJECT_ID = os.getenv('PROJECT_ID', 'licitaciones-486301')
            self.LOCATION = os.getenv('LOCATION', 'us-central1')
            # Usar un modelo robusto por defecto
            self.MODEL_NAME = os.getenv('MODEL_NAME', 'gemini-2.5-pro')
            
        except ImportError:
            logger.warning("python-dotenv no disponible. Usando configuración por defecto.")
            self.API_KEY = ''
            self.MAX_RETRIES = 20
            self.RETRY_DELAY = 5
            self.PRICE_GAP_THRESHOLD = 5.0
            self.PROJECT_ID = 'licitaciones-486301'
            self.LOCATION = 'us-central1'
            self.MODEL_NAME = 'gemini-2.5-pro'
    
    API_BASE_URL = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"
