import logging
from src.core.config import Config
from src.core.models import LicitacionPerdida

# Vertex AI
try:
    import vertexai
    from vertexai.generative_models import GenerativeModel
    VERTEX_AI_AVAILABLE = True
except ImportError:
    VERTEX_AI_AVAILABLE = False

logger = logging.getLogger(__name__)

class VertexAIClient:
    """Cliente para interactuar con Google Vertex AI (Gemini)"""
    
    def __init__(self, config: Config):
        self.config = config
        self.model = None
        self.init_error = None
        
        if VERTEX_AI_AVAILABLE:
            try:
                vertexai.init(project=self.config.PROJECT_ID, location=self.config.LOCATION)
                self.model = GenerativeModel(self.config.MODEL_NAME)
                logger.info(f"🧠 Vertex AI inicializado: {self.config.MODEL_NAME}")
            except Exception as e:
                self.init_error = str(e)
                logger.error(f"❌ Error inicializando Vertex AI: {e}")
                if "403" in str(e):
                    logger.warning("⚠️  IMPORTANTE: La API de Vertex AI no está habilitada en el proyecto.")
                    logger.warning(f"   Visite: https://console.developers.google.com/apis/api/aiplatform.googleapis.com/overview?project={self.config.PROJECT_ID}")
        else:
            logger.warning("⚠️  vertexai no disponible. Instalar con: pip install google-cloud-aiplatform")
    
    def analizar_derrota(self, licitacion: LicitacionPerdida) -> str:
        """
        Genera un análisis detallado usando Gemini
        """
        if not self.model:
            msg = "Análisis AI no disponible (Vertex AI no inicializado)."
            if self.init_error:
                msg += f" Error: {self.init_error}"
            return msg
            
        prompt = f"""
        Actúa como un experto auditor de licitaciones públicas (Contract Defense System).
        Analiza la siguiente licitación perdida y explica detalladamente por qué se perdió comparándola con el ganador.
        
        CONTEXTO DE LA LICITACIÓN:
        - ID Licitación: {licitacion.id_licitacion}
        - Producto/Servicio: {licitacion.producto_cliente}
        - Fecha: {licitacion.fecha_licitacion}
        - Cantidad: {licitacion.cantidad}
        
        MI OFERTA (CLIENTE):
        - Proveedor: {licitacion.rut_cliente}
        - Precio Unitario: ${licitacion.precio_oferta_cliente:,.0f} CLP
        
        GANADOR (COMPETENCIA):
        - Proveedor: {licitacion.proveedor_ganador} ({licitacion.rut_ganador})
        - Precio Unitario: ${licitacion.precio_ganador:,.0f} CLP
        
        INSTRUCCIONES PARA EL ANÁLISIS:
        1. Compara los precios y calcula la diferencia exacta (monto y porcentaje).
        2. Determina la causa raíz probable:
           - Si el ganador es más barato: ¿Es una diferencia marginal (<5%) o agresiva (>20%)? ¿Indica una estrategia de dumping o mayor eficiencia?
           - Si el ganador es más caro: ¿Por qué ganaron? (Factores técnicos, plazos, experiencia, garantías).
        3. Genera una recomendación estratégica accionable para la próxima vez.
        4. Sé directo, profesional y usa un tono de "Consultor Senior".
        5. Formato de salida: Texto plano estructurado en párrafos breves.
        """
        
        try:
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            logger.error(f"❌ Error generando análisis con Gemini: {e}")
            return f"Error al generar análisis AI: {str(e)}"
