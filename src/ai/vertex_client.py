import os
import logging
import time
import vertexai
from vertexai.generative_models import GenerativeModel, Part
from dotenv import load_dotenv
from ..models.data_models import LicitacionPerdida

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class VertexAIClient:
    """Cliente para interactuar con Google Vertex AI (Gemini)"""
    
    def __init__(self):
        self.project_id = os.getenv("PROJECT_ID")
        self.location = os.getenv("LOCATION", "us-central1")
        self.model_name = os.getenv("MODEL_NAME", "gemini-1.5-pro")
        self.model = None
        self.init_error = None
        
        try:
            vertexai.init(project=self.project_id, location=self.location)
            self.model = GenerativeModel(self.model_name)
            logger.info(f"🧠 Vertex AI inicializado: {self.model_name}")
        except Exception as e:
            self.init_error = str(e)
            logger.error(f"❌ Error inicializando Vertex AI: {e}")
            if "403" in str(e) or "404" in str(e):
                logger.warning("⚠️  IMPORTANTE: Verifique que la API de Vertex AI esté habilitada y el nombre del modelo sea correcto.")
    
    def analizar_derrota(self, licitacion: LicitacionPerdida, pdf_path: str = None) -> str:
        """
        Genera un análisis detallado usando Gemini, incorporando evidencia del PDF si existe.
        Soporta Multimodalidad (Lectura directa de PDF/Imágenes).
        """
        if not self.model:
            msg = "Análisis AI no disponible (Vertex AI no inicializado)."
            if self.init_error:
                msg += f" Error: {self.init_error}"
            return msg
            
        # Construcción del prompt
        prompt = f"""
        Actúa como un experto auditor de licitaciones públicas (Contract Defense System).
        Analiza la siguiente licitación perdida y explica detalladamente por qué se perdió.
        
        CONTEXTO DE LA LICITACIÓN:
        - ID Licitación: {licitacion.id_licitacion}
        - Producto/Servicio: {licitacion.producto_cliente}
        - Fecha: {licitacion.fecha_licitacion}
        - Cantidad: {licitacion.cantidad}
        - Estado Oficial: {licitacion.estado_licitacion}
        
        MI OFERTA (CLIENTE):
        - Proveedor: {licitacion.rut_cliente}
        - Precio Unitario: ${licitacion.precio_oferta_cliente:,.0f} CLP
        
        GANADOR (COMPETENCIA):
        - Proveedor: {licitacion.proveedor_ganador} ({licitacion.rut_ganador})
        - Precio Unitario: ${licitacion.precio_ganador if licitacion.precio_ganador else 0:,.0f} CLP
        """

        content_parts = [prompt]
        pdf_loaded = False

        # Intento de cargar PDF como Part (Multimodal)
        if pdf_path and os.path.exists(pdf_path):
            try:
                with open(pdf_path, "rb") as f:
                    pdf_data = f.read()
                
                pdf_part = Part.from_data(
                    mime_type="application/pdf",
                    data=pdf_data
                )
                content_parts.append(pdf_part)
                content_parts.append("""
                
                🔴 EVIDENCIA: Se adjunta el documento oficial (PDF) de la evaluación.
                El documento puede ser escaneado (imagen). Tu tarea es LEERLO COMPLETAMENTE, incluso si es una imagen, buscando tablas de puntajes y razones de rechazo.
                
                INSTRUCCIONES ADICIONALES (CRÍTICO):
                1. BASA TU ANÁLISIS EN EL PDF ADJUNTO. Busca tablas de puntaje, comentarios de la comisión y razones de rechazo.
                2. Si el PDF menciona puntajes técnicos, administrativos o económicos, CÍTALOS.
                3. Contrasta los puntajes obtenidos por mi oferta ({licitacion.rut_cliente}) vs el ganador ({licitacion.rut_ganador}).
                4. Si fui rechazado o inadmisible, explica la razón exacta citada en el informe.
                """)
                pdf_loaded = True
                logger.info(f"📄 PDF cargado en modo Multimodal: {os.path.basename(pdf_path)}")
            except Exception as e:
                logger.error(f"❌ Error cargando PDF multimodal: {e}. Se usará solo texto extraído si existe.")

        # Fallback a texto extraído si no se pudo cargar el PDF multimodal o no se pasó path
        if not pdf_loaded:
            if licitacion.evidencia_pdf:
                content_parts.append(f"""
                
                🔴 EVIDENCIA OFICIAL DEL INFORME DE EVALUACIÓN (Transcripción Texto):
                ------------------------------------------------------------------
                {licitacion.evidencia_pdf}
                ------------------------------------------------------------------
                
                INSTRUCCIONES: Analiza el texto anterior. Si parece incompleto, indícalo.
                """)
            else:
                content_parts.append("""
                ⚠️ NOTA: No se pudo obtener el Informe de Evaluación oficial. Realiza un análisis inferencial basado en los precios.
                """)

        content_parts.append("""
        
        ESTRUCTURA DEL REPORTE:
        1. 🏆 VEREDICTO: ¿Por qué ganaron ellos? (Precio, Técnica, Administrativa).
        2. ❌ MI FALLA: ¿Qué error específico cometí según el informe (si existe) o los datos?
        3. 💡 RECOMENDACIÓN: ¿Qué debo corregir para la próxima? (Sé muy específico).
        
        Formato: Texto plano, directo y profesional.
        """)
        
        max_retries = 5
        base_delay = 10
        
        for attempt in range(max_retries):
            try:
                response = self.model.generate_content(content_parts)
                return response.text
            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "Resource exhausted" in error_str:
                    wait_time = base_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"⚠️ Quota exceeded (429). Retrying in {wait_time}s... (Attempt {attempt+1}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ Error generando análisis con Gemini: {e}")
                    return f"Error al generar análisis AI: {error_str}"
        
        return "Error al generar análisis AI: Quota exceeded after max retries."
