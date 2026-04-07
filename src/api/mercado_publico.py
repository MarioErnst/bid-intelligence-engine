import requests
import time
import logging
import os
from typing import Optional, Dict
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class MercadoPublicoAPI:
    """Cliente para interactuar con la API de Mercado Público"""
    
    BASE_URL = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"
    
    def __init__(self):
        self.api_key = os.getenv("MERCADO_PUBLICO_API_KEY")
        self.max_retries = int(os.getenv("API_MAX_RETRIES", 3))
        self.retry_delay = int(os.getenv("API_RETRY_DELAY", 2))
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Connection': 'close'
        })
    
    def consultar_licitacion(self, id_licitacion: str) -> Optional[Dict]:
        """
        Consulta la API para obtener datos oficiales de una licitación
        """
        if not self.api_key:
            logger.warning("⚠️  API_KEY no configurada en .env")
            return None
        
        url = f"{self.BASE_URL}?codigo={id_licitacion}&ticket={self.api_key}"
        
        for intento in range(1, self.max_retries + 1):
            try:
                logger.info(f"   🔍 Consultando API: {id_licitacion} (intento {intento}/{self.max_retries})")
                response = self.session.get(url, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Verificar error de concurrencia de la API
                    if data.get('Codigo') == 10500:
                         wait_time = min(self.retry_delay * (1.5 ** (intento - 1)), 60)
                         logger.warning(f"   ⏳ API saturada (10500). Esperando {wait_time:.1f}s...")
                         time.sleep(wait_time)
                         continue
                         
                    return data
                elif response.status_code == 404:
                    logger.warning(f"   ⚠️  Licitación {id_licitacion} no encontrada en API")
                    return None
                elif response.status_code >= 500:
                    wait_time = min(self.retry_delay * (1.5 ** (intento - 1)), 60)
                    logger.warning(f"   ⚠️  Error Servidor {response.status_code}. Reintentando en {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.warning(f"   ⚠️  Error HTTP {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"   ❌ Error en petición API: {e}")
                
            if intento < self.max_retries:
                wait_time = min(self.retry_delay * (1.5 ** (intento - 1)), 60)
                time.sleep(wait_time)
        
        logger.error(f"   ❌ Falló después de {self.max_retries} intentos: {id_licitacion}")
        return None
    
    def extraer_datos_ganador(self, json_response: Dict, nombre_producto: str, codigo_producto_onu: Optional[int] = None) -> Optional[Dict]:
        """
        Busca el ítem específico y extrae datos del ganador y UrlActa.
        Prioriza búsqueda por CódigoProductoONU si existe.
        """
        try:
            if 'Listado' not in json_response or not json_response['Listado']:
                return None
            
            licitacion_data = json_response['Listado'][0]
            estado_licitacion = licitacion_data.get('Estado', 'Desconocido')
            
            # Buscar UrlActa en varios lugares
            url_acta = licitacion_data.get('Adjudicacion', {}).get('UrlActa')
            if not url_acta:
                url_acta = licitacion_data.get('UrlActa') # Sometimes it's at root?
            
            items = licitacion_data.get('Items', {}).get('Listado', [])
            
            item_seleccionado = None
            
            # 1. Búsqueda EXACTA por CódigoProductoONU (Prioridad 1)
            if codigo_producto_onu:
                for item in items:
                    # El API a veces devuelve CodigoProducto como int o string
                    try:
                        if int(item.get('CodigoProducto', 0)) == int(codigo_producto_onu):
                            item_seleccionado = item
                            break
                    except:
                        pass
            
            # 2. Búsqueda exacta/parcial por nombre (Fallback)
            if not item_seleccionado:
                for item in items:
                    nombre_item = item.get('Nombre', '').lower()
                    if nombre_producto.lower() in nombre_item or nombre_item in nombre_producto.lower():
                        item_seleccionado = item
                        break
            
            # 3. (OPCIONAL) Si no encuentra, y hay items adjudicados, tomar el primero
            # SE ELIMINA PARA EVITAR FALSOS POSITIVOS. Si no hay match de nombre ni código, mejor no comparar.
            # if not item_seleccionado:
            #     for item in items:
            #         if item.get('Adjudicacion'):
            #             item_seleccionado = item
            #             break
            
            # Construir resultado
            result = {
                'estado_licitacion': estado_licitacion,
                'url_acta': url_acta,
                'precio_ganador': 0.0,
                'proveedor_ganador': '',
                'rut_ganador': '',
                'cantidad': 0.0
            }
            
            if item_seleccionado:
                adjudicacion = item_seleccionado.get('Adjudicacion', {})
                if adjudicacion:
                    result.update({
                        'precio_ganador': float(adjudicacion.get('MontoUnitario', 0)),
                        'proveedor_ganador': adjudicacion.get('NombreProveedor', ''),
                        'rut_ganador': adjudicacion.get('RutProveedor', ''),
                        'cantidad': float(item_seleccionado.get('Cantidad', 1))
                    })
                    
                    # A veces la UrlActa está a nivel de item (menos común pero posible)
                    if not result['url_acta']:
                         result['url_acta'] = adjudicacion.get('UrlActa')

            return result
                    
        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"   ❌ Error parseando JSON de API: {e}")
        
        return None
