import os
import logging
import json
from dotenv import load_dotenv
from src.utils.data_loader import DataLoader
from src.api.mercado_publico import MercadoPublicoAPI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MOD1_API_Fetcher")

def run_module_1():
    """
    Módulo 1:
    1. Lee el Excel de licitaciones perdidas.
    2. Consulta la API de Mercado Público para cada ID.
    3. Guarda la data enriquecida (con UrlActa y datos del ganador) en un JSON.
    """
    load_dotenv()
    
    excel_path = "07LicitacionesEne.xlsx"
    output_file = "data/step1_api_results.json"
    
    if not os.path.exists(excel_path):
        logger.error(f"Archivo Excel no encontrado: {excel_path}")
        return

    # 1. Cargar Datos del Excel
    logger.info("--- 🚀 INICIANDO MÓDULO 1: EXTRACCIÓN API ---")
    licitaciones = DataLoader.cargar_datos_excel(excel_path)
    logger.info(f"📋 Se cargaron {len(licitaciones)} licitaciones del Excel.")

    api = MercadoPublicoAPI()
    results = []
    
    # 2. Consultar API
    for i, licit in enumerate(licitaciones):
        logger.info(f"[{i+1}/{len(licitaciones)}] Consultando API para {licit.id_licitacion}...")
        
        api_data = api.consultar_licitacion(licit.id_licitacion)
        
        if api_data:
            datos_ganador = api.extraer_datos_ganador(
                api_data, 
                licit.producto_cliente,
                codigo_producto_onu=licit.codigo_producto_onu
            )
            if datos_ganador:
                licit.estado_licitacion = datos_ganador.get('estado_licitacion')
                licit.url_acta = datos_ganador.get('url_acta')
                licit.precio_ganador = datos_ganador.get('precio_ganador')
                licit.proveedor_ganador = datos_ganador.get('proveedor_ganador')
                licit.rut_ganador = datos_ganador.get('rut_ganador')
            else:
                logger.warning(f"   ⚠️ No se encontraron datos del ganador para {licit.id_licitacion}")
        else:
            logger.error(f"   ❌ Falló la consulta API para {licit.id_licitacion}")
            licit.causa_derrota = "ERROR_API"

        # Convertir a dict para guardar
        results.append(vars(licit))

    # 3. Guardar Resultados
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    logger.info(f"💾 Resultados guardados en {output_file}")
    logger.info("--- ✅ MÓDULO 1 FINALIZADO ---")

if __name__ == "__main__":
    run_module_1()
