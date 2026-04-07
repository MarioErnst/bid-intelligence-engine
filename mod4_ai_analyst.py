import json
import logging
import os
from src.utils.pdf_extractor import PdfExtractor
from src.ai.vertex_client import VertexAIClient
from src.models.data_models import LicitacionPerdida

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MOD4_AI_Analyst")

def run_module_4():
    """
    Módulo 4:
    1. Lee el output del Módulo 3.
    2. Para cada licitación con PDF descargado:
       - Extrae el texto.
       - Envía a Vertex AI para análisis de causa raíz.
    3. Guarda los resultados enriquecidos.
    """
    input_file = "data/step3_pdf_downloads.json"
    output_file = "data/step4_ai_analysis.json"
    
    if not os.path.exists(input_file):
        logger.error(f"Archivo de entrada no encontrado: {input_file}. Ejecute Módulo 3 primero.")
        return

    logger.info("--- 🧠 INICIANDO MÓDULO 4: ANÁLISIS CON IA ---")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        data_dicts = json.load(f)
        
    vertex = VertexAIClient()
    
    # Convertir dicts a objetos para el cliente Vertex (que espera Dataclass)
    # y luego volver a dicts
    
    processed_count = 0
    
    for i, item in enumerate(data_dicts):
        # Solo analizar si hay PDF y no hay análisis previo (o si hubo error)
        pdf_path = item.get('path_pdf_informe')
        has_analysis = item.get('analisis_ai') and "Error" not in item.get('analisis_ai')
        
        if pdf_path and os.path.exists(pdf_path) and not has_analysis:
            logger.info(f"[{i+1}/{len(data_dicts)}] Analizando {item['id_licitacion']}...")
            
            # 1. Extraer Texto
            texto_pdf = PdfExtractor.extract_text(pdf_path)
            item['evidencia_pdf'] = texto_pdf
            
            # 2. Crear objeto temporal para el cliente
            # Filtramos claves que no estén en el modelo para evitar errores
            valid_keys = LicitacionPerdida.__annotations__.keys()
            filtered_item = {k: v for k, v in item.items() if k in valid_keys}
            licit_obj = LicitacionPerdida(**filtered_item)
            
            # 3. Llamar a Vertex AI
            # Se pasa el path del PDF para habilitar capacidad Multimodal (OCR nativo de Gemini)
            analisis = vertex.analizar_derrota(licit_obj, pdf_path=pdf_path)
            item['analisis_ai'] = analisis
            processed_count += 1
            
            # Guardado incremental
            if processed_count % 2 == 0:
                 with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(data_dicts, f, ensure_ascii=False, indent=2)
                    
        elif pdf_path and has_analysis:
            logger.info(f"   ⏭️ Análisis ya existe para {item['id_licitacion']}")
        else:
            # Si no hay PDF, no hay mucho que analizar en profundidad, 
            # pero podríamos pedir un análisis basado solo en precios si se desea.
            # Por ahora, seguimos la instrucción de "analice esos informes pdf".
            pass

    # Guardado final
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data_dicts, f, ensure_ascii=False, indent=2)
            
    logger.info(f"🧠 Se generaron {processed_count} nuevos análisis.")
    logger.info(f"💾 Resultados guardados en {output_file}")
    logger.info("--- ✅ MÓDULO 4 FINALIZADO ---")

if __name__ == "__main__":
    run_module_4()
