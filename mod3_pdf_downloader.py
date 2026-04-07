import json
import logging
import os
from src.agents.pdf_downloader import PdfDownloader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MOD3_PDF_Downloader")

def run_module_3():
    """
    Módulo 3:
    1. Lee el output del Módulo 2.
    2. Filtra los casos donde 'motivo_perdida_preliminar' == 'OTRO' (Precio Cliente <= Precio Ganador).
    3. Descarga los PDFs de evaluación para esos casos.
    """
    input_file = "data/step2_price_comparison.json"
    output_file = "data/step3_pdf_downloads.json"
    
    if not os.path.exists(input_file):
        logger.error(f"Archivo de entrada no encontrado: {input_file}. Ejecute Módulo 2 primero.")
        return

    logger.info("--- 📥 INICIANDO MÓDULO 3: DESCARGA DE INFORMES ---")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    downloader = PdfDownloader()
    count_downloaded = 0
    
    try:
        for item in data:
            # Criterio: Descargar solo si perdimos por razones NO económicas (o si el precio nuestro era mejor/igual)
            # También verificamos que tengamos UrlActa
            should_download = (item.get('motivo_perdida_preliminar') == "OTRO") and item.get('url_acta')
            
            if should_download:
                logger.info(f"   🎯 Objetivo detectado: {item['id_licitacion']} (Url: {item['url_acta']})")
                
                # Verificar si ya tenemos el path (re-run support)
                existing_path = item.get('path_pdf_informe')
                if existing_path and os.path.exists(existing_path):
                     logger.info(f"      ✅ PDF ya existe: {existing_path}")
                else:
                    pdf_path = downloader.download_informe(item['url_acta'], item['id_licitacion'])
                    if pdf_path:
                        item['path_pdf_informe'] = pdf_path
                        count_downloaded += 1
                    else:
                        logger.warning(f"      ❌ Falló la descarga para {item['id_licitacion']}")
            else:
                if not item.get('url_acta'):
                    logger.debug(f"   ⏭️ Saltando {item['id_licitacion']}: Sin UrlActa")
                else:
                    logger.debug(f"   ⏭️ Saltando {item['id_licitacion']}: Perdido por Precio (No requiere análisis PDF)")

    except KeyboardInterrupt:
        logger.warning("🛑 Interrumpido por usuario.")
    finally:
        downloader.close()
        
        # Guardar resultados actualizados
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"📥 Se descargaron {count_downloaded} nuevos PDFs.")
        logger.info(f"💾 Resultados guardados en {output_file}")
        logger.info("--- ✅ MÓDULO 3 FINALIZADO ---")

if __name__ == "__main__":
    run_module_3()
