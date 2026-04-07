import json
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MOD2_Price_Comparator")

def run_module_2():
    """
    Módulo 2:
    1. Lee el output del Módulo 1 (JSON con datos API).
    2. Compara precio cliente vs precio ganador.
    3. Etiqueta cada caso: 'PRECIO' (ganador más barato) u 'OTRO' (ganador igual o más caro).
    """
    input_file = "data/step1_api_results.json"
    output_file = "data/step2_price_comparison.json"
    
    if not os.path.exists(input_file):
        logger.error(f"Archivo de entrada no encontrado: {input_file}. Ejecute Módulo 1 primero.")
        return

    logger.info("--- ⚖️ INICIANDO MÓDULO 2: COMPARACIÓN DE PRECIOS ---")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    logger.info(f"📋 Procesando {len(data)} registros...")
    
    stats = {"PRECIO": 0, "OTRO": 0, "DESCONOCIDO": 0}
    
    for item in data:
        precio_cliente = item.get('precio_oferta_cliente')
        precio_ganador = item.get('precio_ganador')
        
        # Inicializar campo de motivo
        motivo = "DESCONOCIDO"
        
        if precio_ganador is not None and precio_cliente is not None:
            # Asegurar que sean números
            try:
                p_cli = float(precio_cliente)
                p_gan = float(precio_ganador)
                
                # Calcular GAP
                gap = p_cli - p_gan
                item['gap_monetario'] = gap
                item['gap_porcentual'] = (gap / p_gan * 100) if p_gan > 0 else 0
                
                if p_gan < p_cli:
                    motivo = "PRECIO"
                    logger.info(f"   📉 {item['id_licitacion']}: Perdido por PRECIO (Ganador ${p_gan:,.0f} vs Cliente ${p_cli:,.0f})")
                else:
                    motivo = "OTRO"
                    logger.info(f"   🔍 {item['id_licitacion']}: Perdido por OTRO motivo (Ganador ${p_gan:,.0f} >= Cliente ${p_cli:,.0f}) -> INVESTIGAR")
            except ValueError:
                logger.warning(f"   ⚠️ Error de conversión de precios para {item['id_licitacion']}")
        else:
            logger.info(f"   ❓ {item['id_licitacion']}: Datos incompletos para comparación.")

        item['motivo_perdida_preliminar'] = motivo
        stats[motivo] += 1

    # Guardar resultados
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        
    logger.info(f"📊 Estadísticas: {stats}")
    logger.info(f"💾 Resultados guardados en {output_file}")
    logger.info("--- ✅ MÓDULO 2 FINALIZADO ---")

if __name__ == "__main__":
    run_module_2()
