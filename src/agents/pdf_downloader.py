import os
import time
import glob
import logging
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class PdfDownloader:
    def __init__(self):
        self.download_path = os.getenv("PDF_STORAGE_PATH", "./pdf_reports")
        self.headless = os.getenv("SELENIUM_HEADLESS", "true").lower() == "true"
        
        # Ensure download directory exists
        if not os.path.isabs(self.download_path):
            self.download_path = os.path.abspath(self.download_path)
        os.makedirs(self.download_path, exist_ok=True)
        
        self.playwright = None
        self.browser = None
        self._start_browser()

    def _start_browser(self):
        try:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(headless=self.headless)
            logger.info("🌐 Playwright browser started.")
        except Exception as e:
            logger.error(f"❌ Error starting Playwright: {e}")
            raise

    def download_informe(self, url_acta: str, id_licitacion: str) -> str:
        """
        Navigates to the UrlActa using Playwright, finds the 'INFORME DE EVALUACION' button,
        downloads the PDF, and renames it.
        Returns the full path to the downloaded file.
        """
        if not url_acta:
            logger.warning(f"[{id_licitacion}] No UrlActa provided.")
            return ""

        context = None
        page = None
        try:
            # Create a new context/page for each download to ensure clean state
            context = self.browser.new_context(
                accept_downloads=True,
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            
            logger.info(f"[{id_licitacion}] Navigating to {url_acta}...")
            
            # Retry logic for navigation
            max_nav_retries = 3
            nav_success = False
            for attempt in range(max_nav_retries):
                try:
                    # Increased timeout to 120s
                    page.goto(url_acta, timeout=120000, wait_until="domcontentloaded")
                    nav_success = True
                    break
                except Exception as e:
                    logger.warning(f"[{id_licitacion}] Navigation attempt {attempt+1}/{max_nav_retries} failed: {e}")
                    time.sleep(5)
            
            if not nav_success:
                logger.error(f"[{id_licitacion}] Failed to navigate after {max_nav_retries} attempts.")
                return ""

            time.sleep(3)
            
            download_obj = None

            # --- Helper function to attempt download ---
            def attempt_download(action_name, action_func):
                logger.info(f"[{id_licitacion}] Attempting Strategy: {action_name}")
                try:
                    with page.expect_download(timeout=15000) as download_info:
                        action_func()
                    return download_info.value
                except PlaywrightTimeoutError:
                    logger.warning(f"[{id_licitacion}] Strategy '{action_name}' timed out waiting for download.")
                    return None
                except Exception as e:
                    logger.warning(f"[{id_licitacion}] Strategy '{action_name}' failed with error: {e}")
                    return None

            # --- Strategy 1: 'Ver Anexo' Button (Targeting INFORME DE EVALUACION) ---
            # Busca específicamente filas que contengan "INFORME" y "EVALUACION" y hace click en la lupa
            
            # 1. Primero encontramos el botón (fuera del bloque de descarga para evitar timeouts/conflictos)
            target_lupa = None
            try:
                rows = page.locator("tr")
                count = rows.count()
                for i in range(count):
                    row = rows.nth(i)
                    # Usamos una espera corta o verificamos visibilidad para evitar errores
                    if not row.is_visible():
                        continue
                        
                    text = row.inner_text().upper()
                    
                    # Lógica de filtrado más robusta
                    # Buscamos variantes de "Informe de Evaluación"
                    has_evaluacion = "EVALUA" in text  # Cubre EVALUACION, EVALUACIÓN
                    has_informe = "INFORME" in text
                    has_acta = "ACTA" in text
                    has_comision = "COMISION" in text or "COMISIÓN" in text
                    
                    # Es relevante si menciona Informe/Acta + Evaluación/Comisión
                    is_relevant = (has_informe and has_evaluacion) or \
                                  (has_acta and has_evaluacion) or \
                                  (has_informe and has_comision)
                    
                    # Excluir documentos irrelevantes que suelen tener lupa
                    is_trash = "DECLARACION" in text or "CONFLICTO" in text or "GARANTIA" in text or "JURADA" in text
                    
                    if is_relevant and not is_trash:
                        # Verificar si tiene botón de lupa/ver
                        lupa = row.locator("input[src*='ver.gif']").first
                        if not lupa.count():
                             lupa = row.locator("input[title='Ver Anexo']").first
                        if not lupa.count():
                             lupa = row.locator("input[alt='Ver']").first
                             
                        if lupa.count() > 0 and lupa.is_visible():
                            target_lupa = lupa
                            logger.info(f"[{id_licitacion}] Fila encontrada (MATCH): {text[:60]}...")
                            break
            except Exception as e:
                logger.warning(f"[{id_licitacion}] Error buscando fila: {e}")

            # 2. Ejecutar la descarga si encontramos el botón
            if target_lupa:
                def click_lupa():
                    target_lupa.click()
                
                download_obj = attempt_download("Lupa Informe Evaluación", click_lupa)
            else:
                logger.warning(f"[{id_licitacion}] No se encontró fila específica con 'INFORME DE EVALUACION' y lupa.")
                download_obj = None

            # --- Strategy 2: Fallback General (Cualquier 'Ver Anexo') ---
            # DESHABILITADO: El usuario indicó que descargar archivos incorrectos es peor que nada.
            # Solo descargamos si encontramos algo relevante en la estrategia 1.
            if not download_obj:
                logger.warning(f"[{id_licitacion}] No se encontró documento relevante. Omitiendo descarga genérica para evitar basura.")


            # --- Final Processing ---
            if download_obj:
                logger.info(f"[{id_licitacion}] Download started: {download_obj.suggested_filename}")
                new_filename = f"{id_licitacion}_INFORME_EVALUACION.pdf"
                final_path = os.path.join(self.download_path, new_filename)
                
                if os.path.exists(final_path):
                    os.remove(final_path)
                    
                download_obj.save_as(final_path)
                
                # Verify PDF integrity
                is_valid = False
                try:
                    with open(final_path, 'rb') as f:
                        header = f.read(5)
                        if header.startswith(b'%PDF-'):
                            is_valid = True
                except Exception:
                    pass
                
                if is_valid:
                    logger.info(f"[{id_licitacion}] Saved valid PDF to {final_path}")
                    return final_path
                else:
                    logger.error(f"[{id_licitacion}] Downloaded file is not a valid PDF. Deleting...")
                    if os.path.exists(final_path):
                        os.remove(final_path)
                    return ""
            else:
                logger.error(f"[{id_licitacion}] All download strategies failed.")
                # Take screenshot for debugging
                try:
                    page.screenshot(path=f"error_{id_licitacion}.png")
                except:
                    pass
                return ""

        except Exception as e:
            logger.error(f"[{id_licitacion}] Error in Playwright session: {e}")
            return ""
        finally:
            if context:
                context.close()

    def close(self):
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        logger.info("🛑 Playwright closed.")
