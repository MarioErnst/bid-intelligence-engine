import PyPDF2
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PdfExtractor:
    @staticmethod
    def extract_text(pdf_path: str, max_pages: int = 10) -> str:
        """
        Extracts text from a PDF file.
        Limits the number of pages to avoid overloading the context window.
        """
        if not pdf_path or not os.path.exists(pdf_path):
            logger.warning(f"PDF not found at {pdf_path}")
            return ""

        try:
            text = ""
            with open(pdf_path, "rb") as f:
                reader = PyPDF2.PdfReader(f)
                num_pages = len(reader.pages)
                
                pages_to_read = min(num_pages, max_pages)
                
                for i in range(pages_to_read):
                    page = reader.pages[i]
                    text += page.extract_text() + "\n\n"
                    
                if num_pages > max_pages:
                    text += f"\n[... Truncado. El documento tiene {num_pages} páginas en total ...]"
            
            return text
        except Exception as e:
            logger.error(f"Error reading PDF {pdf_path}: {e}")
            return f"[Error al leer PDF: {str(e)}]"
