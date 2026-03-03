import sys
import pytesseract
from pdf2image import convert_from_path
import tempfile
import os

# Path to your Tesseract executable
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# Check if PDF file path is passed
if len(sys.argv) < 2:
    print("❌ Please provide PDF file path as argument.")
    sys.exit(1)

pdf_path = sys.argv[1]

# Extract text
text_output = ""
with tempfile.TemporaryDirectory() as temp_dir:
    pages = convert_from_path(pdf_path, dpi=300)
    for page_number, _ in enumerate(pages):
        image_path = os.path.join(temp_dir, f"page_{page_number + 1}.png")
        pages[page_number].save(image_path, "PNG")
        text = pytesseract.image_to_string(image_path)
        text_output += f"\n\n--- Page {page_number + 1} ---\n{text}"
        os.remove(image_path)

# Print output text (this will be returned to Power Automate Desktop)
print(text_output)
