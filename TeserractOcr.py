from pdf2image import convert_from_path
import pytesseract
import sys

# REQUIRED: Path to tesseract
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

def extract_pdf_ocr(pdf_path):
    pages = convert_from_path(pdf_path, dpi=150)

    text_output = []
    for page in pages:
        text_output.append(pytesseract.image_to_string(page).strip())

    return "\n\n".join(text_output)


if __name__ == "__main__":
    # Ensure arguments exist
    if len(sys.argv) < 3:
        print("Error: Missing arguments")
        sys.exit(1)

    action = sys.argv[1]
    pdf_path = sys.argv[2]

    if action == "extract_pdf_ocr":
        result = extract_pdf_ocr(pdf_path)
        print(result)  # <-- THIS IS IMPORTANT FOR OUTPUT
