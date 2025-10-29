import json
import boto3
import os
import re
import pytesseract
from PIL import Image, ImageEnhance
import fitz  # PyMuPDF

s3 = boto3.client("s3")

LAMBDA_VERSION = "1.1.0"

def convert_page_to_text(doc, page_num=0):
    """Extract one page using direct text, OCR fallback, and form field capture."""
    page = doc[page_num]

    # --- Try direct text ---
    text_direct = page.get_text("text") or ""
    print(f"Direct text length: {len(text_direct)}")

    if len(text_direct) > 100:
        print("Using direct text extraction.")
        return text_direct, False

    # --- OCR fallback ---
    print("Running OCR fallback...")
    pix = page.get_pixmap(dpi=200)
    mode = "RGBA" if pix.alpha else "RGB"
    img = Image.frombytes(mode, [pix.width, pix.height], pix.samples)
    img_gray = img.convert("L")
    img_contrast = ImageEnhance.Contrast(img_gray).enhance(2.0)

    text_ocr = pytesseract.image_to_string(img_contrast, config="--oem 3 --psm 6")
    print(f"OCR text length: {len(text_ocr)}")

    return text_ocr, True


def extract_names_block(lines):
    """Look for grouped name fields in nearby lines."""
    idx_family = idx_given = idx_middle = None

    for i, line in enumerate(lines):
        if re.search(r'Family\s+Name', line, re.I):
            idx_family = i
        elif re.search(r'Given\s+Name', line, re.I):
            idx_given = i
        elif re.search(r'Middle\s+Name', line, re.I):
            idx_middle = i

    val_family = val_given = val_middle = "[empty]"

    # Find next text lines after label lines
    if idx_family is not None:
        for j in range(idx_family + 1, len(lines)):
            if lines[j] and not re.search(r'Name', lines[j], re.I):
                val_family = lines[j]
                break
    if idx_given is not None:
        for j in range(idx_given + 1, len(lines)):
            if lines[j] and not re.search(r'Name', lines[j], re.I):
                val_given = lines[j]
                break
    if idx_middle is not None:
        for j in range(idx_middle + 1, len(lines)):
            if lines[j] and not re.search(r'Name', lines[j], re.I):
                val_middle = lines[j]
                break

    if idx_family is not None and idx_given is not None:
        return (
            "Family Name (Last Name) | Given Name (First Name) | Middle Name\n"
            f"{val_family:<25} | {val_given:<25} | {val_middle:<25}"
        )
    return None


def lambda_handler(event, context):
    print(f"Lambda version: {LAMBDA_VERSION}")
    print("Event received:", json.dumps(event))

    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    if not key.lower().endswith(".pdf"):
        return {"statusCode": 200, "message": "Skipped - not a PDF"}

    print(f"Processing: s3://{bucket}/{key}")

    local_path = "/tmp/" + os.path.basename(key)
    s3.download_file(bucket, key, local_path)

    try:
        doc = fitz.open(local_path)
        print(f"PDF has {len(doc)} pages")

        # Extract text from first page only
        full_text, used_ocr = convert_page_to_text(doc, page_num=0)
        doc.close()

        # Clean text to ASCII only
        full_text_ascii = re.sub(r"[^\x20-\x7E]+", " ", full_text)
        lines = [ln.strip() for ln in full_text_ascii.splitlines() if ln.strip()]

        # Add line numbers
        numbered_lines = [{"line_number": i + 1, "text": line} for i, line in enumerate(lines)]

        # Create formatted section
        formatted_block = extract_names_block(lines)

        # Detect form number and A-number
        form_number = "I-485" if "I-485" in full_text_ascii else None
        a_match = re.search(r"\bA[-\s]*([0-9]{8,10})\b", full_text_ascii)
        a_number = a_match.group(1) if a_match else None

        result = {
            "statusCode": 200,
            "lambda_version": LAMBDA_VERSION,
            "bucket": bucket,
            "key": key,
            "pdf_form_number": form_number,
            "pdf_a_number": a_number,
            "used_ocr": used_ocr,
            "full_text_length": len(full_text_ascii),
            "page_1_formatted": formatted_block,
            "lines": numbered_lines
        }

        json_key = os.path.splitext(key)[0] + ".json"
        s3.put_object(
            Bucket=bucket,
            Key=json_key,
            Body=json.dumps(result, ensure_ascii=True, indent=2).encode("utf-8"),
            ContentType="application/json"
        )

        print("JSON output saved:", json_key)
        return result

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"statusCode": 500, "error": str(e)}

    finally:
        if os.path.exists(local_path):
            os.remove(local_path)
