import json
import boto3
import fitz                     # PyMuPDF
import pytesseract
from PIL import Image
import io
import re
import os

# ------------------------------------------------------------------
s3 = boto3.client('s3')
BUCKET = 's3-1812-pdf'          # <-- your S3 bucket name
BUILD_VERSION = "1.1.0"         # <-- updated Lambda build version
# ------------------------------------------------------------------

def lambda_handler(event, context):
    # 1️⃣ Get the uploaded PDF key
    key = event['Records'][0]['s3']['object']['key']
    print(f"[INFO] Processing file: {key}")

    # 2️⃣ Download PDF from S3 into memory
    pdf_data = s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()

    # 3️⃣ Open PDF
    doc = fitz.open(stream=pdf_data, filetype="pdf")

    result = {
        "A-Number": "",
        "LastName": "",
        "FirstName": "",
        "MiddleName": "",
        "Status": "Processed"
    }

    page = doc[0]

    # --------------------------------------------------------------
    # 4️⃣ Try extracting text layer first
    # --------------------------------------------------------------
    page_text = page.get_text("text")
    if not page_text.strip():
        # fallback to OCR
        pix = page.get_pixmap(matrix=fitz.Matrix(3, 3))
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        # Enhance OCR accuracy
        img = img.convert('L')  # grayscale
        img = img.point(lambda x: 0 if x < 180 else 255, '1')  # threshold
        page_text = pytesseract.image_to_string(img, lang='eng')

    # Clean text (keep basic ASCII)
    page_text = re.sub(r'[^\x09\x0A\x0D\x20-\x7E]', '', page_text)
    print("----- CLEANED PAGE TEXT -----")
    print(page_text)
    print("------------------------------")

    # --------------------------------------------------------------
    # 5️⃣ Extract A-Number (digits only)
    # --------------------------------------------------------------
    a_match = re.search(r'A[-\s]*Number.*?([0-9\s]{2,})', page_text, re.I)
    if a_match:
        raw = a_match.group(1)
        result['A-Number'] = re.sub(r'[^0-9]', '', raw)
        print(f"[A-Number Detected]: {result['A-Number']}")

    # --------------------------------------------------------------
    # 6️⃣ Extract Name Fields (line-based, OCR-safe)
    # --------------------------------------------------------------
    lines = [line.strip() for line in page_text.splitlines() if line.strip()]

    # Find the header line
    header_idx = None
    for i, line in enumerate(lines):
        if re.search(r'Family\s*Name\s*\(Last\s*Name\).*Given\s*Name.*Middle\s*Name', line, re.I):
            header_idx = i
            break

    if header_idx is not None and header_idx + 1 < len(lines):
        # Take the next line after header as the name values
        name_line = lines[header_idx + 1]
        name_parts = name_line.split()
        result['LastName'] = name_parts[0] if len(name_parts) > 0 else ''
        result['FirstName'] = name_parts[1] if len(name_parts) > 1 else ''
        result['MiddleName'] = name_parts[2] if len(name_parts) > 2 else ''
        print(f"[Names Detected]: {result['LastName']}, {result['FirstName']}, {result['MiddleName']}")

    doc.close()

    # --------------------------------------------------------------
    # 7️⃣ Save JSON result to S3
    # --------------------------------------------------------------
    input_filename = os.path.basename(key)
    output_filename = os.path.splitext(input_filename)[0] + '.json'
    output_key = f'output/{output_filename}'

    s3.put_object(
        Bucket=BUCKET,
        Key=output_key,
        Body=json.dumps(result, indent=2),
        ContentType='application/json'
    )

    print(f"[INFO] Extraction Result: {result}")

    # --------------------------------------------------------------
    # 8️⃣ Return API Response with build version
    # --------------------------------------------------------------
    return {
        'statusCode': 200,
        'body': json.dumps({
            'build_version': BUILD_VERSION,
            'extraction_result': result
        })
    }
