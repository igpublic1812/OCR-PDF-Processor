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
BUILD_VERSION = "1.1.4"         # <-- fixed tuple key + added logging
# ------------------------------------------------------------------


def lambda_handler(event, context):
    # --------------------------------------------------------------
    # 1️⃣ Get the uploaded PDF key safely and log raw value
    # --------------------------------------------------------------
    raw_key = event['Records'][0]['s3']['object']['key']
    print(f"[DEBUG] Raw key from event: {repr(raw_key)} (type: {type(raw_key)})")

    if isinstance(raw_key, tuple):
        key = raw_key[0]
    else:
        key = str(raw_key)

    print(f"[INFO] Processing file: {key}")

    # --------------------------------------------------------------
    # 2️⃣ Download PDF from S3
    # --------------------------------------------------------------
    pdf_data = s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
    doc = fitz.open(stream=pdf_data, filetype="pdf")

    result = {
        "FormNumber": "",
        "A-Number": "",
        "LastName": "",
        "FirstName": "",
        "MiddleName": "",
        "Status": "Processed"
    }

    # --------------------------------------------------------------
    # 3️⃣ Process first page only
    # --------------------------------------------------------------
    page = doc[0]
    page_text = page.get_text("text")

    if not page_text.strip():
        pix = page.get_pixmap(matrix=fitz.Matrix(3, 3))
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        img = img.convert('L')
        img = img.point(lambda x: 0 if x < 180 else 255, '1')
        page_text = pytesseract.image_to_string(img, lang='eng')

    # Clean up text
    page_text = re.sub(r'[^\x09\x0A\x0D\x20-\x7E]', '', page_text)
    lines = [line.strip() for line in page_text.splitlines() if line.strip()]

    print("----- PAGE LINES (debug) -----")
    for idx, line in enumerate(lines):
        print(f"{idx:03}: {line}")
    print("------------------------------")

    # --------------------------------------------------------------
    # 4️⃣ Extract Form Number (e.g., I-485, I-131)
    # --------------------------------------------------------------
    form_match = re.search(r'\bForm\s+(I-\d{1,4}[A-Z]?)', page_text, re.I)
    if form_match:
        result['FormNumber'] = form_match.group(1)
        print(f"[Form Detected]: {result['FormNumber']}")

    # --------------------------------------------------------------
    # 5️⃣ Extract A-Number
    # --------------------------------------------------------------
    a_match = re.search(r'A[-\s]*Number.*?([0-9\s]{2,})', page_text, re.I)
    if a_match:
        raw = a_match.group(1)
        result['A-Number'] = re.sub(r'[^0-9]', '', raw)
        print(f"[A-Number Detected]: {result['A-Number']}")

    # --------------------------------------------------------------
    # 6️⃣ Extract Name fields using line proximity
    # --------------------------------------------------------------
    for i, line in enumerate(lines):
        if re.search(r'Family\s+Name\s*\(Last\s*Name\)', line, re.I):
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                print(f"[Names line detected]: {next_line}")
                parts = next_line.split()
                if len(parts) >= 1:
                    result['LastName'] = parts[0]
                if len(parts) >= 2:
                    result['FirstName'] = parts[1]
                if len(parts) >= 3:
                    result['MiddleName'] = parts[2]
                print(f"[Names Detected]: {result['LastName']}, {result['FirstName']}, {result['MiddleName']}")
            break

    doc.close()

    # --------------------------------------------------------------
    # 7️⃣ Save JSON result to S3
    # --------------------------------------------------------------
    input_filename = os.path.basename(str(key))
    output_filename = input_filename.replace('.pdf', '.json')
    output_key = f'output/{output_filename}'

    s3.put_object(
        Bucket=BUCKET,
        Key=output_key,
        Body=json.dumps(result, indent=2),
        ContentType='application/json'
    )

    print(f"[INFO] Extraction Result: {result}")

    # --------------------------------------------------------------
    # 8️⃣ Return API Response
    # --------------------------------------------------------------
    return {
        'statusCode': 200,
        'body': json.dumps({
            'build_version': BUILD_VERSION,
            'extraction_result': result
        })
    }
