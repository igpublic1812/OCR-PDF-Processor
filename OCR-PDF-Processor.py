import json
import boto3
import fitz                     # PyMuPDF
import pytesseract
from PIL import Image
import io
import re

s3 = boto3.client('s3')
BUCKET = 's3-1812-pdf'

def lambda_handler(event, context):
    # 1. Get the uploaded PDF key
    key = event['Records'][0]['s3']['object']['key']

    # 2. Download PDF into memory
    pdf_data = s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()

    # 3. Open with PyMuPDF (in-memory)
    doc = fitz.open(stream=pdf_data, filetype="pdf")
    result = {"A-Number": "", "LastName": "", "Status": "Processed"}

    # 4. Process **only the first page**
    page = doc[0]

    # 5. Try form fields (will be empty on flattened PDFs)
    widgets = page.widgets()
    if widgets:
        for w in widgets:
            name = w.field_name or ""
            value = w.field_value or ""
            if "A-Number" in name and value:
                result["A-Number"] = value
            elif "LastName" in name and value:
                result["LastName"] = value

    # 6. OCR fallback (high-res, English)
    if not result["A-Number"] or not result["LastName"]:
        pix = page.get_pixmap(matrix=fitz.Matrix(3, 3))   # 300 dpi
        img = Image.open(io.BytesIO(pix.tobytes()))
        ocr_text = pytesseract.image_to_string(img, lang='eng')

        # ---- A-Number ----
        if not result["A-Number"]:
            a_match = re.search(
                r'A[-\s]?(\d{3}[-\s]?\d{3}[-\s]?\d{3}|\d{9}|\d{8})',
                ocr_text, re.IGNORECASE)
            if a_match:
                result["A-Number"] = re.sub(r'[^A0-9]', '',
                                          a_match.group(0).upper())

        # ---- Last Name ----
        if not result["LastName"]:
            l_match = re.search(r'Last\s+Name[:\s]+([A-Za-z\s]+)',
                                ocr_text, re.IGNORECASE)
            if l_match:
                result["LastName"] = l_match.group(1).strip()

    doc.close()

    # 9. Write **only** the JSON file (fixed name)
    s3.put_object(
        Bucket=BUCKET,
        Key='output/I-485-3.json',
        Body=json.dumps(result, indent=2),
        ContentType='application/json'
    )

    # 10. Return
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
