import csv
import json
import re
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase Admin SDK
cred = credentials.Certificate(r'acn-resale-inventories-dde03-firebase-adminsdk-ikyw4-5d72718262.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

def camel_case(s):
    """Convert human-readable headers to camelCase."""
    parts = re.split(r'[\s_()/-]+', s.strip())
    return parts[0].lower() + ''.join(word.title() for word in parts[1:])

def clean_numeric(value):
    """
    Cleans numeric fields by removing commas and extracting numbers.
    """
    if not value or value.strip() in ['--', '']:
        return None
    value = re.sub(r'[^\d.,]', '', value).replace(',', '')
    match = re.search(r'(\d+(\.\d+)?)', value)
    if match:
        num_str = match.group(1)
        try:
            return float(num_str) if '.' in num_str else int(num_str)
        except ValueError:
            return None
    return None

def csv_to_json(csv_file_path, json_file_path):
    """
    Converts the current CSV format to JSON with proper camelCased headers and cleaned numeric fields.
    """
    data = []
    warnings = []
    
    # Define fields that require numeric cleaning
    numeric_fields = {
        "plotSize": "float",       
        "carpet": "int",
        "sbua": "int",
        "totalAskPrice": "float",
        "askPricePerSqft": "float",
        "ageOfInventory": "int",
        "ageOfStatus": "int"
    }

    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        headers = next(csv_reader)  # Skip the first row (human-readable headers)
        camel_case_headers = [camel_case(header) for header in headers]  # Convert headers to camelCase
        
        for row_num, row in enumerate(csv_reader, start=1):
            row_data = dict(zip(camel_case_headers, row))
            for field, dtype in numeric_fields.items():
                if field in row_data:
                    original_value = row_data[field].strip()
                    cleaned_value = clean_numeric(original_value)
                    if cleaned_value is not None:
                        row_data[field] = int(cleaned_value) if dtype == "int" else float(cleaned_value)
                    else:
                        row_data[field] = None
                        warnings.append(f"Row {row_num}: Could not convert '{field}' with value '{original_value}' to {dtype}")
            data.append(row_data)
    
    with open(json_file_path, mode='w', encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=4)

    with open("conversion_warnings.log", "w", encoding="utf-8") as log_file:
        for warning in warnings:
            log_file.write(warning + "\n")
    
    print(f"CSV data successfully converted to JSON at {json_file_path}")
    print(f"Conversion warnings logged in 'conversion_warnings.log'")
    return data

def upload_json_to_firestore(json_file_path, collection_name):
    """
    Uploads JSON data to Firestore with Property ID as document ID.
    """
    with open(json_file_path, 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)

    for item in data:
        property_id = item.get("propertyId")
        if property_id:
            db.collection(collection_name).document(property_id).set(item)
        else:
            print(f"Skipping item with missing Property ID: {item}")

    print("Data uploaded successfully to Firestore.")

# Main Process
csv_file_path = 'acn.csv'  # Your input CSV file path
json_file_path = 'output.json'  # Path to save the intermediate JSON
collection_name = 'ACN123'  # Firestore collection name

# Convert CSV to JSON
csv_to_json(csv_file_path, json_file_path)

# Upload JSON to Firestore
upload_json_to_firestore(json_file_path, collection_name)
