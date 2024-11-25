import csv
import json
import re

def clean_numeric(value):
    """
    Cleans the input value by removing commas and extracting numeric parts.
    Returns a float or int if possible, otherwise returns None.
    """
    if not value or value.strip() in ['--', '']:
        return None
    
    # Remove commas and units if present
    value = re.sub(r'[^\d.,]', '', value).replace(',', '')
    
    # Use regex to extract the first numeric part (integer or float)
    match = re.search(r'(\d+(\.\d+)?)', value)
    if match:
        num_str = match.group(1)
        try:
            return float(num_str) if '.' in num_str else int(num_str)
        except ValueError:
            return None
    return None

def csv_to_json(csv_file_path, json_file_path):
    data = []
    warnings = []
    
    # Define the fields that should be converted to integers or floats
    numeric_fields = {
        "plotSize": "float",       
        "carpet": "int",
        "sbua": "int",
        "totalAskPrice": "float",
        "askPricePerSqft": "float",
        "geOfInventory": "int",
        "ageOfStatus": "int"
    }
    
    # Read the CSV file and process rows
    with open(csv_file_path, mode='r', encoding='utf-8', errors='replace') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row_num, row in enumerate(csv_reader, start=1):
            for field, dtype in numeric_fields.items():
                original_value = row.get(field, "").strip()
                cleaned_value = clean_numeric(original_value)
                
                if cleaned_value is not None:
                    row[field] = int(cleaned_value) if dtype == "int" else float(cleaned_value)
                else:
                    row[field] = None
                    warnings.append(f"Row {row_num}: Could not convert '{field}' with value '{original_value}' to {dtype}")
            data.append(row)
    
    # Write to JSON
    with open(json_file_path, mode='w', encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=4)
    
    # Write warnings to log file
    with open("conversion_warnings.log", "w", encoding="utf-8") as log_file:
        for warning in warnings:
            log_file.write(warning + "\n")
    
    print(f"CSV data successfully converted to JSON at {json_file_path}")
    print(f"Conversion warnings logged in 'conversion_warnings.log'")

# Example usage
csv_file_path = 'acn.csv'  # Replace with your CSV file path
json_file_path = r'output.json'  # Replace with desired JSON file path
csv_to_json(csv_file_path, json_file_path)
