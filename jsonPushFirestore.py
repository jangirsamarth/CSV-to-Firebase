import firebase_admin
from firebase_admin import credentials, firestore
import json

# Initialize Firebase Admin SDK
cred = credentials.Certificate(r'acn-resale-inventories-dde03-firebase-adminsdk-ikyw4-5d72718262.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

def upload_json_to_firestore(json_file_path, collection_name):
    # Load JSON data
    with open(json_file_path, 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)

    # Upload each item in JSON to Firestore using Property ID as the document ID
    for item in data:
        property_id = item.get("propertyId")  # Get the Property ID
        if property_id:  # Ensure Property ID exists
            db.collection(collection_name).document(property_id).set(item)
        else:
            print(f"Missing Property ID for item: {item}")

    print("Data uploaded successfully to Firestore with Property ID as document IDs.")

# Example usage
json_file_path = 'output.json'  # Replace with your JSON file path
collection_name = 'ACN123'  # Replace with your Firestore collection name
upload_json_to_firestore(json_file_path, collection_name)
