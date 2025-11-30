import pandas as pd
import datetime
import logging
import os
import json


try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    from firebase_admin.exceptions import FirebaseError
    FIRESTORE_ENABLED = True
except ImportError:
    FIRESTORE_ENABLED = False
    print("WARNING: firebase-admin not found. Please run 'pip install firebase-admin'")


DATA_FOLDER = "data"
RISK_HISTORY_FILE = os.path.join(DATA_FOLDER, "risk_history.csv") 
NEWS_LOG_FILE = os.path.join(DATA_FOLDER, "daily_news_scan.csv")
SERVICE_ACCOUNT_FILE = os.path.join(DATA_FOLDER, "serviceAccountKey.json")


CANVAS_APP_ID = "sl_risk_monitor" 
CANVAS_USER_ID = "backend_service_user" 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_firestore():
   
    if not FIRESTORE_ENABLED:
        logging.error("Firestore library is not installed.")
        return None
        
    if firebase_admin._apps:
        logging.info("Firebase app already initialized.")
        return firestore.client()

    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        logging.error(f"Service account file not found at: {SERVICE_ACCOUNT_FILE}. Cannot connect to Firebase.")
        return None

    try:
        cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
        firebase_admin.initialize_app(cred)
        logging.info("🔥 Firestore Initialized successfully.")
        return firestore.client()
    except Exception as e:
        logging.error(f"Error initializing Firebase: {e}")
        return None

def migrate_risk_history(db):
 
    if not os.path.exists(RISK_HISTORY_FILE):
        logging.warning(f"Risk history file not found at: {RISK_HISTORY_FILE}. Skipping migration.")
        return

    try:
        df = pd.read_csv(RISK_HISTORY_FILE)
    except Exception as e:
        logging.error(f"Error reading {RISK_HISTORY_FILE}: {e}")
        return

    collection_path = f'artifacts/{CANVAS_APP_ID}/users/{CANVAS_USER_ID}/riskHistory'
    logging.info(f"Starting migration of {len(df)} risk records to Firestore collection: {collection_path}")


    batch = db.batch()
    
    for index, row in df.iterrows():
      
        record = row.to_dict()
        
       
        doc_id = f"{record['Timestamp'].replace(' ', '_').replace(':', '')}_{index}"
        doc_ref = db.collection(collection_path).document(doc_id)
        
        
        cleaned_record = {k: (v if not isinstance(v, (float)) else float(v)) for k, v in record.items()}

        batch.set(doc_ref, cleaned_record)
        
        if (index + 1) % 400 == 0:
         
            batch.commit()
            logging.info(f"Committed batch up to record {index + 1}.")
            batch = db.batch()
            
   
    batch.commit()
    logging.info(f"✅ Risk History migration complete! Total records uploaded: {len(df)}")


def migrate_news_history(db):
   
    if not os.path.exists(NEWS_LOG_FILE):
        logging.warning(f"News log file not found at: {NEWS_LOG_FILE}. Skipping migration.")
        return

    try:
       
        df = pd.read_csv(NEWS_LOG_FILE)
    except Exception as e:
        logging.error(f"Error reading {NEWS_LOG_FILE}: {e}")
        return

    collection_path = f'artifacts/{CANVAS_APP_ID}/users/{CANVAS_USER_ID}/newsHistory'
    logging.info(f"Starting migration of {len(df)} news records to Firestore collection: {collection_path}")

    batch = db.batch()
    
    for index, row in df.iterrows():
        record = row.to_dict()
        
    
        doc_id = f"{record['Timestamp'].replace(' ', '_').replace(':', '')}_{index}"
        doc_ref = db.collection(collection_path).document(doc_id)
        
       
        cleaned_record = {
            "Headline": str(record.get('Headline', 'No Headline')),
            "Link": str(record.get('Link', 'N/A')),
            "Risk": int(record.get('Risk', 0)),
            "Sector": str(record.get('Sector', 'Unknown')),
            "Timestamp": str(record.get('Timestamp', datetime.datetime.now().isoformat()))
        }

        batch.set(doc_ref, cleaned_record)
        
        if (index + 1) % 400 == 0:
            batch.commit()
            logging.info(f"Committed batch up to news record {index + 1}.")
            batch = db.batch()
            
    batch.commit()
    logging.info(f"✅ News History migration complete! Total records uploaded: {len(df)}")


def run_migrator():
    db = initialize_firestore()
    if db:
        migrate_risk_history(db)
        migrate_news_history(db)
        logging.info("🎉 All data migration tasks finished.")

if __name__ == "__main__":
    run_migrator()