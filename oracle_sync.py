import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import oracledb
import time
from datetime import datetime
import pytz
import os
import json

# ==========================================
# CONFIGURATION: Yahan apne tasks ki list dalein
# ==========================================
TASKS_CONFIG = [
    # ---- TASK 1 ----
    {
        "sheet_name": "LASER ADVANCE REQUEST",
        "worksheet_name": "PAYMENT ALLOCATION",
        "query": """select PARTYBILLNO,sum(NVL(DRAMT,0))DRAMT,SUM(NVL(CRAMT,0))CRAMT,SUM(NVL(ALLOC_AMT,0))ALLOC_AMT,
        SUM(NVL(ASON_ALLOC_AMT,0))ASON_ALLOC_AMT ,SUM(NVL(DRAMT,0)- NVL(ALLOC_AMT,0))BAL_TO_ALLC from LPIERP.view_acc_tran_engine 
        where  NVL(DRAMT,0) <> 0 AND PARTYBILLNO IS NOT NULL AND VRDATE >= '10-JUN-2024' and LENGTH(PARTYBILLNO) = '8'
        GROUP BY PARTYBILLNO
        union all
        select substr(narration,2,8)as PARTYBILLNO,sum(NVL(DRAMT,0))DRAMT,SUM(NVL(CRAMT,0))CRAMT,SUM(NVL(ALLOC_AMT,0))ALLOC_AMT,
        SUM(NVL(ASON_ALLOC_AMT,0))ASON_ALLOC_AMT ,SUM(NVL(DRAMT,0)- NVL(ALLOC_AMT,0))BAL_TO_ALLC from LPIERP.view_acc_tran_engine 
        where  NVL(DRAMT,0) <> 0  AND VRDATE >= '10-JUN-2024' and narration like ('%@%')
        GROUP BY substr(narration,2,8)"""
    },
    
    # ---- TASK 2 (Naya wala) ----
    {
        "sheet_name": "LASER PROJECT ADVANCE REQUEST",
        "worksheet_name": "PAYMENT ALLOCATION",
        "query": """select substr(PARTICULAR,-8,8)as PARTYBILLNO,sum(NVL(DRAMT,0))DRAMT,SUM(NVL(CRAMT,0))CRAMT,SUM(NVL(ALLOC_AMT,0))ALLOC_AMT,
        SUM(NVL(ASON_ALLOC_AMT,0))ASON_ALLOC_AMT ,SUM(NVL(DRAMT,0)- NVL(ALLOC_AMT,0))BAL_TO_ALLC from LPIERP.view_acc_tran_engine 
        where  NVL(DRAMT,0) <> 0  AND VRDATE >= '17-JUl-2024' and PARTICULAR LIKE ('%ADVANCE%') AND DIV_CODE = 'RE' 
        GROUP BY substr(PARTICULAR,-8,8)"""
    }
]

# Database Settings (Environment Variables se aayenge)
DB_USER = "lpierp"
DB_PASS = os.getenv('DB_PASSWORD')
DB_DSN = "52.172.224.35:1521/ora12c"

def run_sync():
    # 1. Setup Google Credentials from Secret
    print("🔗 Authenticating with Google...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_key_data = json.loads(os.getenv('GOOGLE_JSON_KEY'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_key_data, scope)
    client = gspread.authorize(creds)

    # 2. Setup Oracle Client (GitHub Runner ke path ke liye)
    try:
        oracledb.init_oracle_client(lib_dir="./instantclient/instantclient_19_24")
    except Exception as e:
        print(f"Oracle Client Info: {e}")

    # 3. Process Each Task
    for task in TASKS_CONFIG:
        start_time = time.time()
        print(f"🔄 Processing: {task['sheet_name']} -> {task['worksheet_name']}")
        
        try:
            conn = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)
            df = pd.read_sql(task['query'], con=conn)
            df = df.fillna('')
            
            sheet = client.open(task['sheet_name']).worksheet(task['worksheet_name'])
            sheet.clear()
            
            data_to_upload = [df.columns.values.tolist()] + df.values.tolist()
            sheet.update(data_to_upload)
            
            # Timer & Note
            time_taken = round((time.time() - start_time) / 60, 2)
            ist_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%d-%b-%Y %I:%M:%S %p")
            note_text = f"Rows: {len(df)}\nLast Run: {ist_time} (IST)\nTime: {time_taken} min"
            sheet.insert_note('A1', note_text)
            
            print(f"✅ Success! {len(df)} rows updated.")
            conn.close()
            
        except Exception as e:
            print(f"❌ Error in Task {task['sheet_name']}: {e}")

if __name__ == "__main__":
    run_sync()
