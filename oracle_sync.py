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
# CONFIGURATION: TASK LIST
# ==========================================
TASKS_CONFIG = [

    # ==========================================
    # TASK 1
    # ==========================================
    {
        "sheet_name": "LASER ADVANCE REQUEST",
        "worksheet_name": "PAYMENT ALLOCATION",
        "query": """
        select PARTYBILLNO,
               sum(NVL(DRAMT,0)) DRAMT,
               SUM(NVL(CRAMT,0)) CRAMT,
               SUM(NVL(ALLOC_AMT,0)) ALLOC_AMT,
               SUM(NVL(ASON_ALLOC_AMT,0)) ASON_ALLOC_AMT,
               SUM(NVL(DRAMT,0)- NVL(ALLOC_AMT,0)) BAL_TO_ALLC
        from LPIERP.view_acc_tran_engine 
        where NVL(DRAMT,0) <> 0 
          AND PARTYBILLNO IS NOT NULL 
          AND VRDATE >= '10-JUN-2024' 
          and LENGTH(PARTYBILLNO) = '8'
        GROUP BY PARTYBILLNO

        union all

        select substr(narration,2,8) as PARTYBILLNO,
               sum(NVL(DRAMT,0)) DRAMT,
               SUM(NVL(CRAMT,0)) CRAMT,
               SUM(NVL(ALLOC_AMT,0)) ALLOC_AMT,
               SUM(NVL(ASON_ALLOC_AMT,0)) ASON_ALLOC_AMT,
               SUM(NVL(DRAMT,0)- NVL(ALLOC_AMT,0)) BAL_TO_ALLC
        from LPIERP.view_acc_tran_engine 
        where NVL(DRAMT,0) <> 0  
          AND VRDATE >= '10-JUN-2024' 
          and narration like ('%@%')
        GROUP BY substr(narration,2,8)
        """
    },

    # ==========================================
    # TASK 2
    # ==========================================
    {
        "sheet_name": "LASER PROJECT ADVANCE REQUEST",
        "worksheet_name": "PAYMENT ALLOCATION",
        "query": """
        select substr(PARTICULAR,-8,8) as PARTYBILLNO,
               sum(NVL(DRAMT,0)) DRAMT,
               SUM(NVL(CRAMT,0)) CRAMT,
               SUM(NVL(ALLOC_AMT,0)) ALLOC_AMT,
               SUM(NVL(ASON_ALLOC_AMT,0)) ASON_ALLOC_AMT,
               SUM(NVL(DRAMT,0)- NVL(ALLOC_AMT,0)) BAL_TO_ALLC
        from LPIERP.view_acc_tran_engine 
        where NVL(DRAMT,0) <> 0  
          AND VRDATE >= '17-JUl-2024' 
          and PARTICULAR LIKE ('%ADVANCE%') 
          AND DIV_CODE = 'RE' 
        GROUP BY substr(PARTICULAR,-8,8)
        """
    },

    # ==========================================
    # TASK 3
    # ==========================================
    {
        "sheet_name": "LASER PARTY MASTER LIVE ERP",
        "worksheet_name": "LASER PARTY MASTER",
        "query": """
        SELECT ACC_CODE,
               ACC_NAME,
               ACC_TYPE
        FROM LPIERP.ACC_MAST
        order by acc_name asc
        """
    },

    # ==========================================
    # TASK 4
    # ==========================================
    {
        "sheet_name": "OPEN MC LASER",
        "worksheet_name": "OPEN MC",
        "query": """
        SELECT 
            a.contract_vrno,
            A.irfield3 AS PRICE_BASIS,
            a.ACC_CODE,
            LPIERP.lhs_utility.get_name('ACC_CODE',A.ACC_CODE) ACC_NAME,
            a.partyrefno,
            a.VRNO AS MC_NO,
            a.VRDATE AS MC_DATE,
            a.SLNO,
            a.ITEM_CODE,
            a.ITEM_NAME,
            A.bom_id,
            G.LEVEL_3_NAME,
            G.LEVEL_4_NAME,
            a.UM,
            nvl(a.qtyorder,0)-nvl(a.QTYCANCELLED,0) as NET_MC,
            a.TNATURE,
            nvl(A.qtyexecute,0) AS BILLED,
            (NVL(a.qtyorder,0)-nvl(a.QTYCANCELLED,0)-NVL(A.qtyexecute,0)) AS BAL_BILL,
            (NVL(a.qtyorder,0)-nvl(a.QTYCANCELLED,0)-NVL(A.qtyexecute,0))*a.rate as BAL_BILL_VALUE,
            nvl(A.INSPO_QTY,0) INSPO_QTY,
            nvl(A.INSPO_SO_QTY,0) INSPO_SO_QTY,
            nvl(A.INSPE_SO_QTY,0) INSPE_SO_QTY,
            nvl(A.DIDM_SO_QTY,0) DIDM_SO_QTY

        FROM lpierp.VIEW_ORDER_LPI a 

        LEFT OUTER JOIN LPIERP.VIEW_ITEM_MAST_ENGINE G 
            ON A.ITEM_CODE = G.ITEM_CODE

        WHERE a.TCODE = 'O' 
          AND a.TNATURE = 'SORD' 
          AND (nvl(a.qtyorder,0)-nvl(a.QTYCANCELLED,0)) > 0 
          AND NVL(A.CLOSED_FLAG,'#') <> 'C' 
          AND (NVL(a.qtyorder,0)-nvl(a.QTYCANCELLED,0)-NVL(a.qtyexecute,0)) > 0
          AND SUBSTR(a.VRNO,1,2) IN ('L1','SP','L2','L6')
        """
    },

    # ==========================================
    # TASK 5
    # ==========================================
    {
        "sheet_name": "BAL TO PRODUCTION AG PRODUCTION ORDER (CONDUCTOR)",
        "worksheet_name": "ERP DUMP_PROD_ORD_OPEN MC",
        "query": """
        SELECT DISTINCT

            B.VRNO AS PORD_NO,
            B.SLNO AS PORD_SLNO,
            B.VRDATE AS PORD_DATE,
            B.ITEM_CODE AS SFG_CODE,
            B.UM AS SFG_UM,
            B.NET_QTYORDER AS SFG_NET_ORD_QTY,
            B.ITEM_NAME AS SFG_NAME,
            G.LEVEL_3_NAME SFG_LEVEL_3_NAME,
            G.LEVEL_4_NAME SFG_LEVEL_4_NAME,
            B.BOM_ID AS SFG_BOM,
            B.CONTRACT_VRNO AS MC_NO,
            A.ACC_CODE,
            LPIERP.LHS_UTILITY.GET_NAME('ACC_CODE', A.ACC_CODE) AS ACC_NAME,

            X.FGITEMCODE,
            H.ITEM_NAME AS FG_ITEM_NAME,
            H.LEVEL_3_NAME FG_LEVEL_3_NAME,
            H.LEVEL_4_NAME FG_LEVEL_4_NAME,

            NVL(X.FG_NET_ORDERQTY, 0) AS FG_NET_ORDERQTY,
            NVL(X.FG_PRODUCTION_QTY, 0) AS FG_PRODUCTION_QTY,
            NVL(X.FG_BALANCE_TO_PRODUCTION, 0) AS FG_BALANCE_TO_PRODUCTION,

            CASE
                WHEN NVL(X.FG_NET_ORDERQTY, 0) = 0 THEN 0
                ELSE ROUND((NVL(X.FG_BALANCE_TO_PRODUCTION, 0) / X.FG_NET_ORDERQTY) * 100, 0)
            END AS BAL_PERCENTAGE

        FROM LPIERP.VIEW_ORDER_ENGINE_BAL B

        LEFT JOIN LPIERP.VIEW_ORDER_LPI A
            ON B.CONTRACT_VRNO = A.VRNO

        LEFT JOIN LPIERP.VIEW_ITEM_MAST_ENGINE G
            ON B.ITEM_CODE = G.ITEM_CODE

        LEFT JOIN (
            SELECT
                T1.VRNO,
                T1.ITEM_CODE,
                T1.FGITEMCODE,
                T2.NET_ORDERQTY AS FG_NET_ORDERQTY,
                T2.PRODUCTION_QTY AS FG_PRODUCTION_QTY,
                T2.BALANCE_TO_PRODUCTION AS FG_BALANCE_TO_PRODUCTION

            FROM (
                SELECT *
                FROM (
                    SELECT
                        VRNO,
                        VRDATE,
                        ITEM_CODE,
                        UM,
                        NET_QTYORDER,
                        AUM,
                        RATE_UM,
                        CONTRACT_VRNO,
                        ITEM_NATURE,
                        ITEM_NAME,
                        ITEM_SCH,
                        BOM_ID,

                        MAX(
                            CASE
                                WHEN ITEM_NATURE = 'FG'
                                THEN ITEM_CODE
                            END
                        ) OVER (PARTITION BY VRNO) AS FGITEMCODE

                    FROM LPIERP.VIEW_ORDER_ENGINE_BAL

                    WHERE TNATURE = 'PORD'
                      AND CLOSED_FLAG IS NULL
                      AND VRDATE >= DATE '2020-04-01'
                )

                WHERE ITEM_NATURE <> 'KG'

            ) T1

            LEFT JOIN (
                SELECT
                    VRNO,
                    ITEM_CODE,
                    NET_ORDERQTY,
                    PRODUCTION_QTY,
                    BALANCE_TO_PRODUCTION

                FROM LPIERP.VIEW_PROD_ORDER_ENGINE

                WHERE NET_ORDERQTY > 0

            ) T2

                ON T1.VRNO = T2.VRNO
               AND T1.FGITEMCODE = T2.ITEM_CODE

        ) X

            ON B.VRNO = X.VRNO
           AND B.ITEM_CODE = X.ITEM_CODE

        LEFT JOIN LPIERP.VIEW_ITEM_MAST_ENGINE H
            ON X.FGITEMCODE = H.ITEM_CODE

        WHERE B.TNATURE = 'PORD'
          AND B.CLOSED_FLAG IS NULL
          AND B.ITEM_NATURE NOT IN ('KG')
          AND A.TCODE = 'O'
          AND A.TNATURE = 'SORD'
          AND (NVL(A.QTYORDER, 0) - NVL(A.QTYCANCELLED, 0)) > 0
          AND NVL(A.CLOSED_FLAG, '#') NOT IN ('C')
          AND (NVL(A.QTYORDER, 0) - NVL(A.QTYCANCELLED, 0) - NVL(A.QTYEXECUTE, 0)) > 0
        """
    }
]

# ==========================================
# DATABASE CONFIG
# ==========================================
DB_USER = "lpierp"
DB_PASS = os.getenv('DB_PASSWORD')
DB_DSN = "52.172.224.35:1521/ora12c"

# ==========================================
# MAIN FUNCTION
# ==========================================
def run_sync():

    print("🔗 Authenticating with Google...")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    json_key_data = json.loads(os.getenv('GOOGLE_JSON_KEY'))

    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json_key_data,
        scope
    )

    client = gspread.authorize(creds)

    # Oracle Client
    try:
        oracledb.init_oracle_client(
            lib_dir="./instantclient/instantclient_19_24"
        )

    except Exception as e:
        print(f"Oracle Client Info: {e}")

    # ==========================================
    # PROCESS TASKS
    # ==========================================
    for task in TASKS_CONFIG:

        start_time = time.time()

        print(f"🔄 Processing: {task['sheet_name']} -> {task['worksheet_name']}")

        conn = None

        try:

            # DB CONNECT
            conn = oracledb.connect(
                user=DB_USER,
                password=DB_PASS,
                dsn=DB_DSN
            )

            # QUERY RUN
            df = pd.read_sql(task['query'], con=conn)

            # DATETIME FORMAT FIX
            for col in df.select_dtypes(include=['datetime64', 'datetimetz']).columns:
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

            # OBJECT FORMAT FIX
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].apply(
                    lambda x: str(x) if isinstance(x, (pd.Timestamp, datetime)) else x
                )

            df = df.fillna('')

            # OPEN SHEET
            sheet = client.open(task['sheet_name']).worksheet(task['worksheet_name'])

            # CLEAR OLD DATA
            sheet.clear()

            # PREPARE DATA
            data_to_upload = [df.columns.values.tolist()] + df.values.tolist()

            # ==========================================
            # TASK 5 -> START FROM COLUMN B
            # ==========================================
            if task['sheet_name'] == "BAL TO PRODUCTION AG PRODUCTION ORDER (CONDUCTOR)":
                sheet.update('B1', data_to_upload)

            else:
                sheet.update(data_to_upload)

            # NOTE
            time_taken = round((time.time() - start_time) / 60, 2)

            ist_time = datetime.now(
                pytz.timezone('Asia/Kolkata')
            ).strftime("%d-%b-%Y %I:%M:%S %p")

            note_text = (
                f"Rows: {len(df)}\n"
                f"Last Run: {ist_time} (IST)\n"
                f"Time: {time_taken} min"
            )

            sheet.insert_note('A1', note_text)

            print(f"✅ Success! {len(df)} rows updated.")

        except Exception as e:

            print(f"❌ Error in Task {task['sheet_name']}: {e}")

        finally:

            if conn:
                conn.close()

# ==========================================
# RUN
# ==========================================
if __name__ == "__main__":
    run_sync()
