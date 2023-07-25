import sqlite3
from dataclasses import dataclass
from datetime import date
from uuid import UUID
from fastapi import FastAPI

@dataclass
class BatchBillingInfo:
    data: list[dict]

def _billing_data_insert_query() -> str:
    return """
    INSERT INTO BillingInfo (
        id,
        customer_id,
        plan_id,
        billing_date,
        total_charges,
        data_charges,
        roaming_charges,
        data_usage,
        sms_count
    )
    VALUES (:id,
        :customer_id,
        :plan_id,
        :billing_date,
        :total_charges,
        :data_charges,
        :roaming_charges,
        :data_usage,
        :sms_count
    );
    """

def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}

with sqlite3.connect("fastapi.db") as conn:
    cur = conn.cursor()
    conn.execute("""CREATE TABLE IF NOT EXISTS BillingInfo (
        id VARCHAR(100) PRIMARY KEY,
        customer_id CHAR(36) CHECK(length(customer_id) = 36),
        plan_id SMALLINT,
        billing_date DATE,
        total_charges FLOAT,
        data_charges FLOAT,
        roaming_charges FLOAT,
        data_usage FLOAT,
        sms_count INTEGER
    );""")

app = FastAPI()

@app.get("/billings")
def read_billing_records(skip: int = 0, limit: int = 1000) -> list[dict]:
    with sqlite3.connect("fastapi.db") as conn:
        conn.row_factory = dict_factory
        cur = conn.cursor()

        # Execute the SELECT statement with LIMIT and OFFSET clauses to implement paging
        query = f"SELECT * FROM BillingInfo LIMIT {limit} OFFSET {skip}"
        cur.execute(query)

        # Fetch the results and return them as a list of dictionaries
        res = cur.fetchall()

    return res

@app.post("/billings")
def save_billing_records(batch_billing: BatchBillingInfo):
    with sqlite3.connect("fastapi.db") as conn:
        cur = conn.cursor()
        cur.executemany(_billing_data_insert_query(), batch_billing.data)
    return {"message":"Billing records are successfully inserted"}

@app.delete("/billing", status_code=204)
def delete_billing_records():
    with sqlite3.connect("fastapi.db") as conn:
        cursor = conn.cursor()

        # Execute the TRUNCATE statement to delete all rows from the BillingInfo table
        cursor.execute("DELETE FROM BillingInfo;")
    
# uvicorn main:app --reload