import numpy as np
import logging
import psycopg2
import psycopg2.extras as p
import requests
import csv
from google.cloud import storage
from time import sleep
from datetime import date
from uuid import UUID, uuid4
from dataclasses import dataclass
from faker import Faker
from contextlib import contextmanager

# Set the logging level and format
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

psycopg2.extras.register_uuid()

@dataclass
class Customer:
    id: UUID
    first_name: str
    last_name: str
    email: str
    phone_number: str
    district_code: int
    date_of_birth: date
    gender: str
    created_at: date


@dataclass
class BillingInfo:
    id: str
    customer_id: UUID
    plan_id: int
    billing_date: date
    total_charges: float
    data_charges: float
    roaming_charges: float
    data_usage: float
    sms_count: int


@dataclass
class SubscriptionStatus:
    id: UUID
    customer_id: UUID
    plan_id: int
    start_date: date
    end_date: date


@dataclass
class CallRecord:
    id: UUID
    customer_id: UUID
    call_date: date
    call_duration: float
    call_type: str
    location_id: int

def write_dict_list_to_csv(file_obj, dict_list):
    """
    Writes a list of dictionaries to a CSV file.

    Args:
        file_obj (file object): The file object where the CSV data will be written.
        dict_list (list): A list of dictionaries, where each dictionary represents a row of data.

    Returns:
        None
    """
    # Create a CSV writer object
    writer = csv.writer(file_obj)

    # Write the header row
    header = dict_list[0].keys()
    writer.writerow(header)

    # Write each row of data
    for row in dict_list:
        writer.writerow(row.values())

def _get_customers(cust_ids: list[UUID]) -> list[dict]:
    """
    Create fake customer profile 
    """
    f = Faker()
    return [
        Customer(
            id = cust_id,
            first_name=f.first_name(),
            last_name=f.last_name(),
            email=f'{cust_id.__str__()[:13]}_{f.email()}',
            phone_number=f.bothify(text="+852 #### ####"),
            district_code=np.random.randint(1, 19),
            date_of_birth=f.date_of_birth(),
            gender=np.random.choice(["male", "female"]),
            created_at=f.date_this_decade(before_today=True, after_today=False)
        ).__dict__
        for cust_id in cust_ids
    ]

def _get_subscription_status(custs: list[dict]) -> list[dict]:
    """
    Create fake subscription status for customer
    """
    f = Faker()
    results = []
    for cust in custs:
        end_date = f.date_this_decade(after_today=True, before_today=False)
        results.append(SubscriptionStatus(
                id = uuid4(),
                customer_id = cust["id"],
                plan_id = np.random.randint(1, 17),
                start_date = f.date_between(start_date=cust["created_at"], end_date=end_date),
                end_date = end_date
        ).__dict__)
    return results

def _get_call_records(cust_ids: list[UUID]) -> list[dict]:
    """
    Create fake call reords for customer
    """
    f = Faker()
    call_types = ["Outgoing Call", "Incoming Call",  "Data Usage Inquiry", "Network Issue", "Billing Inquiry", "Technical Support", "Account Update"]
    return [
        CallRecord(
            id=uuid4(),
            customer_id=np.random.choice(cust_ids),
            call_date=f.date_this_month(),
            call_duration=np.random.exponential(10),
            call_type=np.random.choice(call_types),
            location_id=np.random.randint(1, 51)
        ).__dict__
        for _ in range(1000)
    ]


def _get_billing_infos(active_subscriptions: list[dict]) -> list[dict]:
    """
    Create fake billing statements for customer
    """
    f = Faker()
    results = []
    for s in active_subscriptions:
        fake_billing_date = f.date_between(start_date="now", end_date=s["end_date"])
        fake_data_charges = np.random.uniform(0, 100)
        fake_roaming_charges = np.random.uniform(0, 500)
        fake_total_charges = np.random.uniform(fake_data_charges + fake_roaming_charges, 1000)
        results.append(BillingInfo(
                id=str(s["customer_id"]) + '_' + str(s["plan_id"]) + "_" + fake_billing_date.strftime("%m-%Y"),
                customer_id=str(s["customer_id"]),
                plan_id=str(s["plan_id"]),
                billing_date=fake_billing_date.strftime("%Y-%m-%d"),
                total_charges=fake_total_charges,
                data_charges=fake_data_charges,
                roaming_charges=fake_roaming_charges,
                data_usage=np.random.uniform(0, 20),
                sms_count=np.random.randint(0, 50),
            ).__dict__
        )
    return results

def _customer_data_insert_query() -> str:
    return """
    INSERT INTO Customer (
        id,
        first_name,
        last_name,
        email,
        phone_number,
        district_code,
        date_of_birth,
        gender,
        created_at
    )
    VALUES (
        %(id)s,
        %(first_name)s,
        %(last_name)s,
        %(email)s,
        %(phone_number)s,
        %(district_code)s,
        %(date_of_birth)s,
        %(gender)s,
        %(created_at)s
    )
    """

def _subscription_status_insert_query() -> str:
    return """
    INSERT INTO SubscriptionStatus (
        id,
        customer_id,
        plan_id,
        start_date,
        end_date
    )
    VALUES (
        %(id)s,
        %(customer_id)s,
        %(plan_id)s,
        %(start_date)s,
        %(end_date)s
    )
    """


def create_bucket(name: str) -> storage.bucket.Bucket:
    """Create bucket on GCS if it is not exist"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(name)
    if bucket.exists():
        logging.info('Bucket already exists')
    else:
        # Create the new bucket
        bucket = storage_client.create_bucket(name, location='asia-east2')
        logging.info('Bucket {} created'.format(bucket.name))
    return bucket

def generate_data(iteration: int, calls_bucket: str = "telecom_de") -> None:
    cust_ids = [uuid4() for _ in range(1000)]
    customers = _get_customers(cust_ids)
    subpscription = _get_subscription_status(customers)
    call_records = _get_call_records(cust_ids)
    billing_infos = _get_billing_infos(subpscription)

    # Send call data to GCS
    # Create bucket if not exists
    bucket = create_bucket(calls_bucket)
    # Upload csv file 
    blob = bucket.blob(f"src_calls/data_{str(iteration)}.csv")
    with blob.open("w", newline='') as f:
        write_dict_list_to_csv(f, call_records)
        logging.info(f"CSV file 'src_calls/data_{str(iteration)}.csv' was successfully uploaded to bucket '{calls_bucket}'")
    
    with DatabaseConnection().managed_cursor() as curr:
        # send customers data to telecomdb
        p.execute_batch(curr, _customer_data_insert_query(), customers)
        
        # send subscritions data to telecomdb
        p.execute_batch(curr, _subscription_status_insert_query(), subpscription)

    # send billing data to hosted fast api 
    res = requests.post("http://localhost:8000/billings", json={"data":billing_infos})
    
    logging.info(res.json()["message"])

class DatabaseConnection:
    def __init__(self):
        # DO NOT HARDCODE !!!
        self.conn_url = (
            "postgresql://postgres:password123@localhost:5432/telecomdb"
        )

    @contextmanager
    def managed_cursor(self, cursor_factory=None):
        self.conn = psycopg2.connect(self.conn_url)
        self.conn.autocommit = True
        self.curr = self.conn.cursor(cursor_factory=cursor_factory)
        try:
            yield self.curr
        finally:
            self.curr.close()
            self.conn.close()

if __name__ == "__main__":
    for itr in range(1, 21):
        generate_data(itr)
        sleep(10)