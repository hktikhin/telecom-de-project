CREATE TABLE Customer (
    id UUID PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50) UNIQUE,
    phone_number CHAR(14),
    district_code SMALLINT,
    date_of_birth DATE,
    gender VARCHAR(20),
    created_at DATE
);

CREATE TABLE MobilePlan (
    id INTEGER PRIMARY KEY,
    plan_name VARCHAR(20),
    plan_type VARCHAR(20),
    plan_price FLOAT
);

CREATE TABLE SubscriptionStatus (
    id UUID PRIMARY KEY,
    customer_id UUID,
    plan_id SMALLINT,
    start_date DATE,
    end_date DATE,
    FOREIGN KEY (customer_id) REFERENCES Customer(id),
    FOREIGN KEY (plan_id) REFERENCES MobilePlan(id)
);