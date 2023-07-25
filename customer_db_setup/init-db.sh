#!/bin/bash
# RUN chmod +x /docker-entrypoint-initdb.d/init-db.sh?
set -e

# Wait for PostgreSQL to start
until pg_isready -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER
do
    echo "Waiting for PostgreSQL to start..."
    sleep 1
done

# Create a new database and table
psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "\copy MobilePlan(id,plan_name,plan_type,plan_price) FROM '/data/plans.csv' DELIMITER ',' CSV HEADER;"