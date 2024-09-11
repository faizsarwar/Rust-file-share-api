#!/bin/bash
set -e

# Wait for PostgreSQL to start
until pg_isready -h postgres -p 5432 -U "$POSTGRES_USER"; do
  echo "Waiting for PostgreSQL to start..."
  sleep 2
done

# Create the table
psql -h postgres -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
CREATE TABLE IF NOT EXISTS files (
    id UUID PRIMARY KEY,
    file_id UUID NOT NULL,
    part_number INT NOT NULL,
    data BYTEA NOT NULL
);
"
