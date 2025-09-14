#!/bin/bash
set -e

# Start Postgres in the background
docker-entrypoint.sh postgres &

# Wait for Postgres to be ready
until pg_isready -U "$POSTGRES_USER"; do
  sleep 2
done

BACKUP_FILE="/docker-entrypoint-initdb.d/backup.sql"

# Check if the backup file exists
if [ -f "$BACKUP_FILE" ]; then
  echo "Backup file found. Restoring..."
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < "$BACKUP_FILE"
  echo "Restore completed."
else
  echo "No backup file found at $BACKUP_FILE. Skipping restore."
fi

# Keep container running
wait
