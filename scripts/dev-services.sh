#!/usr/bin/env bash
# Manage the sample's dev Postgres for the compose-managed test.
#
#   ./scripts/dev-services.sh up    # start a throwaway Postgres
#   ./scripts/dev-services.sh down  # stop and remove it
#   ./scripts/dev-services.sh url   # print the DATABASE_URL to point at
#
# The compose-managed deployment the sample's README describes runs its
# own `db` service; this script is the dev loop's shortcut for the tests
# that want a real Postgres without the images — one throwaway container
# with the same credentials and database, published on 127.0.0.1:55432.
#
# Docker and podman both work; whichever is on PATH wins. The default
# image is postgres:17, matching the compose file.
set -euo pipefail

NAME="eventyr-sample-pg"
PORT="${EVENTYR_SAMPLE_PG_PORT:-55432}"
IMAGE="${EVENTYR_SAMPLE_PG_IMAGE:-docker.io/library/postgres:17}"
USER="postgres"
PASSWORD="postgres"
DB="eventyr"

runtime() {
    if command -v podman >/dev/null 2>&1; then
        echo podman
    elif command -v docker >/dev/null 2>&1; then
        echo docker
    else
        echo "dev-services: need podman or docker on PATH" >&2
        exit 1
    fi
}

cmd="${1:-}"
case "$cmd" in
    up)
        rt="$(runtime)"
        if "$rt" container exists "$NAME" >/dev/null 2>&1; then
            "$rt" start "$NAME" >/dev/null
        else
            "$rt" run -d --name "$NAME" \
                -e POSTGRES_USER="$USER" \
                -e POSTGRES_PASSWORD="$PASSWORD" \
                -e POSTGRES_DB="$DB" \
                -p "127.0.0.1:${PORT}:5432" \
                "$IMAGE" >/dev/null
        fi
        # Wait for readiness so a test run right after `up` connects.
        for _ in $(seq 1 30); do
            if "$rt" exec "$NAME" pg_isready -U "$USER" >/dev/null 2>&1; then
                break
            fi
            sleep 0.5
        done
        echo "dev-services: postgres up at $(dirname "$0")/dev-services.sh url"
        "$0" url
        ;;
    down)
        rt="$(runtime)"
        "$rt" rm -f "$NAME" >/dev/null 2>&1 || true
        echo "dev-services: postgres down"
        ;;
    url)
        echo "postgres://${USER}:${PASSWORD}@127.0.0.1:${PORT}/${DB}"
        ;;
    *)
        echo "usage: $0 up|down|url" >&2
        exit 2
        ;;
esac
