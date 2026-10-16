#!/usr/bin/env bash
# Run after `wrangler login` and `wrangler d1 create heartbeat-edge-data`.
set -euo pipefail

database_id="${1:-}"
if [[ ! "$database_id" =~ ^[0-9a-fA-F-]{36}$ ]]; then
  echo "Usage: bash setup.sh D1_DATABASE_ID" >&2
  exit 2
fi

cd "$(dirname "$0")"
if [[ -e wrangler.toml ]]; then
  echo "wrangler.toml already exists; check its database binding before deploying." >&2
  exit 1
fi

sed "s/REPLACE_WITH_D1_DATABASE_ID/$database_id/" wrangler.toml.example > wrangler.toml
npm ci
npx wrangler d1 execute heartbeat-edge-data --remote --file schema.sql
npx wrangler deploy
echo "Enter a new random device token at the Wrangler prompt; keep it out of this repository."
npx wrangler secret put API_TOKEN
