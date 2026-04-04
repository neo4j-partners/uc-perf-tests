#!/usr/bin/env bash
# Delete old Databricks job runs. By default keeps the 5 most recent runs.
#
# Usage:
#   ./cleanup_runs.sh              # delete all but 5 most recent
#   ./cleanup_runs.sh --keep 10    # delete all but 10 most recent
#   ./cleanup_runs.sh --all        # delete every run
#   ./cleanup_runs.sh --dry-run    # show what would be deleted

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/.env" 2>/dev/null || true

PROFILE="${DATABRICKS_PROFILE:?Set DATABRICKS_PROFILE in .env}"
KEEP=5
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep)   KEEP="$2"; shift 2 ;;
    --all)    KEEP=0; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    *) echo "Usage: $0 [--keep N | --all] [--dry-run]"; exit 1 ;;
  esac
done

echo "Fetching runs (profile: $PROFILE)..."
RUNS_JSON=$(databricks --profile "$PROFILE" jobs list-runs --output json)

# Extract run IDs ordered by start_time descending (most recent first)
RUN_IDS=$(echo "$RUNS_JSON" | python3 -c "
import json, sys
data = json.load(sys.stdin)
runs = data if isinstance(data, list) else data.get('runs', [])
runs.sort(key=lambda r: r.get('start_time', 0), reverse=True)
for r in runs:
    print(r['run_id'])
")

TOTAL=$(echo "$RUN_IDS" | wc -l | tr -d ' ')
TO_DELETE=$(echo "$RUN_IDS" | tail -n +$((KEEP + 1)))
DELETE_COUNT=$(echo "$TO_DELETE" | grep -c . || true)

echo "Total runs: $TOTAL"
echo "Keeping: $KEEP most recent"
echo "Deleting: $DELETE_COUNT"
echo ""

if [[ "$DELETE_COUNT" -eq 0 ]]; then
  echo "Nothing to delete."
  exit 0
fi

if $DRY_RUN; then
  echo "[DRY RUN] Would delete these run IDs:"
  echo "$TO_DELETE"
  exit 0
fi

DELETED=0
FAILED=0
for RUN_ID in $TO_DELETE; do
  if databricks --profile "$PROFILE" jobs delete-run "$RUN_ID" 2>/dev/null; then
    DELETED=$((DELETED + 1))
  else
    echo "  Failed to delete run $RUN_ID"
    FAILED=$((FAILED + 1))
  fi
done

echo ""
echo "Done. Deleted: $DELETED, Failed: $FAILED, Kept: $KEEP"
