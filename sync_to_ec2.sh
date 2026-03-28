#!/bin/bash
# Pulls latest emory_med.db from GitHub and merges into prospect_engine on EC2.
# Run via cron: 37 * * * * /home/ubuntu/emory-med-finder/sync_to_ec2.sh >> /home/ubuntu/emory-med-finder/sync.log 2>&1

set -e
cd "$(dirname "$0")"

echo "=== $(date) ==="

# Pull latest from GitHub
git pull --quiet origin main

# Merge new contacts into prospect_engine DB
python3 merge_to_prospect_engine.py

echo ""
