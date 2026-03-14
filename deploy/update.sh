#!/bin/bash
# Update and restart CallCenterPOC services
# Run as root: bash /opt/CallCenterPOC-New/deploy/update.sh

set -e

APP_DIR="/opt/CallCenterPOC-New"

echo "=== Pulling latest code ==="
cd "$APP_DIR"
git pull

echo "=== Building ==="
dotnet build --configuration Release

echo "=== Restarting services ==="
systemctl restart callcenter-api callcenter-app

echo "=== Done ==="
systemctl status callcenter-api callcenter-app --no-pager -l
