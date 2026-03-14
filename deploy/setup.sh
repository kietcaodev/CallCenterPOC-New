#!/bin/bash
# Setup script for CallCenterPOC on Debian 12
# Run as root: bash /opt/CallCenterPOC-New/deploy/setup.sh

set -e

APP_DIR="/opt/CallCenterPOC-New"

echo "=== Building projects ==="
cd "$APP_DIR"
dotnet restore
dotnet build --configuration Release

echo "=== Installing systemd services ==="
cp "$APP_DIR/deploy/callcenter-api.service" /etc/systemd/system/
cp "$APP_DIR/deploy/callcenter-app.service" /etc/systemd/system/

# Update service files to use Release build
sed -i 's|dotnet run --no-build|dotnet run --no-build --configuration Release|g' /etc/systemd/system/callcenter-api.service
sed -i 's|dotnet run --no-build|dotnet run --no-build --configuration Release|g' /etc/systemd/system/callcenter-app.service

systemctl daemon-reload
systemctl enable callcenter-api callcenter-app

echo "=== Done ==="
echo ""
echo "Next steps:"
echo "  1. Edit $APP_DIR/ContactCenter-API/appsettings.json (add Azure OpenAI keys)"
echo "  2. systemctl start callcenter-api"
echo "  3. systemctl start callcenter-app"
echo "  4. Check status: systemctl status callcenter-api callcenter-app"
echo "  5. View logs: journalctl -u callcenter-api -f"
