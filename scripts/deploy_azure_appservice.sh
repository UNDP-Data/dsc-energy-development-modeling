#!/usr/bin/env bash
set -euo pipefail

APP_NAME="undphq220api001"
SKIP_DEPLOY=0
AUTH_MODE="local_password"
AUTH_ADMIN_EMAIL="daniele.ciminieri@gmail.com"
AUTH_SEED_PASSWORD=""
AUTH_SESSION_SECRET=""
AUTH_EMAIL_CONNECTION_STRING=""
AUTH_EMAIL_SENDER=""
AUTH_EMAIL_BASE_URL=""
RESOURCE_GROUP="undphq220rg03"

usage() {
  echo "Usage: $0 [--app-name NAME] [--skip-deploy] [--auth-mode MODE] [--auth-admin-email EMAIL]"
  echo "          [--auth-seed-password PW] [--auth-session-secret SECRET]"
  echo "          [--auth-email-connection-string CONN] [--auth-email-sender SENDER] [--auth-email-base-url URL]"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --app-name) APP_NAME="$2"; shift 2 ;;
    --skip-deploy) SKIP_DEPLOY=1; shift ;;
    --auth-mode) AUTH_MODE="$2"; shift 2 ;;
    --auth-admin-email) AUTH_ADMIN_EMAIL="$2"; shift 2 ;;
    --auth-seed-password) AUTH_SEED_PASSWORD="$2"; shift 2 ;;
    --auth-session-secret) AUTH_SESSION_SECRET="$2"; shift 2 ;;
    --auth-email-connection-string) AUTH_EMAIL_CONNECTION_STRING="$2"; shift 2 ;;
    --auth-email-sender) AUTH_EMAIL_SENDER="$2"; shift 2 ;;
    --auth-email-base-url) AUTH_EMAIL_BASE_URL="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "Repo root : $REPO_ROOT"

bash "$REPO_ROOT/scripts/stage_api_package.sh"

ZIP_PATH="$REPO_ROOT/outputs/azure-deploy/edim-api.zip"
if [ "$SKIP_DEPLOY" = "1" ]; then
  echo "--skip-deploy set; stopping before az webapp deploy."
  exit 0
fi

echo "[4/4] Deploying to $APP_NAME ($RESOURCE_GROUP)..."
az webapp config appsettings delete \
  --resource-group "$RESOURCE_GROUP" \
  --name "$APP_NAME" \
  --setting-names WEBSITE_RUN_FROM_PACKAGE \
  --output none || true

az webapp config appsettings set \
  --resource-group "$RESOURCE_GROUP" \
  --name "$APP_NAME" \
  --settings "SCM_DO_BUILD_DURING_DEPLOYMENT=true" "WEBSITES_PORT=8000" "EDIM_RUNS_DIR=/home/edim-data/runs" "PYTHONPATH=/home/site/wwwroot:/home/site/wwwroot/model_runtime" \
  --output none

SESSION_SECRET="$AUTH_SESSION_SECRET"
if [ -z "$SESSION_SECRET" ]; then
  SESSION_SECRET="$(az webapp config appsettings list --resource-group "$RESOURCE_GROUP" --name "$APP_NAME" --query "[?name=='EDIM_AUTH_SESSION_SECRET'].value | [0]" --output tsv 2>/dev/null || true)"
fi
if [ -z "$SESSION_SECRET" ] || [ "$SESSION_SECRET" = "None" ]; then
  SESSION_SECRET="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
  echo "Generated EDIM_AUTH_SESSION_SECRET."
fi

az webapp config appsettings set \
  --resource-group "$RESOURCE_GROUP" \
  --name "$APP_NAME" \
  --settings "EDIM_AUTH_MODE=$AUTH_MODE" "EDIM_AUTH_ADMIN_EMAIL=$AUTH_ADMIN_EMAIL" "EDIM_AUTH_SESSION_SECRET=$SESSION_SECRET" \
  --output none

if [ -n "$AUTH_SEED_PASSWORD" ]; then
  az webapp config appsettings set \
    --resource-group "$RESOURCE_GROUP" \
    --name "$APP_NAME" \
    --settings "EDIM_AUTH_SEED_PASSWORD=$AUTH_SEED_PASSWORD" \
    --output none
fi

if [ -n "$AUTH_EMAIL_CONNECTION_STRING" ] && [ -n "$AUTH_EMAIL_SENDER" ]; then
  az webapp config appsettings set \
    --resource-group "$RESOURCE_GROUP" \
    --name "$APP_NAME" \
    --settings "EDIM_AUTH_EMAIL_CONNECTION_STRING=$AUTH_EMAIL_CONNECTION_STRING" "EDIM_AUTH_EMAIL_SENDER=$AUTH_EMAIL_SENDER" "EDIM_AUTH_DEV_TOKEN_DISPLAY=false" \
    --output none
  if [ -n "$AUTH_EMAIL_BASE_URL" ]; then
    az webapp config appsettings set \
      --resource-group "$RESOURCE_GROUP" \
      --name "$APP_NAME" \
      --settings "EDIM_AUTH_EMAIL_BASE_URL=$AUTH_EMAIL_BASE_URL" \
      --output none
  fi
  echo "Reset e-mails enabled via ACS sender $AUTH_EMAIL_SENDER."
fi

az webapp config set \
  --resource-group "$RESOURCE_GROUP" \
  --name "$APP_NAME" \
  --startup-file "python -m uvicorn api_service.main:app --host 0.0.0.0 --port 8000" \
  --output none

az webapp deploy \
  --resource-group "$RESOURCE_GROUP" \
  --name "$APP_NAME" \
  --src-path "$ZIP_PATH" \
  --type zip \
  --async true \
  --output none

echo ""
echo "Deployed. App URL: https://$APP_NAME.azurewebsites.net"
echo "UI:  https://$APP_NAME.azurewebsites.net/ui/"
echo "Auth mode : $AUTH_MODE"
echo "Admin user: $AUTH_ADMIN_EMAIL"
if [ -n "$AUTH_SEED_PASSWORD" ]; then
  echo "Admin password: EDIM_AUTH_SEED_PASSWORD is set; sign in with it and change it after first sign-in."
else
  echo "Admin account starts inactive. Open the one-time invite link from the log stream:"
  echo "  az webapp log tail --resource-group $RESOURCE_GROUP --name $APP_NAME"
fi
