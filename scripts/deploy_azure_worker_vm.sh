#!/usr/bin/env bash
set -euo pipefail

VM_NAME="undphq220vm001"
VM_SIZE="Standard_B4s_v2"
OS_DISK_GB=64
LOCATION="westeurope"
ADMIN_USER="edimadmin"
GIT_REPO="https://github.com/Ben-Keller/energy-development-modeling.git"
GIT_BRANCH="develop"
SSH_SOURCE_IP=""
SKIP_CONFIRMATION=0
RESOURCE_GROUP="undphq220rg03"
SUBSCRIPTION="72ccc0bc-4fb9-4b1d-95c0-a897f6e4ff74"
SERVICEBUS_NAMESPACE="undphq220sb001.servicebus.windows.net"
STORAGE_ACCOUNT="undphq220st02"
BLOB_ACCOUNT_URL="https://undphq220st02.blob.core.windows.net/"

usage() {
  echo "Usage: $0 [plan|status|start|stop|update-image|create] [options]"
  echo "  --vm-name NAME --vm-size SIZE --os-disk-gb N --location LOC"
  echo "  --admin-user USER --git-repo URL --git-branch BRANCH"
  echo "  --ssh-source-ip CIDR --yes"
}

CMD="${1:-plan}"
case "$CMD" in
  plan|status|start|stop|update-image|create) shift || true ;;
  -h|--help) usage; exit 0 ;;
  -*) CMD="plan" ;;
  *) echo "Unknown command: $CMD" >&2; usage; exit 1 ;;
esac

while [ $# -gt 0 ]; do
  case "$1" in
    --vm-name) VM_NAME="$2"; shift 2 ;;
    --vm-size) VM_SIZE="$2"; shift 2 ;;
    --os-disk-gb) OS_DISK_GB="$2"; shift 2 ;;
    --location) LOCATION="$2"; shift 2 ;;
    --admin-user) ADMIN_USER="$2"; shift 2 ;;
    --git-repo) GIT_REPO="$2"; shift 2 ;;
    --git-branch) GIT_BRANCH="$2"; shift 2 ;;
    --ssh-source-ip) SSH_SOURCE_IP="$2"; shift 2 ;;
    --yes) SKIP_CONFIRMATION=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KEY_DIR="$REPO_ROOT/.local/worker-vm"
SSH_KEY="$KEY_DIR/id_edim_worker"
NSG_NAME="${VM_NAME}-nsg"

echo "EDIM worker VM"
echo "  resource group : $RESOURCE_GROUP"
echo "  subscription   : $SUBSCRIPTION"
echo "  vm             : $VM_NAME ($VM_SIZE, $LOCATION)"
echo "  repo           : $GIT_REPO @ $GIT_BRANCH"
echo ""

vm_exists() { az vm show -g "$RESOURCE_GROUP" -n "$VM_NAME" -o none 2>/dev/null; }

show_required_roles() {
  local principal="$1"
  local stg_scope="/subscriptions/$SUBSCRIPTION/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT"
  local sb_scope="/subscriptions/$SUBSCRIPTION/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.ServiceBus/namespaces/undphq220sb001"
  echo ""
  echo "OPTIONAL: managed-identity roles (NOT required while Path B is in use)."
  echo "  az role assignment create --assignee-object-id $principal --assignee-principal-type ServicePrincipal --role \"Storage Blob Data Contributor\" --scope \"$stg_scope\""
  echo "  az role assignment create --assignee-object-id $principal --assignee-principal-type ServicePrincipal --role \"Azure Service Bus Data Owner\" --scope \"$sb_scope\""
  echo ""
}

case "$CMD" in
  plan)
    if vm_exists; then
      echo "VM already exists."
      az vm show -g "$RESOURCE_GROUP" -n "$VM_NAME" --query "{power:powerState, principal:identity.principalId}" -o json
    else
      echo "VM does not exist yet. 'create' would: generate SSH keypair, create VM ($VM_SIZE, ${OS_DISK_GB}GB, Ubuntu 22.04), create NSG '$NSG_NAME', install Docker via cloud-init, clone repo, build image, install systemd unit."
      echo "The VM stays STOPPED until you run 'start' (cost lever)."
    fi
    ;;
  status)
    if ! vm_exists; then echo "VM '$VM_NAME' does not exist."; exit 0; fi
    az vm get-instance-view -g "$RESOURCE_GROUP" -n "$VM_NAME" --query "{name:name,power:instanceView.statuses[?starts_with(code,'PowerState')].displayStatus | [0],ip:publicIps,size:hardwareProfile.vmSize,principal:identity.principalId}" -o json
    ;;
  start)
    az vm start -g "$RESOURCE_GROUP" -n "$VM_NAME" -o none
    echo "VM started. It bills while running - remember to stop when idle."
    ;;
  stop)
    az vm deallocate -g "$RESOURCE_GROUP" -n "$VM_NAME" -o none
    echo "VM deallocated. Compute billing stopped (disk + IP still bill)."
    ;;
  update-image)
    if ! vm_exists; then echo "VM '$VM_NAME' does not exist. Run create first." >&2; exit 1; fi
    echo "Rebuilding the worker image on the VM (git pull + docker build)..."
    SCRIPT="$(cat <<EOF
set -e
cd /opt/edim
git fetch --all
git checkout $GIT_BRANCH
git pull --ff-only origin $GIT_BRANCH
docker build -f worker/Dockerfile -t edim-worker:latest .
systemctl restart edim-worker
sleep 5
systemctl --no-pager status edim-worker || true
EOF
)"
    PAYLOAD="$(printf '%s' "$SCRIPT" | base64 -w0)"
    az vm run-command invoke -g "$RESOURCE_GROUP" -n "$VM_NAME" --command-id RunShellScript --scripts "echo '$PAYLOAD' | base64 -d > /tmp/edim-update-image.sh && bash /tmp/edim-update-image.sh" --query "value[0].message" -o tsv
    ;;
  create)
    if vm_exists; then echo "VM '$VM_NAME' already exists - nothing to create."; exit 0; fi
    if [ "$SKIP_CONFIRMATION" != "1" ]; then
      echo "This creates a billable VM in $RESOURCE_GROUP ($VM_SIZE). Keep it stopped when idle."
      read -r -p "Type 'create' to proceed: " answer
      [ "$answer" = "create" ] || { echo "Aborted."; exit 1; }
    fi
    mkdir -p "$KEY_DIR"
    if [ ! -f "$SSH_KEY" ]; then
      echo "[1/5] Generating SSH keypair..."
      ssh-keygen -t rsa -b 4096 -f "$SSH_KEY" -N "" -C edim-worker -q
    else
      echo "[1/5] Reusing existing SSH keypair."
    fi
    [ -f "$SSH_KEY.pub" ] || { echo "SSH public key not found." >&2; exit 1; }
    echo "[2/5] Reading connection strings (values not displayed)..."
    SB="$(az servicebus namespace authorization-rule keys list -g "$RESOURCE_GROUP" --namespace-name "undphq220sb001" --name RootManageSharedAccessKey --query "primaryConnectionString" -o tsv)"
    ST="$(az storage account show-connection-string -g "$RESOURCE_GROUP" -n "$STORAGE_ACCOUNT" --query "connectionString" -o tsv)"
    [ -n "$SB" ] && [ -n "$ST" ] || { echo "Could not read connection strings." >&2; exit 1; }
    echo "      service bus: ${#SB} chars, storage: ${#ST} chars"
    printf '# Worker VM credentials (git-ignored)\n\n## EDIM_SERVICEBUS_CONNECTION_STRING\n```\n%s\n```\n\n## EDIM_BLOB_CONNECTION_STRING\n```\n%s\n```\n' "$SB" "$ST" > "$KEY_DIR/worker-secrets.md"
    echo "[3/5] Preparing cloud-init..."
    {
      echo "EDIM_SERVICEBUS_NAMESPACE=$SERVICEBUS_NAMESPACE"
      echo "EDIM_SERVICEBUS_QUEUE_NAME=execution-queue"
      echo "EDIM_SERVICEBUS_CANCELLATION_QUEUE_NAME=cancellation-queue"
      echo "EDIM_SERVICEBUS_COMPLETION_QUEUE_NAME=completion-queue"
      echo "EDIM_BLOB_ACCOUNT_URL=$BLOB_ACCOUNT_URL"
      echo "EDIM_BLOB_CONTAINER_PREFIX=stg-"
      echo "EDIM_SOLVER=highs"
      echo "EDIM_DEVELOPMENT_ENGINE=mario"
      echo "EDIM_MARIO_TIMEOUT_SECONDS=120"
      echo "EDIM_MARIO_FAIL_ON_ERROR=false"
      echo "EDIM_WORKER_MAX_RUN_SECONDS=14400"
      echo "EDIM_WORKER_LOCK_RENEWAL_SECONDS=60"
      echo "EDIM_SERVICEBUS_CONNECTION_STRING=$SB"
      echo "EDIM_BLOB_CONNECTION_STRING=$ST"
    } > "$KEY_DIR/.env.worker.tmp"
    ENV_LINES="$(cat "$KEY_DIR/.env.worker.tmp")"
    rm -f "$KEY_DIR/.env.worker.tmp"
    cat > "$KEY_DIR/cloud-init.yaml" <<EOF
#cloud-config
package_update: true
packages:
  - docker.io
  - git
  - ca-certificates
runcmd:
  - systemctl enable --now docker
  - mkdir -p /opt/edim
  - git clone --branch $GIT_BRANCH $GIT_REPO /opt/edim
  - |
    cat > /opt/edim/.env.worker <<'ENVEOF'
$ENV_LINES
ENVEOF
  - cd /opt/edim && docker build -f worker/Dockerfile -t edim-worker:latest .
  - |
    cat > /etc/systemd/system/edim-worker.service <<'UNITEOF'
    [Unit]
    Description=EDIM worker (isolated model execution)
    After=docker.service network-online.target
    Requires=docker.service
    [Service]
    Restart=always
    RestartSec=10
    ExecStartPre=-/usr/bin/docker rm -f edim-worker
    ExecStart=/usr/bin/docker run --rm --name edim-worker --env-file /opt/edim/.env.worker -v /opt/edim/outputs:/app/outputs edim-worker:latest
    ExecStop=/usr/bin/docker stop -t 30 edim-worker
    [Install]
    WantedBy=multi-user.target
    UNITEOF
  - systemctl daemon-reload
  - systemctl enable --now edim-worker
EOF
    echo "[4/5] Creating NSG '$NSG_NAME'..."
    az network nsg create -g "$RESOURCE_GROUP" -n "$NSG_NAME" -l "$LOCATION" -o none
    if [ -n "$SSH_SOURCE_IP" ]; then
      az network nsg rule create -g "$RESOURCE_GROUP" --nsg-name "$NSG_NAME" -n AllowSshFromRestrictedSource --priority 1000 --direction Inbound --access Allow --protocol Tcp --source-address-prefixes "$SSH_SOURCE_IP" --destination-port-ranges 22 -o none
    fi
    echo "[5/5] Creating VM (this takes a few minutes)..."
    az vm create --resource-group "$RESOURCE_GROUP" --name "$VM_NAME" --location "$LOCATION" --image "Ubuntu2204" --size "$VM_SIZE" --admin-username "$ADMIN_USER" --ssh-key-values "$SSH_KEY.pub" --os-disk-size-gb "$OS_DISK_GB" --storage-sku Standard_LRS --public-ip-sku Standard --nsg "$NSG_NAME" --encryption-at-host --assign-identity --custom-data "$KEY_DIR/cloud-init.yaml" --output none
    echo "VM created."
    ;;
esac
