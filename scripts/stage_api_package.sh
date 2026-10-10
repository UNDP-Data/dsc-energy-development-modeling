#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$repo_root/outputs/azure-deploy"
stage_dir="$work_dir/staging"
zip_path="$work_dir/edim-api.zip"

api_requirements="$repo_root/backend/requirements-api.txt"
if [ ! -f "$api_requirements" ]; then
    echo "Missing backend/requirements-api.txt - refusing to deploy the full solver stack to App Service." >&2
    exit 1
fi

rm -rf "$stage_dir" "$zip_path"
mkdir -p "$stage_dir"

cache_excludes=(--exclude='__pycache__' --exclude='*.pyc' --exclude='.pytest_cache' --exclude='.mypy_cache')

copy_tree() {
    local src="$1" dst="$2"
    shift 2
    if [ ! -e "$src" ]; then
        echo "Missing source, skipped: $src" >&2
        return 0
    fi
    mkdir -p "$dst"
    rsync -a "$@" "${cache_excludes[@]}" "$src/" "$dst/"
}

echo "[1/3] Copying backend application package..."
copy_tree "$repo_root/backend/api_service" "$stage_dir/api_service"
copy_tree "$repo_root/backend/migrations" "$stage_dir/migrations"
cp "$repo_root/backend/alembic.ini" "$stage_dir/alembic.ini"
cp "$api_requirements" "$stage_dir/requirements.txt"

echo "[2/3] Copying frontend, inputs, and model runtime..."
copy_tree "$repo_root/frontend" "$stage_dir/frontend" --exclude=node_modules --exclude=dist --exclude=tests --exclude=.vite
copy_tree "$repo_root/inputs" "$stage_dir/inputs"
copy_tree "$repo_root/model_runtime/edim_model" "$stage_dir/model_runtime/edim_model"
copy_tree "$repo_root/model_runtime/model_modules/calliope/Calliope-Africa-main" \
    "$stage_dir/model_runtime/model_modules/calliope/Calliope-Africa-main" \
    --exclude=Timeseries

report_doc="$repo_root/Energy Modelling Scenario Report.docx"
if [ -f "$report_doc" ]; then
    cp "$report_doc" "$stage_dir/Energy Modelling Scenario Report.docx"
else
    echo "Scenario report docx not found at repo root; skipping." >&2
fi

echo "[3/3] Creating zip archive..."
(cd "$stage_dir" && zip -qr "$zip_path" .)

zip_mb="$(du -m "$zip_path" | cut -f1)"
echo "Zip size: ${zip_mb} MB"
echo "Zip: $zip_path"
