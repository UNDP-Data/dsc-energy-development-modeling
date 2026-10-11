from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List


_DOTENV_LOADED = False


def load_local_env(repo_root: Path | None = None) -> None:
    """Load local .env values without adding a dependency or overriding the process env."""
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    _DOTENV_LOADED = True
    root = repo_root or Path(__file__).resolve().parents[2]
    env_path = root / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        value = raw_value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _load_runtime_config(repo_root: Path) -> dict:
    load_local_env(repo_root)
    raw_path = os.getenv("EDIM_RUNTIME_CONFIG", "").strip()
    config_path = Path(raw_path).expanduser().resolve() if raw_path else repo_root / "inputs" / "runtime_config.json"
    if not config_path.exists():
        return {}
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _cfg(config: dict, path: list[str], default: Any = None) -> Any:
    node: Any = config
    for key in path:
        if not isinstance(node, dict) or key not in node:
            return default
        node = node[key]
    return node


def _resolve_path(repo_root: Path, value: str | Path | None, default: Path) -> Path:
    if not value:
        return default.resolve()
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return max(parsed, minimum)


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        parsed = float(raw)
    except ValueError:
        return default
    return max(parsed, minimum)


def _env_csv(name: str, default: List[str]) -> List[str]:
    raw = os.getenv(name)
    if raw is None:
        return default
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or default


def _config_list(value: Any, default: List[str]) -> List[str]:
    if isinstance(value, list):
        values = [str(item).strip() for item in value if str(item).strip()]
        return values or default
    if isinstance(value, str):
        values = [item.strip() for item in value.split(",") if item.strip()]
        return values or default
    return default


@dataclass(frozen=True)
class Settings:
    calliope_root: Path
    runs_dir: Path
    config_dir: Path

    dev_subset_start: str
    dev_subset_end: str
    analysis_subset_start: str
    analysis_subset_end: str
    dev_solver_time_limit_seconds: float
    analysis_solver_time_limit_seconds: float
    allow_full_year: bool
    solver: str

    cors_allow_origins: List[str]

    summary_max_generation_techs: int
    summary_max_generation_timesteps: int
    summary_max_category_rows: int
    summary_diagnostics_max_rows: int

    run_retention_days: int
    run_max_dirs: int

    job_history_limit: int
    job_dedupe_enabled: bool
    job_queue_capacity: int
    job_execution_max_attempts: int

    development_engine: str
    mario_db_path: str
    mario_timeout_seconds: float
    mario_fail_on_error: bool
    frontend_dir: Path | None = None
    runtime_config: dict | None = None
    model_runtime_mode: str = "subprocess"
    runtime_artifact_handoff_mode: str = "shared_filesystem"
    dataset_staging_mode: str = "copy_to_run"
    model_manifest_path: Path | None = None
    dataset_manifest_path: Path | None = None
    platform_store_backend: str = "sqlite"
    platform_sqlite_path: Path | None = None

    auth_mode: str = "test_header"
    auth_session_secret: str = "dev-only-change-me"
    auth_session_ttl_seconds: int = 43200
    auth_cookie_name: str = "edim_session"
    auth_cookie_secure: bool = True
    auth_password_min_length: int = 8
    auth_reset_ttl_seconds: int = 3600
    auth_invite_ttl_seconds: int = 172800
    auth_dev_token_display: bool = True
    auth_admin_email: str = "daniele.ciminieri@gmail.com"
    auth_email_connection_string: str = ""
    auth_email_sender: str = ""
    auth_email_base_url: str = ""


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for candidate in (here.parents[2], here.parents[1], Path.cwd()):
        if (candidate / "inputs" / "runtime_config.json").exists():
            return candidate
    return here.parents[2]


def get_settings() -> Settings:
    repo_root = _repo_root()
    runtime_config = _load_runtime_config(repo_root)

    calliope_root = _resolve_path(
        repo_root,
        os.getenv("EDIM_CALLIOPE_ROOT") or _cfg(runtime_config, ["paths", "calliope_root"]),
        repo_root / "model_runtime" / "model_modules" / "calliope" / "Calliope-Africa-main",
    )
    runs_dir = _resolve_path(
        repo_root,
        os.getenv("EDIM_RUNS_DIR") or _cfg(runtime_config, ["paths", "runs_dir"]),
        repo_root / "outputs" / "runs",
    )
    config_dir = _resolve_path(
        repo_root,
        os.getenv("EDIM_CONFIG_DIR") or _cfg(runtime_config, ["paths", "config_dir"]),
        repo_root / "inputs",
    )
    frontend_dir = _resolve_path(
        repo_root,
        os.getenv("EDIM_FRONTEND_DIR") or _cfg(runtime_config, ["paths", "frontend_dir"]),
        repo_root / "frontend",
    )
    model_manifest_path = _resolve_path(
        repo_root,
        os.getenv("EDIM_MODEL_MANIFEST_PATH") or _cfg(runtime_config, ["model_runtime", "manifest_path"]),
        repo_root / "model_runtime" / "edim_model" / "model_manifest.json",
    )
    dataset_manifest_path = _resolve_path(
        repo_root,
        os.getenv("EDIM_DATASET_MANIFEST_PATH") or _cfg(runtime_config, ["model_runtime", "dataset_manifest_path"]),
        repo_root / "model_runtime" / "edim_model" / "dataset_manifest.json",
    )
    platform_sqlite_path = _resolve_path(
        repo_root,
        os.getenv("EDIM_PLATFORM_SQLITE_PATH") or _cfg(runtime_config, ["storage", "platform_sqlite_path"]),
        repo_root / "outputs" / "platform" / "platform.sqlite3",
    )
    solver = (
        os.getenv("EDIM_SOLVER")
        or str(_cfg(runtime_config, ["runtime", "solver"], "highs"))
        or "highs"
    ).strip() or "highs"

    cors_allow_origins_default = _config_list(
        _cfg(runtime_config, ["runtime", "cors_allow_origins"]),
        [
            "http://localhost:8000",
            "http://127.0.0.1:8000",
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://localhost:4173",
            "http://127.0.0.1:4173",
            "http://localhost:4174",
            "http://127.0.0.1:4174",
            "http://localhost:4175",
            "http://127.0.0.1:4175",
            "http://localhost:4176",
            "http://127.0.0.1:4176",
            "http://localhost:4177",
            "http://127.0.0.1:4177",
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:5174",
            "http://127.0.0.1:5174",
            "http://localhost:5175",
            "http://127.0.0.1:5175",
            "http://localhost:5176",
            "http://127.0.0.1:5176",
            "http://localhost:5177",
            "http://127.0.0.1:5177",
        ],
    )
    cors_allow_origins = _env_csv("EDIM_CORS_ALLOW_ORIGINS", cors_allow_origins_default)

    return Settings(
        calliope_root=calliope_root,
        runs_dir=runs_dir,
        config_dir=config_dir,
        dev_subset_start=os.getenv("EDIM_DEV_SUBSET_START", str(_cfg(runtime_config, ["runtime", "dev_subset_start"], "2019-01-01"))),
        dev_subset_end=os.getenv("EDIM_DEV_SUBSET_END", str(_cfg(runtime_config, ["runtime", "dev_subset_end"], "2019-01-02"))),
        analysis_subset_start=os.getenv("EDIM_ANALYSIS_SUBSET_START", str(_cfg(runtime_config, ["runtime", "analysis_subset_start"], "2019-01-01"))),
        analysis_subset_end=os.getenv("EDIM_ANALYSIS_SUBSET_END", str(_cfg(runtime_config, ["runtime", "analysis_subset_end"], "2019-03-31"))),
        dev_solver_time_limit_seconds=_env_float("EDIM_DEV_SOLVER_TIME_LIMIT_SECONDS", float(_cfg(runtime_config, ["runtime", "dev_solver_time_limit_seconds"], 3600.0)), minimum=1.0),
        analysis_solver_time_limit_seconds=_env_float("EDIM_ANALYSIS_SOLVER_TIME_LIMIT_SECONDS", float(_cfg(runtime_config, ["runtime", "analysis_solver_time_limit_seconds"], 14400.0)), minimum=1.0),
        allow_full_year=_env_bool("EDIM_ALLOW_FULL_YEAR", bool(_cfg(runtime_config, ["runtime", "allow_full_year"], True))),
        solver=solver,
        cors_allow_origins=cors_allow_origins,
        summary_max_generation_techs=_env_int("EDIM_SUMMARY_MAX_GENERATION_TECHS", int(_cfg(runtime_config, ["summary", "max_generation_techs"], 40))),
        summary_max_generation_timesteps=_env_int("EDIM_SUMMARY_MAX_GENERATION_TIMESTEPS", int(_cfg(runtime_config, ["summary", "max_generation_timesteps"], 240))),
        summary_max_category_rows=_env_int("EDIM_SUMMARY_MAX_CATEGORY_ROWS", int(_cfg(runtime_config, ["summary", "max_category_rows"], 100))),
        summary_diagnostics_max_rows=_env_int("EDIM_SUMMARY_DIAGNOSTICS_MAX_ROWS", int(_cfg(runtime_config, ["summary", "diagnostics_max_rows"], 200))),
        run_retention_days=_env_int("EDIM_RUN_RETENTION_DAYS", int(_cfg(runtime_config, ["runs", "retention_days"], 30)), minimum=0),
        run_max_dirs=_env_int("EDIM_RUN_MAX_DIRS", int(_cfg(runtime_config, ["runs", "max_dirs"], 200)), minimum=0),
        job_history_limit=_env_int("EDIM_JOB_HISTORY_LIMIT", int(_cfg(runtime_config, ["jobs", "history_limit"], 200))),
        job_dedupe_enabled=_env_bool("EDIM_JOB_DEDUPE_ENABLED", bool(_cfg(runtime_config, ["jobs", "dedupe_enabled"], True))),
        job_queue_capacity=_env_int("EDIM_JOB_QUEUE_CAPACITY", int(_cfg(runtime_config, ["jobs", "queue_capacity"], 12))),
        job_execution_max_attempts=_env_int("EDIM_JOB_EXECUTION_MAX_ATTEMPTS", int(_cfg(runtime_config, ["jobs", "execution_max_attempts"], 1))),
        development_engine=(os.getenv("EDIM_DEVELOPMENT_ENGINE", str(_cfg(runtime_config, ["development_engine", "engine"], "mario"))) or "mario").strip().lower(),
        mario_db_path=(os.getenv("EDIM_MARIO_DB_PATH", str(_cfg(runtime_config, ["development_engine", "mario_db_path"], ""))) or "").strip(),
        mario_timeout_seconds=_env_float("EDIM_MARIO_TIMEOUT_SECONDS", float(_cfg(runtime_config, ["development_engine", "mario_timeout_seconds"], 120.0)), minimum=1.0),
        mario_fail_on_error=_env_bool("EDIM_MARIO_FAIL_ON_ERROR", bool(_cfg(runtime_config, ["development_engine", "mario_fail_on_error"], False))),
        frontend_dir=frontend_dir,
        runtime_config=runtime_config,
        model_runtime_mode=(os.getenv("EDIM_MODEL_RUNTIME_MODE", str(_cfg(runtime_config, ["model_runtime", "mode"], "subprocess"))) or "subprocess").strip().lower(),
        runtime_artifact_handoff_mode=_runtime_artifact_handoff_mode(
            os.getenv("EDIM_RUNTIME_ARTIFACT_HANDOFF_MODE")
            or str(_cfg(runtime_config, ["model_runtime", "artifact_handoff_mode"], "shared_filesystem"))
        ),
        dataset_staging_mode=_dataset_staging_mode(
            os.getenv("EDIM_DATASET_STAGING_MODE")
            or str(_cfg(runtime_config, ["model_runtime", "dataset_staging_mode"], "copy_to_run"))
        ),
        model_manifest_path=model_manifest_path,
        dataset_manifest_path=dataset_manifest_path,
        platform_store_backend=(os.getenv("EDIM_PLATFORM_STORE_BACKEND", str(_cfg(runtime_config, ["storage", "platform_store_backend"], "sqlite"))) or "sqlite").strip().lower(),
        platform_sqlite_path=platform_sqlite_path,
        auth_mode=(os.getenv("EDIM_AUTH_MODE", str(_cfg(runtime_config, ["auth", "mode"], "test_header"))) or "test_header").strip().lower(),
        auth_session_secret=(os.getenv("EDIM_AUTH_SESSION_SECRET") or str(_cfg(runtime_config, ["auth", "session_secret"], "dev-only-change-me")) or "dev-only-change-me"),
        auth_session_ttl_seconds=_env_int("EDIM_AUTH_SESSION_TTL_SECONDS", int(_cfg(runtime_config, ["auth", "session_ttl_seconds"], 43200)), minimum=60),
        auth_cookie_name=(os.getenv("EDIM_AUTH_COOKIE_NAME") or str(_cfg(runtime_config, ["auth", "cookie_name"], "edim_session")) or "edim_session").strip() or "edim_session",
        auth_cookie_secure=_env_bool("EDIM_AUTH_COOKIE_SECURE", bool(_cfg(runtime_config, ["auth", "cookie_secure"], True))),
        auth_password_min_length=_env_int("EDIM_AUTH_PASSWORD_MIN_LENGTH", int(_cfg(runtime_config, ["auth", "password_min_length"], 8)), minimum=6),
        auth_reset_ttl_seconds=_env_int("EDIM_AUTH_RESET_TTL_SECONDS", int(_cfg(runtime_config, ["auth", "reset_ttl_seconds"], 3600)), minimum=60),
        auth_invite_ttl_seconds=_env_int("EDIM_AUTH_INVITE_TTL_SECONDS", int(_cfg(runtime_config, ["auth", "invite_ttl_seconds"], 172800)), minimum=60),
        auth_dev_token_display=_env_bool("EDIM_AUTH_DEV_TOKEN_DISPLAY", bool(_cfg(runtime_config, ["auth", "dev_token_display"], True))),
        auth_admin_email=(os.getenv("EDIM_AUTH_ADMIN_EMAIL") or str(_cfg(runtime_config, ["auth", "admin_email"], "daniele.ciminieri@gmail.com")) or "daniele.ciminieri@gmail.com").strip().lower(),
        auth_email_connection_string=(os.getenv("EDIM_AUTH_EMAIL_CONNECTION_STRING") or str(_cfg(runtime_config, ["auth", "email_connection_string"], "")) or "").strip(),
        auth_email_sender=(os.getenv("EDIM_AUTH_EMAIL_SENDER") or str(_cfg(runtime_config, ["auth", "email_sender"], "")) or "").strip(),
        auth_email_base_url=(os.getenv("EDIM_AUTH_EMAIL_BASE_URL") or str(_cfg(runtime_config, ["auth", "email_base_url"], "")) or "").strip().rstrip("/"),
    )


def _runtime_artifact_handoff_mode(value: str | None) -> str:
    mode = (value or "shared_filesystem").strip().lower().replace("-", "_")
    aliases = {
        "shared": "shared_filesystem",
        "shared_storage": "shared_filesystem",
        "mounted_storage": "shared_filesystem",
        "worker_upload": "worker_staged_upload",
        "staged_upload": "worker_staged_upload",
        "direct_upload": "runtime_direct_upload",
    }
    mode = aliases.get(mode, mode)
    allowed = {"shared_filesystem", "worker_staged_upload", "runtime_direct_upload"}
    if mode not in allowed:
        raise ValueError(
            "Unsupported EDIM runtime artifact handoff mode "
            f"{value!r}. Expected one of: {', '.join(sorted(allowed))}."
        )
    return mode


def _dataset_staging_mode(value: str | None) -> str:
    mode = (value or "copy_to_run").strip().lower().replace("-", "_")
    aliases = {
        "referenced": "reference",
        "in_place": "reference",
        "copy": "copy_to_run",
        "copy_to_inputs": "copy_to_run",
        "object": "object_reference",
        "storage_reference": "object_reference",
    }
    mode = aliases.get(mode, mode)
    allowed = {"reference", "copy_to_run", "object_reference"}
    if mode not in allowed:
        raise ValueError(
            "Unsupported EDIM dataset staging mode "
            f"{value!r}. Expected one of: {', '.join(sorted(allowed))}."
        )
    return mode
