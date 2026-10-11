from __future__ import annotations

import logging
import os
import subprocess
import sys
import uuid
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .api.routers import (
    datasets_router,
    platform_router,
    runs_router,
    scenarios_router,
    system_router,
)
from .auth import auth_pages_router, configure_auth, fastapi_users
from .auth.backends import auth_backend
from .auth.schemas import UserRead
from .jobs import JobManager
from .runtime import EventStore, LocalEventStore
from .services.artifact_storage import ArtifactStorageService, LocalArtifactStorageService
from .services.dataset_repository import DatasetRepository, LocalDatasetRepository
from .services.model_catalog import ModelCatalogProvider, RuntimeCliModelCatalogProvider
from .services.platform_repository import PlatformRepository, create_platform_repository
from .settings import Settings, get_settings

logger = logging.getLogger(__name__)


def _run_alembic_migrations(settings: Settings) -> None:
    """Run 'alembic upgrade head' before the app accepts traffic.

    Only executed when EDIM_DATABASE_URL is set and EDIM_RUN_MIGRATIONS is
    truthy (default true in docker-compose-dev.yml).
    """
    run_migrations = os.getenv("EDIM_RUN_MIGRATIONS", "true").strip().lower()
    if run_migrations not in {"1", "true", "yes"}:
        logger.info("Skipping alembic migrations (EDIM_RUN_MIGRATIONS=%s).", run_migrations)
        return
    database_url = os.getenv("EDIM_DATABASE_URL", "").strip()
    if not database_url:
        return
    backend_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["EDIM_ALEMBIC_URL"] = database_url
    cmd = [sys.executable, "-m", "alembic", "-c", str(backend_root / "alembic.ini"), "upgrade", "head"]
    logger.info("Running alembic upgrade head...")
    result = subprocess.run(cmd, cwd=str(backend_root), env=env, capture_output=True, text=True)
    if result.returncode != 0:
        stdout = result.stdout.replace(database_url, "[REDACTED_DATABASE_URL]")
        stderr = result.stderr.replace(database_url, "[REDACTED_DATABASE_URL]")
        logger.error("alembic upgrade failed (exit %s):\nstdout=%s\nstderr=%s", result.returncode, stdout, stderr)
        raise RuntimeError(f"alembic upgrade failed: {stderr or stdout}")
    logger.info("alembic upgrade head completed.")


def _create_event_store(settings: Settings) -> EventStore:
    """Return PostgresEventStore when EDIM_DATABASE_URL is set, else local JSONL."""
    database_url = os.getenv("EDIM_DATABASE_URL", "").strip()
    if database_url:
        try:
            from .db import build_engine, build_session_factory
            from .services.event_store import PostgresEventStore

            engine = build_engine()
            session_factory = build_session_factory(engine)
            return PostgresEventStore(session_factory)  # type: ignore[return-value]
        except Exception as exc:
            logger.warning("PostgresEventStore unavailable (%s); falling back to LocalEventStore.", exc)
    return LocalEventStore(settings.runs_dir)


def _create_artifact_storage(settings: Settings) -> ArtifactStorageService:
    """Return BlobArtifactStorageService when Azure Blob Storage credentials are set.

    Dev uses EDIM_AZURITE_CONNECTION_STRING (Storage emulator); staging/production
    use EDIM_BLOB_ACCOUNT_URL + Managed Identity.
    """
    conn_str = os.getenv("EDIM_AZURITE_CONNECTION_STRING", "").strip()
    account_url = os.getenv("EDIM_BLOB_ACCOUNT_URL", "").strip()
    if conn_str or account_url:
        try:
            from .services.blob_artifact_storage import BlobArtifactStorageService

            return BlobArtifactStorageService(settings)  # type: ignore[return-value]
        except Exception as exc:
            logger.warning(
                "BlobArtifactStorageService unavailable (%s); falling back to local filesystem.", exc
            )
    return LocalArtifactStorageService(settings)


def _create_execution_queue(settings: Settings):
    """Return a Service Bus queue adapter when EDIM_SERVICEBUS_CONNECTION_STRING is set.

    When Service Bus is active the JobManager dispatches execution messages to the
    bus instead of an in-process queue and its worker thread is disabled — the
    isolated edim-worker container independently consumes the same queue and runs
    models.  Returns None (→ LocalExecutionQueue default) when Service Bus is not
    configured.
    """
    conn_str = os.getenv("EDIM_SERVICEBUS_CONNECTION_STRING", "").strip()
    namespace = os.getenv("EDIM_SERVICEBUS_NAMESPACE", "").strip()
    if not conn_str and not namespace:
        return None, True  # queue=None, start_worker=True (local in-process)

    try:
        from .services.service_bus_queue import ServiceBusExecutionQueue, ServiceBusQueueClient

        queue_name = os.getenv("EDIM_SERVICEBUS_QUEUE_NAME", "execution-queue-local").strip()
        client = ServiceBusQueueClient(
            queue_name=queue_name,
            connection_string=conn_str or None,
            namespace=namespace or None,
        )

        # Load the model runtime manifest so the ServiceBusExecutionQueue can
        # build complete model_run_bundle_v1 bundles for the black-box CLI.
        manifest: dict = {}
        manifest_path = getattr(settings, "model_manifest_path", None)
        if manifest_path and manifest_path.exists():
            try:
                import json as _json
                manifest = _json.loads(manifest_path.read_text(encoding="utf-8"))
                if not isinstance(manifest, dict):
                    manifest = {}
            except Exception:
                logger.warning("Could not load model manifest for Service Bus dispatch.", exc_info=True)

        runtime_config = getattr(settings, "runtime_config", {}) or {}

        logger.info("Dispatching execution messages to Azure Service Bus queue '%s'.", queue_name)
        return ServiceBusExecutionQueue(
            client,
            settings=settings,
            manifest=manifest,
            runtime_config=runtime_config,
        ), False  # queue, start_worker=False
    except Exception as exc:
        logger.warning("Service Bus unavailable (%s); using local in-process queue.", exc)
        return None, True


def _create_completion_bridge(platform_repository, job_manager=None):
    """Start a CompletionBridge when Service Bus is configured.

    Returns None when Service Bus is not available (the in-process
    JobManager writes run status directly in that case).
    """
    conn_str = os.getenv("EDIM_SERVICEBUS_CONNECTION_STRING", "").strip()
    namespace = os.getenv("EDIM_SERVICEBUS_NAMESPACE", "").strip()
    if not conn_str and not namespace:
        return None

    try:
        from .services.completion_bridge import CompletionBridge

        queue_name = os.getenv("EDIM_SERVICEBUS_COMPLETION_QUEUE_NAME", "completion-queue-local").strip()
        bridge = CompletionBridge(
            platform_repository=platform_repository,
            queue_name=queue_name,
            connection_string=conn_str or None,
            namespace=namespace or None,
            job_manager=job_manager,
        )
        logger.info("CompletionBridge ready for queue '%s'.", queue_name)
        return bridge
    except Exception as exc:
        logger.warning("CompletionBridge unavailable (%s); worker status updates will not be persisted.", exc)
        return None


def _discover_frontend_dir(settings) -> Path | None:
    env_dir = (os.getenv("EDIM_FRONTEND_DIR") or "").strip()
    if env_dir:
        path = Path(env_dir).expanduser().resolve()
        if path.exists() and path.is_dir():
            return path
    if settings.frontend_dir and settings.frontend_dir.exists() and settings.frontend_dir.is_dir():
        return settings.frontend_dir
    return None


def create_app(
    settings: Settings | None = None,
    *,
    platform_repository: PlatformRepository | None = None,
    artifact_storage: ArtifactStorageService | None = None,
    dataset_repository: DatasetRepository | None = None,
    event_store: EventStore | None = None,
    model_catalog_provider: ModelCatalogProvider | None = None,
    job_manager: JobManager | None = None,
) -> FastAPI:
    """Create the FastAPI app with replaceable infrastructure providers.

    Environment-driven provider selection (docker-compose-dev and cloud):
      - EDIM_DATABASE_URL  → PostgresPlatformRepository + PostgresEventStore
      - EDIM_AZURITE_CONNECTION_STRING or EDIM_BLOB_ACCOUNT_URL
                           → BlobArtifactStorageService (Azure Blob Storage)
      - Otherwise          → SQLite / local filesystem / in-memory providers

    Models always run in-process via SubprocessModelRuntime (black-box contract
    preserved).  The isolated worker container (edim-worker) independently
    consumes Azure Service Bus when deployed; the API's JobManager does not
    dispatch to Service Bus in the current implementation.
    """
    settings = settings or get_settings()

    # Run migrations before creating providers so the schema is ready.
    _run_alembic_migrations(settings)

    if job_manager is not None:
        platform_repository = platform_repository or getattr(job_manager, "_run_repository", None)
        artifact_storage = artifact_storage or getattr(job_manager, "_artifact_storage", None)
        dataset_repository = dataset_repository or getattr(job_manager, "_dataset_repository", None)
        event_store = event_store or getattr(job_manager, "_event_store", None)
    platform_repository = platform_repository or create_platform_repository(settings)
    artifact_storage = artifact_storage or _create_artifact_storage(settings)
    dataset_repository = dataset_repository or LocalDatasetRepository(settings)
    event_store = event_store or _create_event_store(settings)
    model_catalog_provider = model_catalog_provider or RuntimeCliModelCatalogProvider()

    execution_queue, start_worker = _create_execution_queue(settings)
    job_manager = job_manager or JobManager(
        settings,
        run_repository=platform_repository,
        dataset_repository=dataset_repository,
        event_store=event_store,
        artifact_storage=artifact_storage,
        execution_queue=execution_queue,
        start_worker=start_worker,
    )

    # When Service Bus is configured, start a background bridge that
    # listens on the completion queue and updates run status in Postgres.
    # The isolated worker daemon posts status updates there; the bridge
    # is the only component that writes terminal state to the DB.
    completion_bridge = _create_completion_bridge(platform_repository, job_manager)
    if completion_bridge is not None:
        completion_bridge.start()

    app = FastAPI(title="EDIM Calliope-Africa API", version="0.1.0")

    @app.middleware("http")
    async def attach_request_id(request: Request, call_next):
        incoming = (request.headers.get("X-Request-Id") or "").strip()
        request_id = incoming[:200] if incoming else str(uuid.uuid4())
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-Id"] = request_id
        return response

    app.state.settings = settings
    app.state.platform_repository = platform_repository
    app.state.artifact_storage = artifact_storage
    app.state.dataset_repository = dataset_repository
    app.state.event_store = event_store
    app.state.model_catalog_provider = model_catalog_provider
    app.state.job_manager = job_manager
    app.state.completion_bridge = completion_bridge
    app.state.frontend_dir = _discover_frontend_dir(settings)
    app.state.auth_pending_tokens = {}
    app.state.auth_email_last_sent = {}
    app.state.auth_dev_token_display = bool(getattr(settings, "auth_dev_token_display", True))
    app.state.auth_ready = False
    configure_auth(settings)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1):\d+$",
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    if app.state.frontend_dir is not None:
        app.mount("/ui", StaticFiles(directory=str(app.state.frontend_dir), html=True), name="ui")
    else:
        logger.warning("Frontend directory not found; UI routes disabled.")

    app.mount(
        "/auth/static",
        StaticFiles(directory=str(Path(__file__).resolve().parent / "auth" / "static")),
        name="auth_static",
    )

    users = fastapi_users()
    app.include_router(users.get_auth_router(auth_backend()), prefix="/auth/api", tags=["auth"])
    app.include_router(users.get_reset_password_router(), prefix="/auth/api", tags=["auth"])
    app.include_router(users.get_verify_router(UserRead), prefix="/auth/api", tags=["auth"])
    app.include_router(auth_pages_router)

    for router in (system_router, platform_router, scenarios_router, datasets_router, runs_router):
        app.include_router(router)

    @app.on_event("startup")
    async def _seed_auth_accounts() -> None:
        if str(getattr(settings, "auth_mode", "") or "").strip().lower() == "local_password":
            from .auth.db import ensure_state_ready

            await ensure_state_ready(app.state)

    # Graceful shutdown: stop the completion bridge background thread.
    @app.on_event("shutdown")
    def _stop_bridge() -> None:
        bridge = getattr(app.state, "completion_bridge", None)
        if bridge is not None:
            bridge.stop()

    return app


app = create_app()
