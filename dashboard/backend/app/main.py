"""FastAPI entry point for the real-time streaming control centre."""

import asyncio
import logging

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .api.routes import router, websocket_live
from .config import Settings
from .container import create_services


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("order_lifecycle.dashboard")


async def _metrics_loop(services):
    while True:
        await asyncio.sleep(2)
        try:
            await services.hub.publish(
                {
                    "type": "metrics",
                    "data": services.monitor.metrics(),
                }
            )
        except Exception:
            logger.exception("Metrics broadcast failed")


def create_app(services=None, frontend_dir=None):
    settings = services.settings if services else Settings.from_env()
    services = services or create_services(settings, start_background=True)
    frontend_path = Path(frontend_dir or settings.frontend_dir)

    @asynccontextmanager
    async def lifespan(app):
        await services.hub.start()
        metrics_task = None
        if services.start_background:
            # Kafka monitors are started by one-click demo startup or on-demand;
            # starting here is safe/idempotent and preserves existing behavior.
            services.monitor.start()
            metrics_task = asyncio.create_task(
                _metrics_loop(services),
                name="dashboard-metrics",
            )
            # Publish initial non-destructive readiness status for landing UI.
            try:
                services.startup.refresh_checks()
            except Exception:
                logger.exception("Initial startup status refresh failed")
        services.activity.add(
            "INFO",
            "dashboard",
            "Dashboard backend started",
            f"http://{settings.dashboard_host}:{settings.dashboard_port}",
        )
        try:
            yield
        finally:
            if metrics_task is not None:
                metrics_task.cancel()
                try:
                    await metrics_task
                except asyncio.CancelledError:
                    pass
            services.generator.stop()
            services.monitor.stop()
            await services.hub.stop()
            services.activity.add("INFO", "dashboard", "Dashboard backend stopped")

    app = FastAPI(
        title="E-commerce Order Lifecycle Streaming Dashboard",
        version=settings.project_version,
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
        lifespan=lifespan,
    )
    app.state.services = services
    app.include_router(router)
    app.add_api_websocket_route("/ws/live", websocket_live)

    if frontend_path.exists():
        app.mount(
            "/",
            StaticFiles(directory=str(frontend_path), html=True),
            name="dashboard-frontend",
        )
    else:
        logger.error("Frontend directory not found: %s", frontend_path)

    return app


app = create_app()
