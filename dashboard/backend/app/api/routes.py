"""FastAPI routes for dashboard data and controls."""

import asyncio

from fastapi import APIRouter, HTTPException, Query, Request, WebSocket
from fastapi import WebSocketDisconnect

from ..models.schemas import GeneratorRunRequest


router = APIRouter(prefix="/api")


def _services(request: Request):
    return request.app.state.services


def _operation_status(result):
    if result.get("ok"):
        return 200
    status = result.get("status")
    if status in {"docker_unavailable", "flink_unavailable"}:
        return 503
    if status in {"already_running", "not_running"}:
        return 200
    if status in {"invalid_request", "invalid_job_id"}:
        return 422
    return 502


def _operation(result):
    if not result.get("ok"):
        raise HTTPException(
            status_code=_operation_status(result),
            detail=result,
        )
    return result


@router.get("/health")
def health(request: Request):
    return _services(request).health()


@router.get("/config")
def config(request: Request):
    services = _services(request)
    return services.settings.public_config()


@router.get("/infrastructure/status")
def infrastructure_status(
    request: Request,
    refresh: bool = Query(default=False),
):
    return _services(request).infrastructure.combined(force=refresh)


@router.post("/infrastructure/start")
def infrastructure_start(request: Request):
    return _operation(_services(request).docker.start_infrastructure())


@router.post("/infrastructure/stop")
def infrastructure_stop(request: Request):
    return _operation(_services(request).docker.stop_infrastructure())


@router.get("/flink/status")
def flink_status(
    request: Request,
    refresh: bool = Query(default=False),
):
    return _services(request).flink.status(force=refresh)


@router.post("/flink/submit")
def flink_submit(request: Request):
    return _operation(_services(request).flink.submit_job())


@router.post("/flink/stop")
def flink_stop(request: Request):
    return _operation(_services(request).flink.stop_job())


@router.get("/events/recent")
def events_recent(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
):
    monitor = _services(request).monitor
    snapshot = monitor.snapshot(limit)
    return {
        **snapshot,
        "limit": limit,
        "note": (
            "State records are retained latest values from a compacted topic. "
            "Processed events provide lifecycle history."
        ),
    }


@router.get("/exceptions/recent")
def exceptions_recent(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
):
    monitor = _services(request).monitor
    return {
        "exceptions": monitor.recent_exceptions(limit),
        "limit": limit,
    }


@router.get("/orders")
def orders(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    search: str | None = Query(default=None, max_length=80),
    active_only: bool = Query(default=False),
):
    monitor = _services(request).monitor
    return {
        "orders": monitor.orders(
            limit=limit,
            search=search,
            active_only=active_only,
        ),
        "limit": limit,
    }


@router.get("/orders/{order_id}")
def order_detail(request: Request, order_id: str):
    normalized = order_id.strip()
    if (
        not normalized
        or len(normalized) > 80
        or any(
            character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz0123456789-_.:"
            for character in normalized
        )
    ):
        raise HTTPException(status_code=422, detail="Invalid order identifier")

    result = _services(request).monitor.order_detail(normalized)
    if not result.get("found"):
        raise HTTPException(
            status_code=404,
            detail={
                "message": "No retained data was found for this order.",
                "order_id": normalized,
            },
        )
    return result


@router.post("/generator/run")
def generator_run(request: Request, payload: GeneratorRunRequest):
    if payload.scenario == "continuous":
        raise HTTPException(
            status_code=422,
            detail="Use /api/generator/start for continuous mode",
        )
    result = _services(request).generator.run_once(
        payload.scenario,
        payload.interval,
    )
    return _operation(result)


@router.post("/generator/start")
def generator_start(request: Request, payload: GeneratorRunRequest):
    if payload.scenario != "continuous":
        raise HTTPException(
            status_code=422,
            detail="Start controls only support the continuous scenario",
        )
    result = _services(request).generator.start_continuous(payload.interval)
    return _operation(result)


@router.post("/generator/stop")
def generator_stop(request: Request):
    return _operation(_services(request).generator.stop())


@router.get("/generator/status")
def generator_status(request: Request):
    return _services(request).generator.status()


@router.get("/generator/logs")
def generator_logs(
    request: Request,
    limit: int = Query(default=100, ge=1, le=250),
):
    return {
        "logs": _services(request).generator.logs(limit),
        "limit": limit,
    }


@router.get("/logs/recent")
def logs_recent(
    request: Request,
    limit: int = Query(default=100, ge=1, le=250),
):
    return {
        "logs": _services(request).activity.recent(limit),
        "limit": limit,
    }


async def websocket_live(websocket: WebSocket):
    services = websocket.app.state.services
    hub = services.hub
    await hub.connect(websocket)
    try:
        while True:
            try:
                message = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=20,
                )
                if message == "ping":
                    await websocket.send_json({"type": "pong"})
            except asyncio.TimeoutError:
                await websocket.send_json(
                    {"type": "ping", "message": "keepalive"}
                )
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        hub.disconnect(websocket)
