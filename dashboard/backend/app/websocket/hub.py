"""WebSocket connection manager and bounded broadcast queue."""

import asyncio
import logging


logger = logging.getLogger("order_lifecycle.dashboard.websocket")


class LiveHub:
    def __init__(self, queue_size=2000):
        self._clients = set()
        self._loop = None
        self._queue = None
        self._task = None
        self._queue_size = queue_size

    async def start(self):
        if self._task is not None:
            return
        self._loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue(maxsize=self._queue_size)
        self._task = asyncio.create_task(self._run(), name="dashboard-ws-hub")

    async def stop(self):
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        self._loop = None
        self._queue = None

    async def connect(self, websocket):
        await websocket.accept()
        self._clients.add(websocket)
        await websocket.send_json(
            {
                "type": "connected",
                "message": "Live dashboard connection established",
                "clients": len(self._clients),
            }
        )

    def disconnect(self, websocket):
        self._clients.discard(websocket)

    @property
    def client_count(self):
        return len(self._clients)

    async def broadcast(self, message):
        if not self._clients:
            return
        stale = []
        for websocket in list(self._clients):
            try:
                await asyncio.wait_for(
                    websocket.send_json(message),
                    timeout=2.0,
                )
            except Exception:
                stale.append(websocket)
        for websocket in stale:
            self._clients.discard(websocket)

    def publish_threadsafe(self, message):
        if self._loop is None or self._queue is None:
            return
        try:
            self._loop.call_soon_threadsafe(self._enqueue, message)
        except RuntimeError:
            pass

    async def publish(self, message):
        await self.broadcast(message)

    def _enqueue(self, message):
        if self._queue is None:
            return
        if self._queue.full():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        try:
            self._queue.put_nowait(message)
        except asyncio.QueueFull:
            pass

    async def _run(self):
        while True:
            message = await self._queue.get()
            try:
                await self.broadcast(message)
            except Exception:
                logger.exception("WebSocket broadcast failed")
