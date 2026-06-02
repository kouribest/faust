"""
POC FastStream : 2 producers, 2 consumers (subscribers), 1 API web /test.
"""

import asyncio
import json
import random

from faststream.asgi import AsgiFastStream, AsgiResponse, get
from faststream.kafka import KafkaBroker
from pydantic import BaseModel


class Metric(BaseModel):
    metric_id: int
    value: float


class Alert(BaseModel):
    alert_id: int
    message: str


def build_alert(metric: Metric) -> Alert:
    return Alert(
        alert_id=metric.metric_id,
        message=f"Alerte {metric.metric_id} : métrique à {metric.value}",
    )


def log_alert(alert: Alert) -> str:
    line = f"[consumer-2] {alert.message}"
    print(line)
    return line


broker = KafkaBroker("localhost:9092")

to_metrics = broker.publisher("metrics")
to_alerts = broker.publisher("alerts")


@broker.subscriber("metrics")
async def process_metric(metric: Metric) -> None:
    await to_alerts.publish(build_alert(metric))


@broker.subscriber("alerts")
async def process_alert(alert: Alert) -> None:
    log_alert(alert)


async def produce_metric(metric_id: int, value: float) -> Metric:
    metric = Metric(metric_id=metric_id, value=value)
    await to_metrics.publish(metric)
    return metric


async def periodic_producer(interval: float = 5.0) -> None:
    try:
        while True:
            metric = await produce_metric(
                metric_id=random.randint(1000, 9999),
                value=round(random.uniform(10, 500), 2),
            )
            print(f"[producer-1] métrique envoyée: {metric}")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass


async def handle_test() -> dict:
    """l'endpoint produit une métrique et la renvoie."""
    metric = await produce_metric(
        metric_id=random.randint(1000, 9999),
        value=round(random.uniform(10, 500), 2),
    )
    return {
        "status": "ok",
        "produced": {"metric_id": metric.metric_id, "value": metric.value},
    }


@get
async def test_endpoint(scope) -> AsgiResponse:
    """Endpoint web GET /test."""
    payload = await handle_test()
    return AsgiResponse(
        body=json.dumps(payload).encode(),
        status_code=200,
        headers={"content-type": "application/json"},
    )


app = AsgiFastStream(
    broker,
    asgi_routes=[("/test", test_endpoint)],
)


_background_tasks: set[asyncio.Task] = set()


@app.after_startup
async def _launch_periodic_producer() -> None:
    task = asyncio.create_task(periodic_producer())
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


@app.on_shutdown
async def _stop_periodic_producer() -> None:
    for task in _background_tasks:
        task.cancel()
