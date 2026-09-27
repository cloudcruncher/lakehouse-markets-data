"""Coinbase trades to Kafka: the public `matches` feed, one record per trade, keyed by product.

Runs for as long as the platform runs it: a tenant service (`services:` in the platform's
tenants/markets-data.yaml), not a Dagster asset. Needs no API key. Kafka is the durable store,
so the producer keeps no state: after a reconnect Coinbase resends only the latest trade per
product (`last_match`), so trades during an outage are lost, and silver sees the gap as missing
trade_ids. Duplicates (a `last_match` already sent) are removed in silver by trade_id.

Usage: python3 -m markets_data.coinbase   (env: KAFKA_BOOTSTRAP, PRODUCTS=BTC-USD,ETH-EUR,...)
"""

import asyncio
import json
import os
import signal
import time

import websockets
from confluent_kafka import Producer

FEED = "wss://ws-feed.exchange.coinbase.com"
TOPIC = "markets.coinbase.trades"
PRODUCTS = ("BTC-USD", "ETH-USD", "SOL-USD", "BTC-EUR", "ETH-EUR")
# Price and size stay strings, as Coinbase sends them: decimals are parsed once, in the stream.
FIELDS = ("trade_id", "product_id", "price", "size", "side", "time", "sequence")
LOG_EVERY_S = 60
MAX_BACKOFF_S = 60


class FeedError(Exception):
    """Coinbase refused the subscription (unknown product, feed change): retrying won't help."""


def subscribe(products: tuple[str, ...]) -> str:
    return json.dumps({"type": "subscribe", "product_ids": list(products), "channels": ["matches"]})


def to_record(msg: dict) -> tuple[str, bytes] | None:
    """(key, value) for a trade; None for subscription acks and heartbeats."""
    if msg.get("type") == "error":
        raise FeedError(f"{msg.get('message')}: {msg.get('reason')}")
    if msg.get("type") not in ("match", "last_match"):
        return None
    value = {field: msg[field] for field in FIELDS}
    return msg["product_id"], json.dumps(value, separators=(",", ":")).encode()


class Stats:
    def __init__(self) -> None:
        self.sent = self.failed = 0
        self.since = time.monotonic()

    def delivered(self, err, _msg) -> None:
        if err is None:
            self.sent += 1
        else:
            self.failed += 1
            print(f"[coinbase] delivery failed: {err}", flush=True)

    def maybe_log(self) -> None:
        if time.monotonic() - self.since >= LOG_EVERY_S:
            print(f"[coinbase] {self.sent} trades to {TOPIC}, {self.failed} failed", flush=True)
            self.sent = self.failed = 0
            self.since = time.monotonic()


async def stream(producer: Producer, products: tuple[str, ...], stats: Stats) -> None:
    async with websockets.connect(FEED, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(subscribe(products))
        print(f"[coinbase] subscribed to {', '.join(products)}", flush=True)
        async for raw in ws:
            if record := to_record(json.loads(raw)):
                key, value = record
                try:
                    producer.produce(TOPIC, key=key, value=value, on_delivery=stats.delivered)
                except BufferError:  # Kafka unreachable for long enough to fill the buffer
                    stats.delivered("local queue full", None)
            producer.poll(0)
            stats.maybe_log()


async def run(producer: Producer, products: tuple[str, ...]) -> None:
    stats, backoff = Stats(), 1
    while True:
        started = time.monotonic()
        try:
            await stream(producer, products, stats)
        except FeedError:
            raise
        except (OSError, websockets.WebSocketException) as e:
            # A connection that stayed up a while was healthy: start the backoff again.
            backoff = 1 if time.monotonic() - started > MAX_BACKOFF_S else backoff
            print(f"[coinbase] feed lost ({e!r}); reconnecting in {backoff}s", flush=True)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF_S)


def main() -> None:
    products = tuple(p for p in os.environ.get("PRODUCTS", ",".join(PRODUCTS)).split(",") if p)
    producer = Producer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
            "client.id": f"{os.environ.get('TENANT', 'markets-data')}-coinbase",
            "enable.idempotence": True,  # acks=all, no duplicates from producer retries
            "compression.type": "zstd",
            "linger.ms": 100,
            "queue.buffering.max.kbytes": 16384,  # a Kafka outage buffers 16 MB, then drops
        }
    )
    loop = asyncio.new_event_loop()
    task = loop.create_task(run(producer, products))
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, task.cancel)
    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        print("[coinbase] stopping", flush=True)
    finally:
        producer.flush(10)
        loop.close()


if __name__ == "__main__":
    main()
