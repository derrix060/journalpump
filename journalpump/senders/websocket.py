from .base import LogSender
from aiohttp_socks import ProxyConnectionError, ProxyError, ProxyTimeoutError
from aiohttp_socks.utils import Proxy
from concurrent.futures import TimeoutError as ConnectionTimeoutError
from janus import Queue
from threading import Thread
from urllib.parse import urlparse

import asyncio
import logging
import random
import socket
import ssl
import time
import websockets


class ExponentialBackoff:
    """
    An implementation of the exponential backoff algorithm with full jitter described at
    https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    """

    def __init__(self, base: float, factor: float, maximum: float, jitter: bool):
        if base <= 0:
            raise ValueError("base must be positive")
        if factor <= 0:
            raise ValueError("factor must be positive")
        if maximum < base:
            raise ValueError("maximum must be greater than or equal to base")
        self._base = base
        self._factor = factor
        self._maximum = maximum
        self._jitter = jitter
        self._attempts = 0

    def next_sleep(self) -> float:
        result = self._base * (self._factor ** self._attempts)
        if result <= self._maximum:
            self._attempts += 1
        else:
            result = self._maximum

        if self._jitter:
            return random.uniform(0, result)

        return result

    def reset(self) -> None:
        self._attempts = 0


class WebsocketRunner(Thread):
    def __init__(self, *, websocket_uri, socks5_proxy_url, ssl_enabled, ssl_ca, ssl_key, ssl_cert):
        super().__init__()
        self.setDaemon(True)
        self.log = logging.getLogger(self.__class__.__name__)
        self.websocket_uri = websocket_uri
        self.socks5_proxy_url = socks5_proxy_url
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.ssl_key = ssl_key
        self.ssl_cert = ssl_cert
        self.websocket_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.websocket_loop)
        self.stop_event = asyncio.Event()
        self.queue = None
        self.running = True
        self.connected = False
        self.backoff = ExponentialBackoff(
            base=10, factor=1.8, maximum=60, jitter=True
        )
        # prevent websockets from logging message contents when we're otherwise in DEBUG mode
        logging.getLogger("websockets").setLevel(logging.INFO)

        self.socks5_proxy = None
        if self.socks5_proxy_url:
            self.socks5_proxy = Proxy.from_url(self.socks5_proxy_url, loop=self.websocket_loop)

    async def consumer_handler(self, websocket):
        # Dummy consumer to read and ignore messages from the websocket
        while self.running:
            _ = await websocket.recv()

    async def producer_handler(self, websocket):
        while self.running:
            messages = await self.queue.async_q.get()

            for message in messages:
                try:
                    await websocket.send(message)
                except Exception as ex:  # pylint:disable=broad-except
                    self.log.warning("Exception while sending messages to websocket: %s", ex)

            self.queue.async_q.task_done()

    def run(self):
        self.log.info("WebsocketRunner starting")
        self.websocket_loop.run_until_complete(self.comms_channel_loop())
        self.log.info("WebsocketRunner finished")

    def send(self, *, messages):
        if self.queue:
            self.queue.sync_q.put(messages)
            self.queue.sync_q.join()
            return True
        return False

    def close(self):
        if self.running:
            self.log.info("Closing WebsocketRunner")
            self.running = False
            asyncio.run_coroutine_threadsafe(self.set_stop_event(), self.websocket_loop).result()

    async def set_stop_event(self) -> None:
        self.stop_event.set()

    async def wait_for_stop_event(self) -> None:
        await self.stop_event.wait()

    async def websocket_connect(self, *, timeout=40):
        ssl_context = None
        if self.ssl_enabled:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.load_cert_chain(self.ssl_cert, keyfile=self.ssl_key)
            ssl_context.load_verify_locations(self.ssl_ca)

        headers = {"User-Agent": "journalpump"}

        sock = None
        url_parsed = urlparse(self.websocket_uri)
        if self.socks5_proxy:
            socks_url_parsed = urlparse(self.socks5_proxy_url)
            self.log.info("Connecting via SOCKS5 proxy at %s:%d", socks_url_parsed.hostname, socks_url_parsed.port)
            sock = await self.socks5_proxy.connect(dest_host=url_parsed.hostname, dest_port=url_parsed.port)

        websocket = await asyncio.wait_for(
            websockets.connect(
                self.websocket_uri,
                ssl=ssl_context,
                extra_headers=headers,
                sock=sock,
                server_hostname=url_parsed.hostname if self.ssl_enabled else None,
                close_timeout=20,
            ), timeout
        )

        return websocket

    async def comms_channel_round(self):
        stop_event_task = asyncio.create_task(self.wait_for_stop_event())
        consumer_task = producer_task = None
        try:
            self.log.info("Connecting to websocket at %s", self.websocket_uri)

            websocket = await self.websocket_connect()
            self.log.info("Connected to websocket at %s", self.websocket_uri)
            consumer_task = asyncio.create_task(self.consumer_handler(websocket))
            producer_task = asyncio.create_task(self.producer_handler(websocket))
            established_time = time.monotonic()
            self.connected = True

            _, pending = await asyncio.wait(
                [consumer_task, producer_task, stop_event_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            await websocket.close()
            self.log.info(
                "Websocket connection closed after %d s: %s",
                time.monotonic() - established_time, self.websocket_uri
            )

            # If we have had a long enough connection, reset backoff for a quicker
            # reconnection. Do not reset if we're thrown out quickly due to failed
            # authorization or such.
            if time.monotonic() - established_time > 60:
                self.backoff.reset()

            for task in pending:
                task.cancel()
        except ConnectionRefusedError as ex:
            self.log.warning("Websocket connection refused: %r. Retrying.", ex)
        except (ConnectionTimeoutError, asyncio.exceptions.TimeoutError) as ex:
            self.log.warning("Websocket connection timed out: %r. Retrying.", ex)
        except socket.gaierror as ex:
            self.log.error("DNS lookup for websocket endpoint or SOCKS5 proxy failed: %r. Retrying.", ex)
        except websockets.exceptions.InvalidStatusCode as ex:
            self.log.error("Websocket server rejected connection with HTTP status code: %r. Retrying.", ex)
        except (ProxyError, ProxyConnectionError, ProxyTimeoutError) as ex:
            self.log.warning("SOCKS5 proxy connection error: %r. Retrying.", ex)
        except ssl.SSLCertVerificationError as ex:
            self.log.error("Websocket certificate verification error: %r. Retrying.", ex)
        except OSError as ex:  # Network unreachable, etc, may happen sporadically
            self.log.warning("Websocket connection error: %r. Retrying.", ex)
        except Exception as ex:  # pylint:disable=broad-except
            self.log.exception("Unhandled exception occurred on websocket connection: %r", ex)
        finally:
            websocket = None
            self.connected = False

        for task in [consumer_task, producer_task, stop_event_task]:
            try:
                if task:
                    task.cancel()
            except Exception as ex:  # pylint:disable=broad-except
                self.log.exception("Unhandled exception occurred: %r", ex)

    async def sleep_before_reconnect(self):
        if self.running:
            sleep_interval = self.backoff.next_sleep()
            self.log.info("Retrying websocket connection in %.1f", sleep_interval)
            while sleep_interval > 0 and self.running:
                await asyncio.sleep(min(sleep_interval, 1))
                sleep_interval -= 1

    async def comms_channel_loop(self):
        self.queue = Queue(maxsize=1000)

        while self.running:
            await self.comms_channel_round()
            await self.sleep_before_reconnect()

        self.log.info("Websocket closed")


class WebsocketSender(LogSender):
    def __init__(self, *, config, **kwargs):
        super().__init__(config=config, max_send_interval=config.get("max_send_interval", 0.5), **kwargs)
        self.runner = None
        self.config = config

    def _init_websocket(self) -> None:
        self.log.info("Initializing Websocket client, address: %r", self.config["websocket_uri"])

        if self.runner:
            self.runner.close()
            self.runner = None

        self.mark_disconnected()

        while self.running and not self._connected:
            # retry connection
            time.sleep(1)

            try:
                runner = WebsocketRunner(
                    websocket_uri=self.config["websocket_uri"],
                    socks5_proxy_url=self.config.get("socks5_proxy_url"),
                    ssl_enabled=self.config.get("ssl"),
                    ssl_ca=self.config.get("ca"),
                    ssl_key=self.config.get("keyfile"),
                    ssl_cert=self.config.get("certfile"),
                )
                runner.start()
            except Exception as ex:  # pylint:disable=broad-except
                self.mark_disconnected(ex)
                self.log.exception("Retriable error during Websocket initialization: %s: %s", ex.__class__.__name__, ex)
                self._backoff()
            else:
                self.log.info("Initialized Websocket client, address: %r", self.config["websocket_uri"])
                self.runner = runner
                self.mark_connected()

    def request_stop(self):
        super().request_stop()

        if self.runner:
            self.runner.close()
            self.runner = None

        self.mark_disconnected()

    def send_messages(self, *, messages, cursor):
        if not self.runner:
            self._init_websocket()
        try:
            if self.runner.send(messages=messages):
                self.mark_sent(messages=messages, cursor=cursor)
                return True
        except Exception as ex:  # pylint: disable=broad-except
            self.mark_disconnected(ex)
            self.log.exception("Unexpected exception during send to websocket")
            self.stats.unexpected_exception(ex=ex, where="sender", tags=self.make_tags({"app": "journalpump"}))
            self._backoff()
            self._init_websocket()
        return False
