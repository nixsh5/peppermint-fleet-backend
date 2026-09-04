import json
import logging
import asyncio
from typing import Callable, Coroutine, Any
import paho.mqtt.client as mqtt

logger = logging.getLogger("mqtt_client")
logging.basicConfig(level=logging.INFO)

class MQTTSubscriber:
    def __init__(
        self,
        broker_host: str,
        broker_port: int,
        topic: str,
        on_event_callback: Callable[[dict], Coroutine[Any, Any, None]],
        loop: asyncio.AbstractEventLoop
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic = topic
        self.on_event_callback = on_event_callback
        self.loop = loop

        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            logger.info(f"Connected to MQTT broker at {self.broker_host}:{self.broker_port}")
            client.subscribe(self.topic, qos=1)
            logger.info(f"Subscribed to {self.topic}")
        else:
            logger.error(f"Failed to connect to broker, return code {rc}")

    def _on_disconnect(self, client, userdata, flags, rc, properties=None):
        logger.warning(f"Disconnected from MQTT broker with code {rc}. Auto-reconnecting...")

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8")) # Safely schedule the async state update and WS broadcast on the main event loop
            asyncio.run_coroutine_threadsafe(self.on_event_callback(payload), self.loop)
        except Exception as e:
            logger.error(f"Error handling message on {msg.topic}: {e}")

    def start(self):
        """Starts network loop in background thread with reconnect support."""
        self.client.connect_async(self.broker_host, self.broker_port, keepalive=60)
        self.client.loop_start()

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()