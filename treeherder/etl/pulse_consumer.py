import json
import logging

from kombu import Queue
from kombu.mixins import ConsumerMixin

from treeherder.etl.tasks.pulse_tasks import store_pulse_jobs

logger = logging.getLogger(__name__)


class JobConsumer(ConsumerMixin):
    """
    Consume jobs from Pulse exchanges
    """
    def __init__(self, connection):
        self.connection = connection
        self.consumers = []
        self.queue = None

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(**c) for c in self.consumers
        ]

    def listen_to(self, exchange, routing_key, queue_name,
                  durable=True, auto_delete=False):
        if not self.queue:
            self.queue = Queue(
                name=queue_name,
                channel=self.connection.channel(),
                exchange=exchange,
                routing_key=routing_key,
                durable=durable,
                auto_delete=auto_delete
            )
            self.consumers.append(dict(
                queues=self.queue,
                callbacks=[self.on_message])
            )
            self.queue.queue_declare()
            self.queue.queue_bind()
            logging.info("Created pulse queue: {}".format(queue_name))
        else:
            self.queue.bind_to(exchange, routing_key)
            logging.info("BoundCreated pulse queue: {}".format(queue_name))

    def on_message(self, body, message):
        try:
            try:
                jobs = json.loads(body)

            except TypeError:
                # self.stdout.write("got type error, trying as object")
                jobs = body

            store_pulse_jobs.apply_async(
                args=[jobs],
                routing_key='store_pulse_jobs'
            )
            logger.info("<><><> received pulse job message")
            print("<><> got message")
            message.ack()

        except Exception:
            logger.error("Unable to load jobs: {}".format(message), exc_info=1)

    def close(self):
        self.connection.release()
