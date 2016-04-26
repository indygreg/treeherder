import json
import logging

from kombu import Queue
from kombu.mixins import ConsumerMixin
from kombu import Exchange

from treeherder.etl.tasks.pulse_tasks import store_pulse_jobs

logger = logging.getLogger(__name__)


class JobConsumer(ConsumerMixin):
    """
    Consume jobs from Pulse exchanges
    """
    def __init__(self, connection, queue_name, sources):
        self.connection = connection
        self.queue_name = queue_name
        self.sources = sources
        self.stored_queues = None

    @property
    def queues(self):
        """List of queues used by worker.
        Multiple queues are used to track multiple routing keys.
        """
        if not self.stored_queues:
            self.stored_queues = []
            for source in self.sources:
                # ensure the exchange exists.  Throw an error if it doesn't
                exchange = Exchange(source["exchange"], type="topic")
                exchange(self.connection).declare(passive=True)

                logger.info("Connected to Pulse Exchange: {}".format(
                    source["name"]))

                for project in source["projects"]:
                    for destination in source['destinations']:
                        routing_key = "{}.{}".format(destination, project)
                        queue = Queue(name=self.queue_name, exchange=exchange,
                                      routing_key=routing_key, durable=True,
                                      exclusive=False,
                                      auto_delete=False)

                        self.stored_queues.append(queue)
        return self.stored_queues


    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.queues, callbacks=[self.on_message])]

    def on_message(self, body, message):
        try:
            try:
                jobs = json.loads(body)

            except TypeError:
                jobs = body

            store_pulse_jobs.apply_async(
                args=[jobs],
                routing_key='store_pulse_jobs'
            )
            logger.info("<><><> received pulse job message")
            message.ack()

        except Exception:
            logger.error("Unable to load jobs: {}".format(message), exc_info=1)

    def close(self):
        self.connection.release()

# this is what they look like in the config
# PULSE_DATA_INGESTION_EXCHANGES = [
    # {
    #     "exchange": "exchange/garndt-debug/v1/jobs",
    #     "name": "garndt-debug/jobs",
    #     "projects": [
    #         '#',
    #     ],
    #     "destinations": [
    #         'treeherder',
    #         '#'
    #     ]
    # },
    # {
    #     "exchange": "exchange/treeherder-test/jobs",
    #     "name": "localtest/jobs",
    #     "projects": [
    #         '#'
    #     ],
    #     "destinations": [
    #         '#',
    #         'treeherder',
    #         'treeherder-staging'
    #     ]
    # },
    # ... other CI systems
# ]
