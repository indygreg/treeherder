import logging
from urlparse import urlparse

from django.conf import settings
from django.core.management.base import BaseCommand
from kombu import (Connection,
                   Exchange)

from treeherder.etl.pulse_consumer import JobConsumer

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    """Management command to ingest jobs from a set of pulse exchanges."""

    help = "Ingest jobs from a set of pulse exchanges"

    def handle(self, *args, **options):
        config = settings.PULSE_DATA_INGESTION_CONFIG
        userid = urlparse(config).username
        queue_name = "queue/{}/{}".format(
            userid,
            settings.PULSE_DATA_INGESTION_QUEUE_SUFFIX
        )
        durable = settings.PULSE_DATA_INGESTION_QUEUES_DURABLE
        auto_delete = settings.PULSE_DATA_INGESTION_QUEUES_AUTO_DELETE

        with Connection(config, ssl=True) as connection:
            consumer = JobConsumer(connection)

            for exchange_obj in settings.PULSE_DATA_INGESTION_EXCHANGES:
                # ensure the exchange exists.  Throw an error if it doesn't
                exchange = Exchange(exchange_obj["name"], type="topic")
                exchange(connection).declare(passive=True)

                self.stdout.write("Connected to Pulse Exchange: {}".format(
                    exchange_obj["name"]))

                for project in exchange_obj["projects"]:
                    for destination in exchange_obj['destinations']:
                        routing_key = "{}.{}".format(destination, project)

                        # I need to somehow audit the exchanges/topics that the
                        # durable queue is bound to and unbind them if they're
                        # no longer valid.  We will likely add and remove things
                        # from the config.  This will re-add anything new,
                        # but will not remove anything, afaict.  Unless I'm
                        # replacing the queue bindings on Pulse when I do this
                        # which is possible, I guess.

                        consumer.listen_to(
                            exchange,
                            routing_key,
                            queue_name,
                            durable,
                            auto_delete)
                        self.stdout.write(
                            "Pulse message consumer listening to : {} {}".format(
                                exchange.name,
                                routing_key
                            ))

            # print(consumer.queue.channel)
            # print(consumer.queue.bindings)
            try:
                consumer.run()
            except KeyboardInterrupt:
                self.stdout.write("Pulse listening stopped...")
