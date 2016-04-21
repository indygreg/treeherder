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
        userid = config["userid"]
        durable = settings.PULSE_DATA_INGESTION_QUEUES_DURABLE
        auto_delete = settings.PULSE_DATA_INGESTION_QUEUES_AUTO_DELETE
        connection = Connection(**config)
        consumer = JobConsumer(connection)
        queue_name = "queue/{}/jobs".format(userid)

        try:
            for exchange_obj in settings.PULSE_DATA_INGESTION_EXCHANGES:
                # ensure the exchange exists.  Throw an error if it doesn't
                exchange = Exchange(exchange_obj["name"], type="topic")
                exchange(connection).declare(passive=True)

                self.stdout.write("Connected to Pulse Exchange: {}".format(
                    exchange_obj["name"]))

                for project in exchange_obj["projects"]:
                    for destination in exchange_obj['destinations']:
                        routing_key = "{}.{}".format(destination, project)
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

            try:
                consumer.run()
            except KeyboardInterrupt:

                #TODO: need to un-bind and shut down gracefully
                self.stdout.write("Pulse listening stopped...")
        finally:
            consumer.close()
