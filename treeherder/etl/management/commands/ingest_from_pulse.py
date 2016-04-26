import logging
from urlparse import urlparse

from django.conf import settings
from django.core.management.base import BaseCommand
from kombu import Connection

from treeherder.etl.pulse_consumer import JobConsumer

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    """Management command to ingest jobs from a set of pulse exchanges."""

    help = "Ingest jobs from a set of pulse exchanges"

    def handle(self, *args, **options):
        config = settings.PULSE_DATA_INGESTION_CONFIG
        userid = urlparse(config).username
        queue_name = "queue/{}/jobs".format(userid)
        sources = settings.PULSE_DATA_INGESTION_EXCHANGES

        with Connection(config, ssl=True) as connection:
            consumer = JobConsumer(connection, queue_name, sources)

            try:
                consumer.run()
            except KeyboardInterrupt:
                logger.info("Pulse listening stopped...")
