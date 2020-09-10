import time
import logging
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)

class Sensor:
    def __init__(self, project, subscription_name, bucket,
                    table_destination, db_client):
        self.__table_destination = table_destination
        self.__db_client = db_client
        self.__bucket = bucket

        self.__subscriber = pubsub_v1.SubscriberClient()
        self.__subscription_path = self.__subscriber.subscription_path(project, subscription_name)

    def get_object_id(self, message):
        _ = message.data.decode("utf-8")
        attributes = message.attributes
        return attributes["objectId"]

    def poll_notifications(self):
        """Polls a Cloud Pub/Sub subscription for new GCS events for display."""
        def callback(message):
            logger.info(f"Start uploading new file...")
            message.ack()
            source_file = self.get_object_id(message)
            self.__bucket.download_blob(source_file)
            logger.info(f"Finish uploading new file.")
            self.__db_client.insert_values(source_file, self.__table_destination)

        self.__subscriber.subscribe(self.__subscription_path, callback=callback)

        # The subscriber is non-blocking, so we must keep the main thread from
        # exiting to allow it to process messages in the background.
        logger.info("Listening for messages on {}".format(self.__subscription_path))
        while True:
            time.sleep(60)
