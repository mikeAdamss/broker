
import os
import logging
import uuid
import sys
import asyncio

from message.controller import SchemaController

from google.cloud import pubsub_v1
from configuration import get_config

logging.basicConfig(level=logging.DEBUG)

class Logger:
    """
    Wraps the logging to pass through a live_id
    live_id is created per subscription, so we can track issues
    """

    def __init__(self):
        self._live_id = None

    def _set_id(self, sub_id):
        """
        Extend the id where need, such as a child process of a single process
        eg: one distinct message within an already identified listener thread
        """
        return self._live_id if sub_id is None else "{} & {}".format(self._live_id, sub_id)

    def debug(self, msg, sub_id=None):
        id = self._set_id(sub_id)
        logging.debug("{} - {}".format(id, msg))

    def warning(self, msg, sub_id=None):
        id = self._set_id(sub_id)
        logging.warning("{} - {}".format(id, msg))


class Broker:
    """
    The main Broker class that does all the things
    """
    def __init__(self):
        self.cfg = get_config()
        self.subscribed = False
        self.subscriber = None
        self.subscription_path = None
        self.live_id = None
        self.log = Logger()
        self.schema = SchemaController()

        # Export credentials if we havn't
        env_key = "GOOGLE_APPLICATION_CREDENTIALS"
        if env_key not in os.environ:
            os.environ[env_key] = self.cfg.credential_path
            logging.warning('Exporting "{}" as {}, we ' \
                            'shouldn\'t need to do this outside of local dev. Please set' \
                            ' them in the environment'.format(env_key, self.cfg.credential_path))


    def _set_new_identifier(self):
        """
        Create a new id each time we spin up the listener.
        Should allow us to sanely track what's going on in the envent of an issue
        """
        live_id = str(uuid.uuid1())
        self.live_id = live_id
        self.log._live_id = live_id

    def _new_subscriber(self):
        """
        Creates a new subscriber client
        """
        logging.debug("Attempting to ceate a new subscriber client (confirmation should follow).")
        self.subscriber = pubsub_v1.SubscriberClient()
        logging.debug("Successfully created new subscriber client.")

    def _subscribe(self):
        """
        Use a subscriber client to subscribe to topic subscription
        """
        if self.subscriber is None:
            logging.debug("Attempting to create new subscriber client")
            self._new_subscriber()

    def _create_subscription_path(self):
        """
        Create a subscription path, using the instantiated subscriber and the provided config
        """

        # Ensure subscription
        self._subscribe()

        # Ensure subscription path
        if self.subscription_path is None:
            logging.debug("Attempting to create new subscription path")
            self.subscription_path = self.subscriber.subscription_path(self.cfg.project_id, self.cfg.subscription_id)

    def listen(self):
        """
        Listen for messages
        """
        self._set_new_identifier()
        self._create_subscription_path()

        self.log.debug("Attempting to create streaming future pull")
        streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=self.callback)
        self.log.debug("Streaming future pull created. Listening for messages on {}..\n".format(self.subscription_path))

        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with self.subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                streaming_pull_future.result(timeout=self.cfg.listen_timeout)
            except asyncio.TimeoutError:
                streaming_pull_future.cancel()

                # We've timed out, clear and begin again
                self.log.debug("Timeout hit, reinitialising broker.")
                self.subscribed = False
                self.subscriber = None
                self.subscription_path = None
                self.live_id = None

                self.listen()

    def returnToSender(self, error):
        """
        Where an user error is encountered (i.e bad schema or message)
        return that error to the sender.
        """
        print("RETURN TO SENDER")
        print(error)
        print("RETURN TO SENDER")

        import sys
        sys.exit(1)

    def callback(self, message):
        """
        What to do when we receive a message
        """

        # What type of message did we get?
        try:
            # Dev note, this will let us broker messages for multiple
            # purposes via the same queue
            message_handler = self.schema.confirm(message)
        except Exception as e:
            raise Exception("Unable to identifier handler") from e
            

        msg_id = str(uuid.uuid1())
        self.log.debug("recieved message: {}".format(message), sub_id=msg_id)
        
        print("Received message: {}".format(message))

        message_handler.handle()
        message.ack()


Broker().listen()

