"""
File Monitoring and Kafka Publishing Script

This script monitors a specified log file for changes and publishes new log entries to a Kafka topic.

Dependencies:
- pyinotify
- kafka-python (assumed, based on the use of a Kafka producer)
"""

import os
import time
import pyinotify
from kafka_config import create_kafka_producer
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global configuration variables
KAFKA_PROPERTIES_FILE = 'k0100-client.properties'
KAFKA_TOPIC = 'logCentral'
WATCH_FILE = '/var/log/bind9/security.log'
MAX_MESSAGE_SIZE = 100000  # 100KB

class FileHandler(pyinotify.ProcessEvent):
    """
    Handles file system events for the monitored log file.

    This class is responsible for processing file modifications and creations,
    reading new log entries, and sending them to a Kafka topic.
    """

    def __init__(self):
        """
        Initialize the FileHandler with a Kafka producer and file tracking information.
        """
        self.producer = create_kafka_producer(KAFKA_PROPERTIES_FILE)
        self.current_file = WATCH_FILE
        self.file_position = 0

    def process_IN_MODIFY(self, event):
        """
        Handle file modification events.

        Args:
            event (pyinotify.Event): The file system event object.
        """
        if event.pathname == self.current_file:
            self.process_file()

    def process_IN_CREATE(self, event):
        """
        Handle file creation events.

        Args:
            event (pyinotify.Event): The file system event object.
        """
        if event.pathname == WATCH_FILE:
            logging.info(f"New log file created: {WATCH_FILE}")
            self.current_file = WATCH_FILE
            self.file_position = 0
            self.process_file()

    def process_file(self):
        """
        Read and process new lines from the log file.
        """
        try:
            with open(self.current_file, 'r') as file:
                file.seek(self.file_position)
                for line in file:
                    line = line.strip()
                    if line:
                        self.send_line_to_kafka(line)
                self.file_position = file.tell()
        except FileNotFoundError:
            logging.warning(f"File not found: {self.current_file}. Waiting for new file.")
            self.file_position = 0
        except Exception as e:
            logging.error(f"Error processing {self.current_file}: {str(e)}")

    def send_line_to_kafka(self, line):
        """
        Send a log line to the Kafka topic.

        Args:
            line (str): The log line to send.
        """
        try:
            timestamp = self.extract_timestamp(line)
            message = {
                'file_name': os.path.basename(self.current_file),
                'content': line,
                'timestamp': timestamp
            }
            future = self.producer.send(KAFKA_TOPIC, value=message)
            result = future.get(timeout=60)
            logging.debug(f"Sent line to Kafka. Offset: {result.offset}")
        except Exception as e:
            logging.error(f"Error sending line to Kafka: {str(e)}")

    def extract_timestamp(self, line):
        """
        Extract the timestamp from a log line.

        Args:
            line (str): The log line to extract the timestamp from.

        Returns:
            float: The extracted timestamp as a Unix timestamp.
        """
        try:
            # Assuming the timestamp format is "MMM DD HH:MM:SS"
            timestamp_str = ' '.join(line.split()[:3])
            dt = datetime.strptime(timestamp_str, "%b %d %H:%M:%S")
            # Set the year to current year as it's not in the log
            dt = dt.replace(year=datetime.now().year)
            return dt.timestamp()
        except Exception:
            # If parsing fails, return current timestamp
            return time.time()

def main():
    """
    Main function to set up the file watcher and start the event loop.

    This function initializes the pyinotify WatchManager and Notifier,
    sets up the FileHandler, and starts watching the specified log file
    for modify and create events.
    """
    wm = pyinotify.WatchManager()
    handler = FileHandler()
    notifier = pyinotify.Notifier(wm, handler)
    mask = pyinotify.IN_MODIFY | pyinotify.IN_CREATE  # watch for modify and create events
    wm.add_watch(os.path.dirname(WATCH_FILE), mask)
    logging.info(f"Starting to watch {WATCH_FILE}")
    notifier.loop()

if __name__ == "__main__":
    main()